// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";

// =====================
// CONFIGURATION FROM ENV VARIABLES
// =====================
const ttnServer = process.env.TTN_MQTT_SERVER;
const ttnUsername = process.env.TTN_USERNAME;
const ttnPassword = process.env.TTN_PASSWORD;

const influxUrl = process.env.INFLUX_URL;
const influxToken = process.env.INFLUX_TOKEN;
const influxOrg = process.env.INFLUX_ORG;
const influxBucket = process.env.INFLUX_BUCKET;

// =====================
// CONNECT TO INFLUXDB
// =====================
const influxDB = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, "s");
writeApi.useDefaultTags({ host: "render-ttn" });

// =====================
// CONNECT TO TTN MQTT
// =====================
console.log("🔌 Connecting to TTN MQTT...");
const client = mqtt.connect(ttnServer, {
  username: ttnUsername,
  password: ttnPassword,
  reconnectPeriod: 5000, // auto reconnect every 5 seconds
});

client.on("connect", () => {
  console.log("✅ Connected to TTN MQTT broker");
  client.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
    if (err) console.error("❌ Subscription error:", err);
    else console.log("📡 Subscribed to TTN uplink topic");
  });
});

client.on("reconnect", () => console.log("♻️ Reconnecting to TTN..."));
client.on("error", (err) => console.error("❌ MQTT Error:", err.message));
client.on("close", () => console.log("⚠️ MQTT Connection closed."));

// =====================
// HANDLE UPLINK MESSAGES
// =====================
client.on("message", (topic, message) => {
  try {
    const json = JSON.parse(message.toString());
    const devId = json.end_device_ids.device_id;
    const decoded = json.uplink_message?.decoded_payload;

    if (!decoded) return;

    const point = new Point("air_quality").tag("device", devId);

    // Add all numeric fields dynamically
    for (const [key, value] of Object.entries(decoded)) {
      if (typeof value === "number") {
        point.floatField(key, value);
      }
    }

    writeApi.writePoint(point);
    console.log(`📥 Data written from device: ${devId}`, decoded);

  } catch (err) {
    console.error("⚠️ Error processing message:", err.message);
  }
});

// =====================
// SAFE SHUTDOWN
// =====================
process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down...");
  try {
    await writeApi.close();
    client.end();
  } finally {
    process.exit(0);
  }
});


// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import http from "http";

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
// MQTT CONNECT
// =====================
console.log("🔌 Connecting to TTN MQTT...");

const client = mqtt.connect(ttnServer, {
  username: ttnUsername,
  password: ttnPassword,
  reconnectPeriod: 5000, // auto reconnect every 5s
});

// On successful connect
client.on("connect", () => {
  console.log("✅ Connected to TTN MQTT broker");

  client.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
    if (err) console.error("❌ Subscription error:", err);
    else console.log("📡 Subscribed to TTN uplink topic");
  });
});

// =====================
// IMPROVED MQTT RECOVERY HANDLERS
// =====================
client.on("reconnect", () => {
  console.log("♻️ MQTT reconnecting...");
});

client.on("offline", () => {
  console.log("⚠️ MQTT offline. Waiting for network...");
});

client.on("close", () => {
  console.log("⚠️ MQTT connection closed. Retrying...");
});

client.on("end", () => {
  console.log("⚠️ MQTT client ended. Will not receive messages until restarted.");
});

client.on("error", (err) => {
  console.error("❌ MQTT Error:", err.message);
});

// =====================
// HANDLE INCOMING TTN MESSAGES
// =====================
client.on("message", (topic, message) => {
  try {
    const json = JSON.parse(message.toString());
    const devId = json.end_device_ids.device_id;
    const decoded = json.uplink_message?.decoded_payload;

    if (!decoded) return;

    const point = new Point("air_quality").tag("device", devId);

    // =====================
    // PM10 — separate fields per device
    // =====================
    if (decoded.pm10 !== undefined) {
      if (devId === "mkrwan-1") {
        point.floatField("pm10_1", decoded.pm10);
      } else if (devId === "mkrwan-2") {
        point.floatField("pm10_2", decoded.pm10);
      } else {
        // fallback for unknown devices
        point.floatField(`pm10_${devId}`, decoded.pm10);
      }
    }

    // =====================
    // Add all other numeric fields dynamically
    // =====================
    for (const [key, value] of Object.entries(decoded)) {
      if (typeof value === "number" && key !== "pm10") {
        point.floatField(key, value);
      }
    }

    writeApi.writePoint(point);

    console.log(`📥 Data written | Device: ${devId}`, decoded);

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

// =====================
// DUMMY HTTP SERVER (REQUIRED BY RENDER FREE PLAN)
// =====================
const PORT = process.env.PORT || 10000;

http.createServer((req, res) => {
  console.log("🔔 UptimeRobot ping received at", new Date().toISOString());
  res.writeHead(200);
  res.end("Alive");
}).listen(PORT, () => {
  console.log("✅ Dummy HTTP server running on port", PORT);
});








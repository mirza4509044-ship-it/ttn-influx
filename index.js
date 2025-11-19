import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import http from "http";
let lastMQTTOnline = Date.now();


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

// InfluxDB client
const influxDB = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, "s");
writeApi.useDefaultTags({ host: "render-ttn" });

// =====================
// START HTTP SERVER FIRST
// =====================
function startServer() {
  const PORT = process.env.PORT || 10000;

  http.createServer((req, res) => {
    console.log("🔔 UptimeRobot ping received at", new Date().toISOString());
    res.writeHead(200);
    res.end("Alive");
  }).listen(PORT, () => {
    console.log("✅ Dummy HTTP server running on port", PORT);
  });
}

// =====================
// START MQTT LISTENER
// =====================
function startMQTT() {
  console.log("🔌 Connecting to TTN MQTT...");

  const client = mqtt.connect(ttnServer, {
    username: ttnUsername,
    password: ttnPassword,
    reconnectPeriod: 5000,
  });

  client.on("connect", () => {
    lastMQTTOnline = Date.now();   
    console.log("✅ Connected to TTN MQTT broker");

    client.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
      if (err) console.error("❌ Subscription error:", err);
      else console.log("📡 Subscribed to TTN uplink topic");
    });
  });

  // Recovery logs
  client.on("reconnect", () => console.log("♻️ MQTT reconnecting..."));
  client.on("offline", () => console.log("⚠️ MQTT offline. Waiting..."));
  client.on("close", () => console.log("⚠️ MQTT connection closed."));
  client.on("end", () => console.log("⚠️ MQTT client ended."));
  client.on("error", (err) => console.error("❌ MQTT Error:", err.message));

  // Message handler
  client.on("message", (topic, message) => {
    console.log(`📨 MQTT Msg ${message.length} bytes`);

    try {
      const json = JSON.parse(message.toString());
      const devId = json.end_device_ids?.device_id;
      const decoded = json.uplink_message?.decoded_payload;

      if (!devId || !decoded) return;

      const point = new Point("air_quality").tag("device", devId);

      // PM10 logic
      if (decoded.pm10 !== undefined) {
        if (devId === "mkrwan-1") {
          point.floatField("pm10_1", decoded.pm10);
        } else if (devId === "mkrwan-2") {
          point.floatField("pm10_2", decoded.pm10);
        } else {
          point.floatField(`pm10_${devId}`, decoded.pm10);
        }
      }

      // Other numeric fields
      for (const [key, val] of Object.entries(decoded)) {
        if (typeof val === "number" && key !== "pm10") {
          point.floatField(key, val);
        }
      }

      // Safe write
      try {
        writeApi.writePoint(point);
        console.log(`📥 Data written | Device: ${devId}`, decoded);
      } catch (err) {
        console.log("❌ Influx write error:", err.message);
      }

    } catch (err) {
      console.error("⚠️ Error processing message:", err.message);
    }
  });

  return client;
}
// =====================
// MQTT SELF-RESTART WATCHDOG (5 minutes)
// =====================
setInterval(() => {
  const offlineMinutes = (Date.now() - lastMQTTOnline) / 60000;
  if (offlineMinutes > 5) {
    process.exit(1); // Render restarts automatically
  }
}, 30000); // check every 30s


// =====================
// GRACEFUL SHUTDOWN
// =====================
process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down...");
  try {
    await writeApi.close();
  } finally {
    process.exit(0);
  }
});

// =====================
// ORDER MATTERS HERE
// =====================
startServer();  // Start HTTP immediately (Render health passes)
startMQTT();    // Start MQTT after — no effect on uptime











// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import http from "http";

/*
  Globals
*/
let lastMQTTConnected = 0; // timestamp of last successful CONNECT
let mqttConnected = false; // whether the mqtt client is currently connected
let mqttClient = null;     // keep client in outer scope so it's available everywhere

/* =====================
   CONFIG FROM ENV
   ===================== */
const ttnServer = process.env.TTN_MQTT_SERVER;
const ttnUsername = process.env.TTN_USERNAME;
const ttnPassword = process.env.TTN_PASSWORD;

const influxUrl = process.env.INFLUX_URL;
const influxToken = process.env.INFLUX_TOKEN;
const influxOrg = process.env.INFLUX_ORG;
const influxBucket = process.env.INFLUX_BUCKET;

/* =====================
   INIT INFLUX
   ===================== */
const influxDB = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, "s");
writeApi.useDefaultTags({ host: "render-ttn" });

/* =====================
   START HTTP SERVER (Render health + UptimeRobot)
   ===================== */
function startServer() {
  const PORT = process.env.PORT || 10000;

  http.createServer((req, res) => {
    console.log("🔔 UptimeRobot/Health ping received at", new Date().toISOString());
    res.writeHead(200);
    res.end("Alive");
  }).listen(PORT, () => {
    console.log("✅ Dummy HTTP server running on port", PORT);
  });
}

/* =====================
   START MQTT (and keep client globally)
   ===================== */
function startMQTT() {
  console.log("🔌 Connecting to TTN MQTT...");

  mqttClient = mqtt.connect(ttnServer, {
    username: ttnUsername,
    password: ttnPassword,
    reconnectPeriod: 5000,
    connectTimeout: 30_000,
    keepalive: 60,
    // clientId can be set here if desired
  });

  mqttClient.on("connect", () => {
    mqttConnected = true;
    lastMQTTConnected = Date.now();
    console.log("✅ Connected to TTN MQTT broker");
    mqttClient.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
      if (err) console.error("❌ Subscription error:", err?.message || err);
      else console.log("📡 Subscribed to TTN uplink topic");
    });
  });

  mqttClient.on("reconnect", () => {
    console.log("♻️ MQTT reconnecting...");
  });

  mqttClient.on("offline", () => {
    mqttConnected = false;
    console.log("⚠️ MQTT offline.");
  });

  mqttClient.on("close", () => {
    mqttConnected = false;
    console.log("⚠️ MQTT connection closed.");
  });

  mqttClient.on("end", () => {
    mqttConnected = false;
    console.log("⚠️ MQTT client ended.");
  });

  mqttClient.on("error", (err) => {
    // don't exit here — we rely on watchdog to restart if needed
    console.error("❌ MQTT Error:", err?.message || err);
  });

  mqttClient.on("message", (topic, message) => {
    // mark last seen (we want to know we are actively receiving)
    lastMQTTConnected = Date.now(); // update on message too
    try {
      console.log(`📨 MQTT Msg ${message.length} bytes`);
      const json = JSON.parse(message.toString());

      const devId = json?.end_device_ids?.device_id;
      const decoded = json?.uplink_message?.decoded_payload;

      if (!devId) {
        console.warn("⚠️ Message missing device id — skipped");
        return;
      }
      if (!decoded || typeof decoded !== "object") {
        console.warn("⚠️ Message had no decoded_payload — skipped");
        return;
      }

      const point = new Point("air_quality").tag("device", devId);

      // PM10 — avoid all devices writing into the same single field
      try {
        if (decoded.pm10 !== undefined && typeof decoded.pm10 === "number") {
          if (devId === "mkrwan-1") point.floatField("pm10_1", decoded.pm10);
          else if (devId === "mkrwan-2") point.floatField("pm10_2", decoded.pm10);
          else point.floatField(`pm10_${devId}`, decoded.pm10);
        }
      } catch (pmErr) {
        console.warn("⚠️ pm10 handling error:", pmErr?.message || pmErr);
      }

      // Add other numeric fields dynamically, excluding pm10
      for (const [key, value] of Object.entries(decoded)) {
        if (key === "pm10") continue;
        if (typeof value === "number") {
          try {
            point.floatField(key, value);
          } catch (fErr) {
            console.warn(`⚠️ Skipped field ${key} for ${devId}:`, fErr?.message || fErr);
          }
        }
      }

      // Safe write
      try {
        writeApi.writePoint(point);
        console.log(`📥 Data written | Device: ${devId}`, decoded);
      } catch (writeErr) {
        console.error("❌ Influx write error:", writeErr?.message || writeErr);
      }
    } catch (err) {
      console.error("⚠️ Error processing MQTT message (ignored):", err?.message || err);
    }
  });

  return mqttClient;
}

/* =====================
   WATCHDOG (Option B variant)
   - Only exit if we have not had a successful CONNECT for > 5 minutes
   - Check every 30s
   - If mqttConnected is false AND lastMQTTConnected older than threshold -> exit to let Render restart
   ===================== */
const WATCHDOG_MINUTES = 5;
setInterval(() => {
  try {
    const minutesSinceConnect = (Date.now() - lastMQTTConnected) / 60000;
    // Conditions to restart:
    // - We haven't had a successful connect for WATCHDOG_MINUTES
    // - AND the client is not currently connected
    if (!mqttConnected && lastMQTTConnected > 0 && minutesSinceConnect > WATCHDOG_MINUTES) {
      console.error(`⏱️ MQTT has not CONNECTED for ${minutesSinceConnect.toFixed(1)} minutes — exiting to force restart`);
      // flush writes, then exit
      writeApi
        .close()
        .catch(() => {})
        .finally(() => process.exit(1));
    }
  } catch (err) {
    console.error("⚠️ Watchdog error:", err?.message || err);
  }
}, 30_000);

/* =====================
   Global crash handlers (flush and exit)
   ===================== */
process.on("uncaughtException", (err) => {
  console.error("🔥 Uncaught Exception - exiting:", err?.stack || err?.message || err);
  writeApi.close().catch(() => {}).finally(() => process.exit(1));
});
process.on("unhandledRejection", (reason) => {
  console.error("🔥 Unhandled Rejection - exiting:", reason);
  writeApi.close().catch(() => {}).finally(() => process.exit(1));
});

/* =====================
   STARTUP
   ===================== */
startServer();
startMQTT();

/* =====================
   Graceful shutdown for local dev / SIGTERM
   ===================== */
process.on("SIGINT", async () => {
  console.log("\n🛑 SIGINT received — shutting down gracefully...");
  try { await writeApi.close(); } catch {}
  try { if (mqttClient) mqttClient.end(true); } catch {}
  process.exit(0);
});
process.on("SIGTERM", async () => {
  console.log("\n🛑 SIGTERM received — shutting down gracefully...");
  try { await writeApi.close(); } catch {}
  try { if (mqttClient) mqttClient.end(true); } catch {}
  process.exit(0);
});


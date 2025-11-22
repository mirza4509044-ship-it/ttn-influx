// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import http from "http";

/*
  === Globals ===
  Track when MQTT was last CONNECTED (not last message)
*/
let lastMQTTConnected = Date.now();

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
   START HTTP SERVER
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
   MQTT CLIENT
   ===================== */
function startMQTT() {
  console.log("🔌 Connecting to TTN MQTT...");

  const client = mqtt.connect(ttnServer, {
    username: ttnUsername,
    password: ttnPassword,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    keepalive: 60,
  });

  client.on("connect", () => {
    lastMQTTConnected = Date.now(); // ⬅️ only update on CONNECT
    console.log("✅ Connected to TTN MQTT broker");

    client.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
      if (err) console.error("❌ Subscription error:", err);
      else console.log("📡 Subscribed to TTN uplink topic");
    });
  });

  client.on("reconnect", () => console.log("♻️ MQTT reconnecting..."));
  client.on("offline", () => console.log("⚠️ MQTT offline."));
  client.on("close", () => console.log("⚠️ MQTT connection closed."));
  client.on("end", () => console.log("⚠️ MQTT client ended."));
  client.on("error", (err) => console.error("❌ MQTT Error:", err?.message || err));

  // message handler (unchanged)
  client.on("message", (topic, message) => {
    console.log(`📨 MQTT Msg ${message.length} bytes`);

    try {
      const json = JSON.parse(message.toString());
      const devId = json?.end_device_ids?.device_id;
      const decoded = json?.uplink_message?.decoded_payload;

      if (!devId) return console.warn("⚠️ Missing device id, skipped");
      if (!decoded || typeof decoded !== "object") return console.warn("⚠️ No decoded_payload, skipped");

      const point = new Point("air_quality").tag("device", devId);

      // pm10 special handling
      try {
        if (decoded.pm10 !== undefined && typeof decoded.pm10 === "number") {
          if (devId === "mkrwan-1") point.floatField("pm10_1", decoded.pm10);
          else if (devId === "mkrwan-2") point.floatField("pm10_2", decoded.pm10);
          else point.floatField(`pm10_${devId}`, decoded.pm10);
        }
      } catch (pmErr) {
        console.warn("⚠️ pm10 handling error:", pmErr);
      }

      // add other numeric fields
      for (const [key, value] of Object.entries(decoded)) {
        if (key === "pm10") continue;
        if (typeof value === "number") {
          try {
            point.floatField(key, value);
          } catch (fErr) {
            console.warn(`⚠️ Skipped field ${key}:`, fErr);
          }
        }
      }

      writeApi.writePoint(point);
      console.log(`📥 Data written | Device: ${devId}`, decoded);

    } catch (err) {
      console.error("⚠️ Error processing MQTT message:", err);
    }
  });

  return client;
}

/* =====================
   WATCHDOG (Option B)
   Restart only if no CONNECT for > 5 minutes
   ===================== */
setInterval(() => {
  try {
    const minutesSinceConnect = (Date.now() - lastMQTTConnected) / 60000;

    if (minutesSinceConnect > 5) {
      console.error(
        `⏱️ MQTT has not CONNECTED for ${minutesSinceConnect.toFixed(
          1
        )} minutes — exiting to force restart`
      );

      writeApi
        .close()
        .catch(() => {})
        .finally(() => process.exit(1));
    }
  } catch (err) {
    console.error("⚠️ Watchdog error:", err);
  }
}, 30_000);

/* =====================
   Crash Handlers
   ===================== */
process.on("uncaughtException", (err) => {
    console.error("🔥 Uncaught Exception - exiting:", err);
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
   Local SIGINT
   ===================== */
process.on("SIGINT", async () => {
  console.log("\n🛑 SIGINT received — shutting down...");
  try { await writeApi.close(); } catch {}
  process.exit(0);
});

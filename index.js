// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import http from "http";

/*
  === Globals & top-level requirements ===
  Keep lastMQTTOnline at top so watchdog sees it.
*/
let lastMQTTOnline = Date.now();

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
   START HTTP SERVER FIRST (Render health + UptimeRobot)
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
   START MQTT CONNECTION & HANDLERS
   returns mqtt client
   ===================== */
function startMQTT() {
  console.log("🔌 Connecting to TTN MQTT...");

  const client = mqtt.connect(ttnServer, {
    username: ttnUsername,
    password: ttnPassword,
    reconnectPeriod: 5000, // reconnect every 5s (mqtt lib handles reconnection)
    connectTimeout: 30_000,
    keepalive: 60,
    // optionally you can set clientId here; letting mqtt lib choose is fine
  });

  // when we successfully connect
  client.on("connect", (connack) => {
    lastMQTTOnline = Date.now();
    console.log("✅ Connected to TTN MQTT broker");
    client.subscribe("v3/srsp-lorawan@ttn/devices/+/up", (err) => {
      if (err) console.error("❌ Subscription error:", err.message || err);
      else console.log("📡 Subscribed to TTN uplink topic");
    });
  });

  // diagnostic / recovery handlers
  client.on("reconnect", () => console.log("♻️ MQTT reconnecting..."));
  client.on("offline", () => console.log("⚠️ MQTT offline. Waiting..."));
  client.on("close", () => console.log("⚠️ MQTT connection closed."));
  client.on("end", () => console.log("⚠️ MQTT client ended."));
  client.on("error", (err) => console.error("❌ MQTT Error:", err?.message || err));

  // message handler - guarded so malformed TTN messages don't crash the process
  client.on("message", (topic, message) => {
    lastMQTTOnline = Date.now(); // mark we're receiving again
    console.log(`📨 MQTT Msg ${message.length} bytes`);

    try {
      const json = JSON.parse(message.toString());

      // Guard: make sure expected structure exists
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

      // build Influx point
      const point = new Point("air_quality").tag("device", devId);

      // PM10 special handling: write a device-specific field so different devices don't overwrite one column
      try {
        if (decoded.pm10 !== undefined && typeof decoded.pm10 === "number") {
          // common device names (mkrwan-1 / mkrwan-2) handled explicitly,
          // but we fallback to pm10_<device> for unknown ids (guard requested).
          if (devId === "mkrwan-1") {
            point.floatField("pm10_1", decoded.pm10);
          } else if (devId === "mkrwan-2") {
            point.floatField("pm10_2", decoded.pm10);
          } else {
            point.floatField(`pm10_${devId}`, decoded.pm10);
          }
        }
      } catch (pmErr) {
        console.warn("⚠️ pm10 handling error (ignored):", pmErr?.message || pmErr);
      }

      // Add all other numeric fields dynamically (exclude pm10)
      for (const [key, value] of Object.entries(decoded)) {
        if (key === "pm10") continue;
        if (typeof value === "number") {
          try {
            point.floatField(key, value);
          } catch (fErr) {
            // field might be incompatible; log and continue
            console.warn(`⚠️ Skipped field ${key} for device ${devId}:`, fErr?.message || fErr);
          }
        }
      }

      // write to Influx (safe)
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

  return client;
}

/* =====================
   WATCHDOG: if we haven't seen MQTT online for >5 minutes, exit (Render will restart)
   check every 30s
   ===================== */
setInterval(() => {
  try {
    const offlineMinutes = (Date.now() - lastMQTTOnline) / 60000;
    if (offlineMinutes > 5) {
      console.error(
        `⏱️ MQTT has been offline for ${offlineMinutes.toFixed(1)} minutes — exiting to force restart`
      );
      // Flush writes before exit (best-effort)
      writeApi
        .close()
        .catch(() => {})
        .finally(() => {
          // explicit exit - Render will restart service
          process.exit(1);
        });
    }
  } catch (err) {
    console.error("⚠️ Watchdog error:", err?.message || err);
  }
}, 30_000); // every 30s

/* =====================
   global crash handlers: exit so the process can be restarted
   ===================== */
process.on("uncaughtException", (err) => {
  console.error("🔥 Uncaught Exception - exiting:", err?.stack || err?.message || err);
  // flush writes then exit
  writeApi
    .close()
    .catch(() => {})
    .finally(() => process.exit(1));
});

process.on("unhandledRejection", (reason) => {
  console.error("🔥 Unhandled Rejection - exiting:", reason);
  writeApi
    .close()
    .catch(() => {})
    .finally(() => process.exit(1));
});

/* =====================
   Start everything in correct order
   (server first so Render health checks pass)
   ===================== */
startServer();
startMQTT();

/* =====================
   Optional graceful SIGINT for local dev:
   ===================== */
process.on("SIGINT", async () => {
  console.log("\n🛑 SIGINT received — shutting down gracefully...");
  try {
    await writeApi.close();
  } catch (e) {
    // ignore
  } finally {
    process.exit(0);
  }
});

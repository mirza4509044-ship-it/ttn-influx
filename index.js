// index.js
import mqtt from "mqtt";
import { InfluxDB, Point } from "@influxdata/influxdb-client";

// =====================
// CONFIGURATION
// =====================
const ttnServer = "mqtts://au1.cloud.thethings.network:8883";
const ttnUsername = "srsp-lorawan@ttn";
const ttnPassword = "NNSXS.HJPE2DIFYQNZBWCQBKHJIMU7LPPB4WZATW4OIXQ.RMADM4HTKOP7MH4HKM4L7IB3GYLKBK334SKPSEXKZCBISUUYYW4Q";

const influxUrl = "https://eu-central-1-1.aws.cloud2.influxdata.com";
const influxToken = "NOB93u1L4-eOCqcGusVgXZCPEfNcR3qu_4mo-GE3zS227VJIjVyeBS5jUC9QwgPEh0RlNbVXY_I8HPWuLYGeVA==";
const influxOrg = "srsp_lorawan_mobair";
const influxBucket = "ttn_data";

// =====================
// CONNECT TO INFLUXDB
// =====================
const influxDB = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, "s");
writeApi.useDefaultTags({ host: "render-ttn" });

// =====================
// CONNECT TO TTN MQTT
// =====================
const client = mqtt.connect(ttnServer, {
  username: ttnUsername,
  password: ttnPassword,
  reconnectPeriod: 5000, // auto reconnect every 5s
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
    const payload = JSON.parse(message.toString());
    const devId = payload.end_device_ids.device_id;
    const decoded = payload.uplink_message.decoded_payload;

    if (!decoded) return;

    const point = new Point("air_quality").tag("device", devId);
    for (const [key, value] of Object.entries(decoded)) {
      if (typeof value === "number") {
        point.floatField(key, value);
      }
    }

    writeApi.writePoint(point);
    console.log(`📥 Data written from device: ${devId}`, decoded);
  } catch (error) {
    console.error("❌ Error parsing message:", error.message);
  }
});

// =====================
// GRACEFUL SHUTDOWN
// =====================
process.on("SIGINT", async () => {
  console.log("\n🛑 Disconnecting...");
  await writeApi.close();
  client.end();
  process.exit();
});

require("dotenv").config();
const mqtt = require("mqtt");
const admin = require("firebase-admin");

// Khởi tạo Firebase Admin SDK
console.log("Đang cố gắng khởi tạo Firebase Admin SDK...");
try {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: process.env.FIREBASE_PROJECT_ID,
      privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(
        /\\n/g,
        "\n",
      ).replace(/^"|"$/g, ""),
      clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
    }),
    databaseURL: process.env.FIREBASE_DB_URL,
  });
  console.log("Firebase Admin SDK đã khởi tạo thành công!");
  console.log("Kết nối đến RTDB URL:", process.env.FIREBASE_DB_URL);
} catch (error) {
  console.error("Lỗi khi khởi tạo Firebase Admin SDK:", error.message);
}

const db = admin.firestore();
const rtdb = admin.database();

// Kết nối MQTT
const options = {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
};
const client = mqtt.connect(process.env.MQTT_BROKER_URL, options);

console.log("Đang cố gắng kết nối đến:", process.env.MQTT_BROKER_URL);

let subscribedTopics = new Set();

client.on("connect", () => {
  console.log("Đã kết nối HiveMQ");

  // Lắng nghe snapshot từ gateways để cập nhật topic cần subscribe
  db.collection("gateways").onSnapshot(
    (snapshot) => {
      const currentTopics = new Set();

      snapshot.forEach((doc) => {
        const data = doc.data();
        if (data.topicAlert) {
          currentTopics.add(data.topicAlert);
        }
      });

      // Subscribe các topic mới
      currentTopics.forEach((topic) => {
        if (!subscribedTopics.has(topic)) {
          client.subscribe(topic, (err) => {
            if (err) {
              console.error(`Lỗi khi subscribe topic ${topic}:`, err);
            } else {
              console.log(`Subscribed đến topic: ${topic}`);
              subscribedTopics.add(topic);
            }
          });
        }
      });

      // Unsubscribe các topic không còn sử dụng
      subscribedTopics.forEach((topic) => {
        if (!currentTopics.has(topic)) {
          client.unsubscribe(topic, (err) => {
            if (err) {
              console.error(`Lỗi khi unsubscribe topic ${topic}:`, err);
            } else {
              console.log(`Unsubscribed topic: ${topic}`);
              subscribedTopics.delete(topic);
            }
          });
        }
      });
    },
    (error) => {
      console.error(
        "Lỗi khi lắng nghe changes trên collection gateways:",
        error,
      );
    },
  );
});

client.on("error", (err) => {
  console.error("Lỗi khi kết nối MQTT:", err.message);
});

const nodeMacCache = {};

async function findNodeByAddress(gatewayMac, meshAddress) {
  const cacheKey = `${gatewayMac}_${meshAddress}`;
  if (nodeMacCache[cacheKey]) {
    return nodeMacCache[cacheKey]; // Lấy thẳng từ RAM, 0 tốn read Firestore
  }

  const nodesRef = db.collection("nodes");
  const snapshot = await nodesRef
    .where("gatewayMac", "==", String(gatewayMac))
    .where("meshAddress", "==", Number(meshAddress))
    .get();

  if (snapshot.empty) {
    return null;
  }

  // Lưu vào cache
  nodeMacCache[cacheKey] = snapshot.docs[0].id;
  return nodeMacCache[cacheKey];
}

const nodeDataBuffer = {};
const nodeAggBuffer = {};

client.on("message", async (topic, message) => {
  const data = message.toString();
  console.log(`Nhận được từ ${topic}: ${data}`);

  try {
    const payload = JSON.parse(data);
    const { name, devExtAddr } = payload;

    if (name === "CmdAlertOnline") {
      const { online } = payload;
      const gatewaysRef = db.collection("gateways").doc(String(devExtAddr));
      // Cập nhật trạng thái gateway trên Firestore
      await gatewaysRef
        .update({
          online: online === 1,
        })
        .catch((err) =>
          console.log(
            `Cập nhật gateway online thất bại (Có thể do doc chưa tồn tại):`,
            err.message,
          ),
        );
      console.log(
        `Đã cập nhật trạng thái Gateway ${devExtAddr} online: ${online === 1}`,
      );
    } else if (name === "CmdAlertData") {
      const { timestamp, address, light_lux, current_ma } = payload;
      const nodeMac = await findNodeByAddress(devExtAddr, address);

      if (nodeMac) {
        // 1. Cập nhật RTDB
        await rtdb.ref(`nodes/${nodeMac}/sensor`).set({
          light: light_lux,
          current: current_ma,
          updatedAt: timestamp,
        });

        // 2. Gom nhóm Cập nhật Firestore measurements
        const date = new Date(timestamp * 1000);
        // Reset giây và mili giây để tạo khoá phút
        const minuteDate = new Date(date);
        minuteDate.setSeconds(0, 0);

        const year = minuteDate.getFullYear();
        const month = String(minuteDate.getMonth() + 1).padStart(2, "0");
        const day = String(minuteDate.getDate()).padStart(2, "0");
        const hours = String(minuteDate.getHours()).padStart(2, "0");
        const minutes = String(minuteDate.getMinutes()).padStart(2, "0");
        const minuteId = `${year}-${month}-${day}T${hours}:${minutes}`;

        const newData = {
          t: date.getSeconds(),
          light: light_lux,
          current: current_ma,
        };

        // Xử lý nodeAggBuffer (cập nhật 10 phút một lần)
        const min10 = Math.floor(date.getMinutes() / 10) * 10;
        const id10m = `${year}-${month}-${day}T${hours}:${String(min10).padStart(2, "0")}`;

        if (!nodeAggBuffer[nodeMac]) {
          nodeAggBuffer[nodeMac] = {
            id10m: id10m,
            fYear: year,
            fMonth: month,
            fDay: day,
            fHours: hours,
            min10: min10,
            fDate: date,
            sumLight: light_lux,
            sumCurrent: current_ma,
            count: 1,
          };
        } else {
          if (nodeAggBuffer[nodeMac].id10m !== id10m) {
            // Chuyển sang khung 10 phút mới -> Ghi aggregates của khung cũ
            const prevAgg = nodeAggBuffer[nodeMac];

            const hour4 = Math.floor(prevAgg.fHours / 4) * 4;
            const id1h = `${prevAgg.fYear}-${prevAgg.fMonth}-${prevAgg.fDay}T${prevAgg.fHours}:00`;
            const id4h = `${prevAgg.fYear}-${prevAgg.fMonth}-${prevAgg.fDay}T${String(hour4).padStart(2, "0")}:00`;

            const ts10m = new Date(
              prevAgg.fYear,
              prevAgg.fDate.getMonth(),
              prevAgg.fDate.getDate(),
              prevAgg.fHours,
              prevAgg.min10,
            );
            const ts1h = new Date(
              prevAgg.fYear,
              prevAgg.fDate.getMonth(),
              prevAgg.fDate.getDate(),
              prevAgg.fHours,
              0,
            );
            const ts4h = new Date(
              prevAgg.fYear,
              prevAgg.fDate.getMonth(),
              prevAgg.fDate.getDate(),
              hour4,
              0,
            );

            const ref10m = db
              .collection("nodes")
              .doc(nodeMac)
              .collection("measurements_10m")
              .doc(prevAgg.id10m);
            const ref1h = db
              .collection("nodes")
              .doc(nodeMac)
              .collection("measurements_1h")
              .doc(id1h);
            const ref4h = db
              .collection("nodes")
              .doc(nodeMac)
              .collection("measurements_4h")
              .doc(id4h);

            const sumL = prevAgg.sumLight;
            const sumC = prevAgg.sumCurrent;
            const countA = prevAgg.count;

            db.runTransaction(async (transaction) => {
              const snap10m = await transaction.get(ref10m);
              const snap1h = await transaction.get(ref1h);
              const snap4h = await transaction.get(ref4h);

              const updateAgg = (ref, snap, timestampDate) => {
                if (!snap.exists) {
                  transaction.set(ref, {
                    timestamp:
                      admin.firestore.Timestamp.fromDate(timestampDate),
                    count: countA,
                    avg_light: sumL / countA,
                    avg_current: sumC / countA,
                  });
                } else {
                  const existing = snap.data();
                  const exCount = existing.count || 0;
                  const newCount = exCount + countA;

                  // Tính toán lại tổng dựa trên trung bình cũ (vì đã loại bỏ trường sum_light, sum_current)
                  const exSumLight = (existing.avg_light || 0) * exCount;
                  const exSumCurrent = (existing.avg_current || 0) * exCount;

                  transaction.update(ref, {
                    count: newCount,
                    avg_light: (exSumLight + sumL) / newCount,
                    avg_current: (exSumCurrent + sumC) / newCount,
                  });
                }
              };

              updateAgg(ref10m, snap10m, ts10m);
              updateAgg(ref1h, snap1h, ts1h);
              updateAgg(ref4h, snap4h, ts4h);
            })
              .then(() => {
                console.log(
                  `[Firestore] Đã ghi AGGREGATES (10m, 1h, 4h) Node ${nodeMac} (phút ${prevAgg.id10m})`,
                );
              })
              .catch((err) => {
                console.error(
                  `[Firestore] Lỗi ghi AGGREGATES Node ${nodeMac}:`,
                  err,
                );
              });

            // Reset agg buffer
            nodeAggBuffer[nodeMac] = {
              id10m: id10m,
              fYear: year,
              fMonth: month,
              fDay: day,
              fHours: hours,
              min10: min10,
              fDate: date,
              sumLight: light_lux,
              sumCurrent: current_ma,
              count: 1,
            };
          } else {
            // Cùng khung 10 phút, tiếp tục cộng dồn
            nodeAggBuffer[nodeMac].sumLight += light_lux;
            nodeAggBuffer[nodeMac].sumCurrent += current_ma;
            nodeAggBuffer[nodeMac].count += 1;
          }
        }

        if (!nodeDataBuffer[nodeMac]) {
          // Chưa có buffer cho node này thì khởi tạo
          nodeDataBuffer[nodeMac] = {
            minuteId: minuteId,
            minuteDate: minuteDate,
            data: [newData],
          };
        } else {
          if (nodeDataBuffer[nodeMac].minuteId !== minuteId) {
            // Cùng node nhưng sang phút mới -> Đẩy toàn bộ dữ liệu của phút trước lên Firestore
            const prevData = nodeDataBuffer[nodeMac];

            const firestoreMinuteDate = prevData.minuteDate;
            const firestoreData = prevData.data;

            const rawRef = db
              .collection("nodes")
              .doc(nodeMac)
              .collection("measurements_raw")
              .doc(prevData.minuteId);

            // Ghi RAW mỗi phút (Sử dụng set có merge, tránh 100% lệnh đọc)
            rawRef
              .set(
                {
                  minute:
                    admin.firestore.Timestamp.fromDate(firestoreMinuteDate),
                  data: admin.firestore.FieldValue.arrayUnion(...firestoreData),
                },
                { merge: true },
              )
              .catch((err) => {
                console.error(`[Firestore] Lỗi ghi RAW Node ${nodeMac}:`, err);
              });

            // Khởi tạo lại buffer cho phút hiện tại
            nodeDataBuffer[nodeMac] = {
              minuteId: minuteId,
              minuteDate: minuteDate,
              data: [newData],
            };
          } else {
            // Cùng phút -> Nạp tiếp vào buffer
            nodeDataBuffer[nodeMac].data.push(newData);
          }
        }
        console.log(
          `Đã nạp data sensor vào buffer của Node ${nodeMac} (phút ${minuteId})`,
        );
      } else {
        console.log(
          `Không tìm thấy node với Address ${address} thuộc Gateway ${devExtAddr}`,
        );
      }
    } else if (name === "CmdAlertHistory") {
      const { timestamp, address, history_type, onoff } = payload;
      const nodeMac = await findNodeByAddress(devExtAddr, address);

      if (nodeMac) {
        const isOn = onoff === 1;

        // 1. Cập nhật thuộc tính nodes trên RTDB
        await rtdb.ref(`nodes/${nodeMac}`).update({
          online: true,
          state_on: isOn,
        });

        // 2. Cập nhật state và online trên Firestore
        await db.collection("nodes").doc(nodeMac).update({
          online: true,
          state_on: isOn,
        });

        // 3. Lưu mốc lịch sử event
        await db
          .collection("nodes")
          .doc(nodeMac)
          .collection("events")
          .add({
            timestamp: admin.firestore.Timestamp.fromMillis(timestamp * 1000),
            type: history_type,
            onoff: isOn,
          });
        console.log(`Đã lưu history của Node ${nodeMac}`);
      } else {
        console.log(
          `Không tìm thấy node với Address ${address} thuộc Gateway ${devExtAddr}`,
        );
      }
    }
  } catch (error) {
    if (error instanceof SyntaxError) {
      console.log("Bỏ qua message không phải là JSON format");
    } else {
      console.error("Lỗi khi xử lý MQTT message:", error);
    }
  }
});

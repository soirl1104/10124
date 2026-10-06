/**
 * 智光高級商工職業學校資處科一仁午餐訂餐系統
 * 核心應用程式：AppState 集中狀態管理、型態安全轉換、純淨無測試假資料、跨裝置同步與徹底重置
 */

// 一、 學生名單資料（共 6 排 31 人）
const STUDENTS = [
  // 第一排（1 ~ 6 號）
  { id: 1, row: 1 },
  { id: 2, row: 1 },
  { id: 3, row: 1 },
  { id: 4, row: 1 },
  { id: 5, row: 1 },
  { id: 6, row: 1 },

  // 第二排（7 ~ 12 號）
  { id: 7, row: 2 },
  { id: 8, row: 2 },
  { id: 9, row: 2 },
  { id: 10, row: 2 },
  { id: 11, row: 2 },
  { id: 12, row: 2 },

  // 第三排（13 ~ 18 號）
  { id: 13, row: 3 },
  { id: 14, row: 3 },
  { id: 15, row: 3 },
  { id: 16, row: 3 },
  { id: 17, row: 3 },
  { id: 18, row: 3 },

  // 第四排（19 ~ 24 號）
  { id: 19, row: 4 },
  { id: 20, row: 4 },
  { id: 21, row: 4 },
  { id: 22, row: 4 },
  { id: 23, row: 4 },
  { id: 24, row: 4 },

  // 第五排（25 ~ 30 號）
  { id: 25, row: 5 },
  { id: 26, row: 5 },
  { id: 27, row: 5 },
  { id: 28, row: 5 },
  { id: 29, row: 5 },
  { id: 30, row: 5 },

  // 第六排（31 號）
  { id: 31, row: 6 }
];

// 二、 餐點清單（共 12 項）
const MENU_ITEMS = [
  { id: "m1", name: "涼麵", price: 50, icon: "🍜" },
  { id: "m2", name: "滷肉飯", price: 50, icon: "🍚" },
  { id: "m3", name: "麻辣燙（附白飯）", price: 80, icon: "🍲" },
  { id: "m4", name: "沙茶豬肉鍋（附白飯）", price: 90, icon: "🍲" },
  { id: "m5", name: "麻辣沙茶豬肉鍋（附白飯）", price: 90, icon: "🥘" },
  { id: "m6", name: "咖哩雞排飯", price: 80, icon: "🍛" },
  { id: "m7", name: "便當（無骨雞）", price: 80, icon: "🍱" },
  { id: "m8", name: "便當（椒麻雞）", price: 80, icon: "🍱" },
  { id: "m9", name: "便當（日式豬）", price: 80, icon: "🍱" },
  { id: "m10", name: "便當（豬柳飯）", price: 80, icon: "🍱" },
  { id: "m11", name: "便當（糖醋排骨）", price: 80, icon: "🍱" },
  { id: "m12", name: "羹麵", price: 60, icon: "🍜" }
];

// =========================================================
// 集中式應用程式狀態管理 (AppState)
// 預設純淨無測試假資料（空物件 {} 與空陣列 []）
// =========================================================
const STORAGE_KEY = "zhiguang_cs1ren_orders_clean_v5";
const STORAGE_FEED_KEY = "zhiguang_cs1ren_feed_clean_v5";
const STORAGE_RESET_EPOCH = "zhiguang_cs1ren_reset_epoch_v5";

const AppState = {
  orders: {},          // 預設純淨空物件，絕無硬編碼假資料 (如 29, 30)
  activityFeed: [],    // 預設純淨空陣列
  selectedMealId: null,
  soundEnabled: true,
  resetEpoch: 0,
  stateTimestamp: 0    // Unix ms 狀態版本號：每次 orders 變動時更新，用於拒絕過時 retained 訊息
};

// 掛載至 window 全域物件，解決控制台 Console 呼叫 AppState is not defined 作用域問題
window.AppState = AppState;

// 唯一裝置辨識標記
const DEVICE_ID = 'dev_' + Math.random().toString(36).substring(2, 9);
const IS_MOBILE = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
const DEVICE_LABEL = IS_MOBILE ? "📱 手機" : "💻 電腦";

// 環境偵測：自動識別是用 node server.js 本地伺服器開啟，還是 GitHub Pages / 雲端託管
const IS_GITHUB_PAGES = location.hostname.endsWith('github.io') || location.hostname.includes('github');
let IS_LOCAL_SERVER = location.protocol.startsWith('http') && !IS_GITHUB_PAGES;

async function detectServerMode() {
  if (IS_GITHUB_PAGES) {
    IS_LOCAL_SERVER = false;
    return false;
  }
  try {
    const res = await fetch(`${getServerBaseUrl()}/api/orders`, { cache: 'no-store' });
    if (res.ok) {
      const data = await res.json();
      if (data && data.success) {
        IS_LOCAL_SERVER = true;
        console.log("🟢 [伺服器對接成功] 已自動辨識並連接 Node.js 伺服器 HTTP API");
        return true;
      }
    }
  } catch (e) {}
  return IS_LOCAL_SERVER;
}

// MQTT 跨網域無限制即時同步 (支援 4G / 5G / 各類 Wi-Fi 跨網雲端叢集)
const MQTT_BROKERS = [
  "wss://broker.emqx.io:8084/mqtt",
  "wss://broker.hivemq.com:8884/mqtt",
  "wss://test.mosquitto.org:8081/mqtt"
];
let currentBrokerIndex = 0;
const MQTT_TOPIC_EVENT = "zhiguang/cs1ren/prod_clean_2026/events";
const MQTT_TOPIC_STATE = "zhiguang/cs1ren/prod_clean_2026/state";
const MQTT_TOPIC_REQ = "zhiguang/cs1ren/prod_clean_2026/sync_request";
let mqttClient = null;

// 瀏覽器跨分頁廣播
const BROADCAST_CHANNEL_NAME = "zhiguang_cs1ren_channel_clean_v5";
let broadcastChannel = null;

// 本地 WebSocket
let localWs = null;

// =========================================================
// 訂單資料驗證與清理 (Sanitization)
// =========================================================
function sanitizeOrders(rawOrders) {
  const clean = {};
  if (!rawOrders || typeof rawOrders !== 'object') return clean;
  for (const seatIdKey in rawOrders) {
    const seatId = Number(seatIdKey); // 嚴格轉換為數字
    if (isNaN(seatId) || seatId < 1 || seatId > 31) continue;
    const student = STUDENTS.find(s => Number(s.id) === seatId);
    const item = rawOrders[seatIdKey];
    if (student && item && item.mealId) {
      const meal = MENU_ITEMS.find(m => String(m.id) === String(item.mealId));
      if (meal) {
        clean[seatId] = {
          seatId: Number(student.id),
          mealId: String(meal.id),
          mealName: meal.name,
          price: Number(meal.price),
          time: item.time || getNowTime(),
          deviceType: item.deviceType || "📱 裝置",
          timestamp: Number(item.timestamp || Date.now())
        };
      }
    }
  }
  return clean;
}

function getResetEpoch() {
  try {
    return Number(localStorage.getItem(STORAGE_RESET_EPOCH) || "0");
  } catch (e) {
    return 0;
  }
}

function setResetEpoch(epoch) {
  try {
    localStorage.setItem(STORAGE_RESET_EPOCH, String(Number(epoch)));
  } catch (e) { }
}

// =========================================================
// 資料持久化 (LocalStorage 安全讀寫)
// 沒讀到資料時 Fallback 嚴格回傳空物件 {} 或空陣列 []
// =========================================================
function persistOrders() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(AppState.orders || {}));
    localStorage.setItem(STORAGE_FEED_KEY, JSON.stringify(AppState.activityFeed ? AppState.activityFeed.slice(0, 30) : []));
  } catch (e) {
    console.error("LocalStorage 寫入失敗:", e);
  }
}
const saveLocalData = persistOrders;

function loadLocalData() {
  // 自動清理舊版殘留快取
  ["zhiguang_cs1ren_lunch_orders_v1", "zhiguang_cs1ren_lunch_orders_v2", "zhiguang_cs1ren_feed_v2"].forEach(k => {
    try { localStorage.removeItem(k); } catch (e) { }
  });

  // 安全解析 LocalStorage，加入 try...catch 避免 JSON 解析失敗引發頁面崩潰
  try {
    const rawOrders = localStorage.getItem(STORAGE_KEY);
    if (rawOrders) {
      const parsed = JSON.parse(rawOrders);
      AppState.orders = (parsed && typeof parsed === 'object') ? sanitizeOrders(parsed) : {};
    } else {
      AppState.orders = {}; // 嚴格 Fallback 為空物件
    }

    const rawFeed = localStorage.getItem(STORAGE_FEED_KEY);
    if (rawFeed) {
      const parsedFeed = JSON.parse(rawFeed);
      AppState.activityFeed = Array.isArray(parsedFeed) ? parsedFeed : [];
    } else {
      AppState.activityFeed = []; // 嚴格 Fallback 為空陣列
    }
  } catch (e) {
    console.warn("LocalStorage 讀取解析異常，回退至純淨空狀態:", e);
    AppState.orders = {};
    AppState.activityFeed = [];
  }
}

// =========================================================
// 核心封裝函式：取消點餐 cancelOrder(seat)
// 支援在 Console 直接調用，自動執行 delete AppState.orders[seat]、
// 呼叫 persistOrders()、廣播通知並重新渲染畫面
// =========================================================
function cancelOrder(seat) {
  const seatId = Number(seat); // 確保嚴格型態轉換，避免 "29" 與 29 比對失敗
  if (isNaN(seatId) || seatId < 1 || seatId > 31) return;
  if (!AppState.orders[seatId]) return;

  const order = AppState.orders[seatId];
  delete AppState.orders[seatId];
  persistOrders();

  // 先廣播事件，再立即更新 MQTT retained 狀態（確保 broker 上的狀態是已刪除後的版本）
  broadcastToAllDevices('ORDER_CANCELLED', { seatId: seatId });
  // 明確覆寫 MQTT retained，避免新連線裝置收到舊的 retained 訊息而把取消的訂單「補回來」
  publishFullStateToMQTT();

  // 雙重保證：向伺服器發送取消請求，同步清除 orders.json
  fetch(`${getServerBaseUrl()}/api/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ seatId: seatId })
  }).catch(e => console.warn('POST /api/cancel 靜默跳過:', e.message));

  renderAllViews();
  playCancelChime();
  addActivityItem('cancel', `${seatId}號 取消點餐`, `${order.mealName} 已取消`, getNowTime());

  console.log(`✅ [cancelOrder] 座號 ${seatId} 點餐已取消`);
}
window.cancelOrder = cancelOrder;

function resetSystem() {
  const newEpoch = Date.now();
  setResetEpoch(newEpoch);

  // 1. 一次清空 AppState 記憶體資料
  AppState.orders = {};
  AppState.activityFeed = [];
  AppState.stateTimestamp = 0;

  // 2. 徹底清理 LocalStorage
  try {
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(STORAGE_FEED_KEY);
  } catch (e) {
    console.error("LocalStorage 清理失敗:", e);
  }

  // 3. 發布空狀態包覆寫 MQTT 雲端 Retained 訊息，抹除歷史保留
  if (mqttClient && mqttClient.connected) {
    try {
      mqttClient.publish(MQTT_TOPIC_STATE, JSON.stringify({ orders: {}, resetEpoch: newEpoch, cleared: true }), { retain: true, qos: 1 });
    } catch (e) { }
  }

  // 4. 廣播重置事件（附帶操作裝置標籤）
  broadcastToAllDevices('ORDERS_RESET', { resetEpoch: newEpoch, operatorLabel: DEVICE_LABEL });

  // 雙重保證：向伺服器發送重置請求，同步清空 orders.json
  fetch(`${getServerBaseUrl()}/api/reset`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ resetEpoch: newEpoch, operatorLabel: DEVICE_LABEL })
  }).catch(e => console.warn('POST /api/reset 靜默跳過:', e.message));

  // 5. 同步重新渲染所有 DOM 畫面
  renderAllViews();

  // 6. 在本機動態牆新增重置記錄
  addActivityItem('reset', `🔄 訂餐紀錄已全部重置`, `由 ${DEVICE_LABEL} 執行，所有座位已重新開放`, getNowTime());

  console.log("✅ [resetSystem] 系統已徹底重置！AppState.orders 與 LocalStorage 已清空，畫面已同步更新。");
}
window.resetSystem = resetSystem;

// 供外部習慣使用的別名函式
window.renderLiveRosterTable = function () {
  renderAdminDashboard();
};

// =========================================================
// 音效合成器 (Web Audio API)
// =========================================================
let audioCtx = null;
function getAudioContext() {
  if (!audioCtx) {
    const AudioContextClass = window.AudioContext || window.webkitAudioContext;
    if (AudioContextClass) audioCtx = new AudioContextClass();
  }
  if (audioCtx && audioCtx.state === 'suspended') {
    audioCtx.resume();
  }
  return audioCtx;
}

function playSuccessChime() {
  if (!AppState.soundEnabled) return;
  try {
    const ctx = getAudioContext();
    if (!ctx) return;
    const now = ctx.currentTime;

    const osc1 = ctx.createOscillator();
    const gain1 = ctx.createGain();
    osc1.type = 'sine';
    osc1.frequency.setValueAtTime(523.25, now);
    osc1.frequency.exponentialRampToValueAtTime(659.25, now + 0.12);
    gain1.gain.setValueAtTime(0.2, now);
    gain1.gain.exponentialRampToValueAtTime(0.001, now + 0.4);
    osc1.connect(gain1);
    gain1.connect(ctx.destination);
    osc1.start(now);
    osc1.stop(now + 0.4);

    const osc2 = ctx.createOscillator();
    const gain2 = ctx.createGain();
    osc2.type = 'triangle';
    osc2.frequency.setValueAtTime(783.99, now + 0.1);
    osc2.frequency.exponentialRampToValueAtTime(1046.50, now + 0.28);
    gain2.gain.setValueAtTime(0.25, now + 0.1);
    gain2.gain.exponentialRampToValueAtTime(0.001, now + 0.65);
    osc2.connect(gain2);
    gain2.connect(ctx.destination);
    osc2.start(now + 0.1);
    osc2.stop(now + 0.65);
  } catch (e) { }
}

function playCancelChime() {
  if (!AppState.soundEnabled) return;
  try {
    const ctx = getAudioContext();
    if (!ctx) return;
    const now = ctx.currentTime;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.setValueAtTime(440, now);
    osc.frequency.linearRampToValueAtTime(311.13, now + 0.25);
    gain.gain.setValueAtTime(0.18, now);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(now);
    osc.stop(now + 0.35);
  } catch (e) { }
}

function addActivityItem(type, title, desc, time) {
  const item = { type, title, desc, time: time || getNowTime() };
  AppState.activityFeed.unshift(item);
  if (AppState.activityFeed.length > 30) AppState.activityFeed.pop();
  persistOrders();
  renderActivityFeed();
  updateMarquee(title + "：" + desc);
}

function getNowTime() {
  const d = new Date();
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
}

// =========================================================
// 跨裝置同步引擎 (HTTP REST API + WebSocket + MQTT + BroadcastChannel)
// =========================================================
function initSyncEngine() {
  if (typeof BroadcastChannel !== 'undefined') {
    broadcastChannel = new BroadcastChannel(BROADCAST_CHANNEL_NAME);
    broadcastChannel.onmessage = (event) => {
      handleRemotePayload(event.data);
    };
  }

  window.addEventListener('storage', (e) => {
    if (e.key === STORAGE_KEY) {
      loadLocalData();
      renderAllViews();
    }
  });

  connectMQTT();

  // 無論是透過 http:// 伺服器網址還是 file:// 點開，皆啟動區網對接與 500ms 極速靜默輪詢
  connectLocalWebSocket();
  fetchServerOrders();
  setInterval(fetchServerOrders, 500);

  // 手機全生命週期極速喚醒與自動重連
  const triggerActiveSync = () => {
    fetchServerOrders();
    if (!localWs || localWs.readyState !== WebSocket.OPEN) {
      connectLocalWebSocket();
    }
    if (mqttClient && !mqttClient.connected) {
      try { mqttClient.reconnect(); } catch (e) {}
    }
  };

  window.addEventListener('focus', triggerActiveSync);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') triggerActiveSync();
  });
  window.addEventListener('pageshow', triggerActiveSync);
  window.addEventListener('online', triggerActiveSync);
  document.addEventListener('touchstart', triggerActiveSync, { passive: true, once: false });
}

function connectMQTT() {
  if (typeof mqtt === 'undefined') {
    if (!window._mqttRetryCount) window._mqttRetryCount = 0;
    if (window._mqttRetryCount < 30) {
      window._mqttRetryCount++;
      updateNetworkStatus(true, "⚡ 正在啟動全網極速通道...");
      setTimeout(connectMQTT, 200);
      return;
    }
    updateNetworkStatus(true, "🟢 本地廣播同步模式");
    return;
  }

  const currentBroker = MQTT_BROKERS[currentBrokerIndex % MQTT_BROKERS.length];

  try {
    const clientId = 'zg_' + Math.random().toString(16).substring(2, 10);
    mqttClient = mqtt.connect(currentBroker, {
      clientId: clientId,
      clean: true,
      connectTimeout: 4000,
      reconnectPeriod: 1000,
      keepalive: 15
    });

    mqttClient.on('connect', () => {
      updateNetworkStatus(true, "🟢 4G/5G/全網段 0 延遲同步中");
      mqttClient.subscribe(MQTT_TOPIC_EVENT);
      mqttClient.subscribe(MQTT_TOPIC_STATE);
      mqttClient.subscribe(MQTT_TOPIC_REQ);
      mqttClient.publish(MQTT_TOPIC_REQ, JSON.stringify({ reqFrom: DEVICE_ID }));
      flushPendingBroadcasts();
      if (Object.keys(AppState.orders).length > 0) {
        publishFullStateToMQTT();
      }
    });

    mqttClient.on('message', (topic, message) => {
      try {
        const payload = JSON.parse(message.toString());
        const currentEpoch = getResetEpoch();

        if (topic === MQTT_TOPIC_STATE) {
          if (payload) {
            const incomingEpoch = Number(payload.resetEpoch || 0);
            if (currentEpoch > 0 && incomingEpoch < currentEpoch) {
              publishFullStateToMQTT();
              return;
            }

            const incomingUpdatedAt = Number(payload.updatedAt || 0);

            if (payload.cleared === true) {
              if (IS_LOCAL_SERVER) {
                return;
              }
              if (incomingEpoch > currentEpoch) {
                setResetEpoch(incomingEpoch);
                AppState.orders = {};
                AppState.activityFeed = [];
                AppState.stateTimestamp = incomingUpdatedAt || Date.now();
                persistOrders();
                renderAllViews();
              }
              return;
            }
            if (payload.orders) {
              applyOrdersUnionMerge(payload.orders, '雲端');
            }
          }
        } else if (topic === MQTT_TOPIC_EVENT) {
          handleRemotePayload(payload);
        } else if (topic === MQTT_TOPIC_REQ) {
          if (payload.reqFrom !== DEVICE_ID && Object.keys(AppState.orders).length > 0) {
            publishFullStateToMQTT();
          }
        }
      } catch (err) {
        console.warn("MQTT parse error:", err);
      }
    });

    mqttClient.on('error', () => {
      currentBrokerIndex++;
      setTimeout(connectMQTT, 1000);
    });

    mqttClient.on('offline', () => {
      updateNetworkStatus(true, "🟢 區網/雙通道極速同步中");
    });
  } catch (e) {
    currentBrokerIndex++;
    setTimeout(connectMQTT, 1000);
  }
}

// 自動辨識伺服器網址（支援 http、手機 IP 或直接點開 file://）
function getServerBaseUrl() {
  if (location.protocol.startsWith('http')) return location.origin;
  return 'http://localhost:3000';
}

function getServerWsUrl() {
  if (location.protocol === 'https:') return `wss://${location.host}/ws`;
  if (location.protocol.startsWith('http')) return `ws://${location.host}/ws`;
  return 'ws://localhost:3000/ws';
}

let _wsHeartbeatTimer = null;
function connectLocalWebSocket() {
  try {
    if (localWs && (localWs.readyState === WebSocket.OPEN || localWs.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const wsUrl = getServerWsUrl();
    localWs = new WebSocket(wsUrl);

    localWs.onopen = () => {
      updateNetworkStatus(true, "🟢 班級區網高速同步中");
      try {
        localWs.send(JSON.stringify({ type: 'REQ_SYNC', senderId: DEVICE_ID }));
      } catch (e) { }
      fetchServerOrders();

      if (_wsHeartbeatTimer) clearInterval(_wsHeartbeatTimer);
      _wsHeartbeatTimer = setInterval(() => {
        if (localWs && localWs.readyState === WebSocket.OPEN) {
          try { localWs.send(JSON.stringify({ type: 'PING' })); } catch (e) { }
        }
      }, 15000);
    };

    localWs.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data);
        if (payload.type === 'PONG') return;
        handleRemotePayload(payload);
      } catch (err) { }
    };

    localWs.onclose = () => {
      updateNetworkStatus(false, "🟡 區網重新連線中...");
      if (_wsHeartbeatTimer) {
        clearInterval(_wsHeartbeatTimer);
        _wsHeartbeatTimer = null;
      }
      localWs = null;
      setTimeout(() => {
        connectLocalWebSocket();
      }, 2000);
    };

    localWs.onerror = () => {
      updateNetworkStatus(false, "🟡 區網連線異常");
      try { localWs.close(); } catch (e) { }
    };
  } catch (e) { }
}

// =========================================================
// 全網單一跳通知與音效/橫幅即時觸發器 (Deduplicated Order Effects Trigger)
// 保證無論訂單從 HTTP 輪詢、WebSocket、MQTT 還是點擊表單傳入，100% 立即彈出頂部 HUD 橫幅、閃爍與音效！
// =========================================================
const _notifiedOrderKeys = new Set();

function triggerNewOrderEffects(order, deviceLabel = '同學') {
  if (!order || !order.seatId) return;
  const seatId = Number(order.seatId);
  const orderKey = `${seatId}_${order.mealId}_${order.timestamp || order.time}`;

  if (_notifiedOrderKeys.has(orderKey)) return; // 防重複彈出
  _notifiedOrderKeys.add(orderKey);

  if (_notifiedOrderKeys.size > 120) {
    const firstKey = _notifiedOrderKeys.values().next().value;
    _notifiedOrderKeys.delete(firstKey);
  }

  // 1. 頂部醒目即時推播橫幅 (HUD Banner 即刻彈出)
  triggerLivePushBanner(
    `📢 ${deviceLabel} 剛送出點餐！`,
    `【${seatId} 號】點了「${order.mealName}」($${order.price}元)！`
  );

  // 2. 即時跑馬燈快報
  updateMarquee(`⚡【${seatId} 號】剛點了「${order.mealName}」($${order.price}元)！全班總計已同步更新`);

  // 3. 浮動 Toast 提示
  showToast(`🔔 [${seatId}號] 剛點了餐！`, `餐點：${order.mealName} ($${order.price}元)`);

  // 4. 清脆音效
  playSuccessChime();

  // 5. 課桌卡片綠光脈衝
  pulseDeskCard(seatId);

  // 6. 標題分頁閃爍
  flashPageTitle(`🔔【${seatId}號 點了 ${order.mealName}】`);

  // 7. 系統級桌面/手機推播
  triggerDesktopNotification(`🍱【${seatId}號 點餐通知】`, `${seatId} 號同學點了「${order.mealName}」($${order.price}元)`);
}

// =========================================================
// 核心權威資料對接與極速同步演算法
// 確保任何裝置點餐或取消，全班裝置毫秒級 100% 畫面對齊！
// =========================================================
function applyOrdersUnionMerge(incomingOrders, sourceLabel = '遠端', isAuthoritativeServer = false) {
  if (!incomingOrders || typeof incomingOrders !== 'object') return false;
  const sanitizedIncoming = sanitizeOrders(incomingOrders);

  let hasNewChanges = false;

  // 1. 同步傳入的所有座號訂單至本機
  for (const seatId in sanitizedIncoming) {
    const incoming = sanitizedIncoming[seatId];
    const local = AppState.orders[seatId];

    if (!local) {
      AppState.orders[seatId] = incoming;
      hasNewChanges = true;
      addActivityItem('order', `⚡ [${sourceLabel}極速] ${incoming.seatId}號`, `${incoming.mealName}`, incoming.time || getNowTime());
      triggerNewOrderEffects(incoming, incoming.deviceType || sourceLabel);
    } else if (local.mealId !== incoming.mealId || (isAuthoritativeServer && local.time !== incoming.time)) {
      AppState.orders[seatId] = incoming;
      hasNewChanges = true;
      triggerNewOrderEffects(incoming, incoming.deviceType || sourceLabel);
    }
  }

  // 2. 若為權威伺服器傳回的最新點餐對照表 (HTTP API)：
  // 本機有記錄但伺服器端已沒有該座位 -> 代表已在其他裝置取消，本機精準同步移除
  if (isAuthoritativeServer) {
    for (const seatId in AppState.orders) {
      if (!sanitizedIncoming[seatId]) {
        delete AppState.orders[seatId];
        hasNewChanges = true;
      }
    }
  }

  if (hasNewChanges) {
    AppState.stateTimestamp = Date.now();
    persistOrders();
    renderAllViews();
  }

  return hasNewChanges;
}

// 核心函式：自伺服器取得全量訂單（100% 保證同步與頁面刷新資料不遺失！）
let _isFetchingServerOrders = false;
async function fetchServerOrders() {
  if (_isFetchingServerOrders) return;

  _isFetchingServerOrders = true;
  try {
    const res = await fetch(`${getServerBaseUrl()}/api/orders`, { cache: 'no-store' });
    if (!res.ok) return;
    const data = await res.json();
    if (!data || !data.success) return;

    IS_LOCAL_SERVER = true;

    const serverEpoch = Number(data.lastResetEpoch || 0);
    if (serverEpoch > 0) {
      setResetEpoch(serverEpoch);
    }

    if (Array.isArray(data.activityFeed) && data.activityFeed.length > 0) {
      AppState.activityFeed = data.activityFeed;
      renderActivityFeed();
    }

    applyOrdersUnionMerge(data.orders || {}, '伺服器', true);
  } catch (e) {
    // 離線或非伺服器模式，靜默略過
  } finally {
    _isFetchingServerOrders = false;
  }
}

function updateNetworkStatus(online, text) {
  const badge = document.getElementById("network-status-badge");
  if (badge) {
    badge.innerHTML = `<span class="live-dot" style="background-color: ${online ? '#10b981' : '#f59e0b'};"></span> ${text}`;
  }
}

function publishFullStateToMQTT() {
  if (mqttClient && mqttClient.connected) {
    try {
      const now = Date.now();
      if (now > AppState.stateTimestamp) AppState.stateTimestamp = now;
      mqttClient.publish(MQTT_TOPIC_STATE, JSON.stringify({
        orders: sanitizeOrders(AppState.orders),
        resetEpoch: getResetEpoch(),
        updatedBy: DEVICE_ID,
        updatedAt: AppState.stateTimestamp,
        time: getNowTime()
      }), { retain: true, qos: 1 });
    } catch (e) { }
  }
}

const _pendingBroadcasts = [];

function flushPendingBroadcasts() {
  if (!mqttClient || !mqttClient.connected) return;
  while (_pendingBroadcasts.length > 0) {
    const payload = _pendingBroadcasts.shift();
    try {
      mqttClient.publish(MQTT_TOPIC_EVENT, JSON.stringify(payload), { qos: 1 });
    } catch (e) { }
  }
  publishFullStateToMQTT();
}

function broadcastToAllDevices(type, data) {
  const payload = {
    type: type,
    data: data,
    resetEpoch: getResetEpoch(),
    senderId: DEVICE_ID,
    deviceLabel: DEVICE_LABEL,
    time: getNowTime()
  };

  if (broadcastChannel) {
    try { broadcastChannel.postMessage(payload); } catch (e) { }
  }

  if (mqttClient && mqttClient.connected) {
    try {
      mqttClient.publish(MQTT_TOPIC_EVENT, JSON.stringify(payload), { qos: 1 });
      publishFullStateToMQTT();
    } catch (e) { }
  } else {
    _pendingBroadcasts.push(payload);
  }

  if (localWs && localWs.readyState === WebSocket.OPEN) {
    try { localWs.send(JSON.stringify(payload)); } catch (e) { }
  }
}

function handleRemotePayload(payload) {
  if (!payload || !payload.type) return;
  if (payload.senderId === DEVICE_ID) return;

  const currentEpoch = getResetEpoch();

  // 若訊息附帶全量點餐快照 (allOrders)，優先精準畫面對齊
  if (payload.allOrders && typeof payload.allOrders === 'object') {
    applyOrdersUnionMerge(payload.allOrders, '伺服器', true);
  }

  if (payload.type === 'REQ_SYNC') {
    fetchServerOrders();
    if (Object.keys(AppState.orders).length > 0) {
      publishFullStateToMQTT();
    }
    return;
  }

  // 伺服器全量狀態下發 (STATE_INIT)
  if (payload.type === 'STATE_INIT') {
    if (payload.data) {
      const serverEpoch = Number(payload.data.resetEpoch || payload.resetEpoch || 0);
      if (serverEpoch > currentEpoch) {
        setResetEpoch(serverEpoch);
        AppState.orders = {};
        AppState.activityFeed = [];
      }
      if (Array.isArray(payload.data.activityFeed) && payload.data.activityFeed.length > 0) {
        AppState.activityFeed = payload.data.activityFeed;
        renderActivityFeed();
      }
      if (payload.data.orders) {
        applyOrdersUnionMerge(payload.data.orders, '區網', true);
      }
    }
    return;
  }

  if (payload.type === 'ORDERS_RESET') {
    if (payload.resetEpoch) setResetEpoch(payload.resetEpoch);
    AppState.orders = {};
    AppState.activityFeed = [];
    AppState.stateTimestamp = 0;
    persistOrders();
    publishFullStateToMQTT();
    renderAllViews();
    const operatorLabel = (payload.data && payload.data.operatorSeat)
      ? `${payload.data.operatorSeat}號`
      : ((payload.data && payload.data.operatorLabel) ? payload.data.operatorLabel : (payload.deviceLabel || '其他裝置'));
    triggerLivePushBanner(`🔄 全班訂餐紀錄已重置`, `${operatorLabel} 重置了班上整個訂單，所有座位已重新開放`);
    showToast(`🔄 系統通知`, `${operatorLabel} 重置了班上整個訂單`, true);
    addActivityItem('reset', `🔄 ${operatorLabel} 重置了班上整個訂單`, `所有座位已重新開放`, getNowTime());
    return;
  } else if (payload.type === 'ORDER_CREATED') {
    const order = payload.data;
    if (order && order.seatId) {
      const seatNum = Number(order.seatId);
      const isNew = !AppState.orders[seatNum];
      AppState.orders[seatNum] = order;
      persistOrders();
      renderAllViews();
      if (isNew) {
        triggerNewOrderEffects(order, payload.deviceLabel || '同學');
      }
    }
  } else if (payload.type === 'ORDER_CANCELLED') {
    const { seatId } = payload.data;
    const sId = Number(seatId);
    const hadOrder = !!AppState.orders[sId];
    delete AppState.orders[sId];
    persistOrders();
    renderAllViews();

    triggerLivePushBanner(`⚠️ 取消點餐推播`, `【${sId} 號】已取消點餐，座位已重新釋放`);
    showToast(`⚠️ [${sId}號] 取消了點餐`, `該座位已重新開放點餐`, true);
    playCancelChime();
    addActivityItem('cancel', `${sId}號 取消點餐`, hadOrder ? `座位已釋放` : `（座位已確認釋放）`, getNowTime());
  }
}

/**
 * 一鍵強制作業：馬上同步所有同學的裝置！
 */
async function forceSyncAllDevices(showUserToast = true) {
  // 1. 本機立即向伺服器拉取最新點餐 (GET /api/orders)
  await fetchServerOrders();

  // 2. 廣播 REQ_SYNC 至所有同學裝置，要求全班連線中的手機/電腦立即對齊
  broadcastToAllDevices('REQ_SYNC', { forceSync: true });

  if (showUserToast) {
    showToast('⚡ 全班強制作業同步！', '已向所有同學的手機/電腦發送畫面對齊指令');
  }
}
window.forceSyncAllDevices = forceSyncAllDevices;

function pulseDeskCard(seatId) {
  const desk = document.getElementById(`desk-seat-${Number(seatId)}`);
  if (desk) {
    desk.classList.add('pulse-highlight');
    setTimeout(() => desk.classList.remove('pulse-highlight'), 4000);
  }
}

let bannerTimeout = null;
function triggerLivePushBanner(badgeText, mainText) {
  const banner = document.getElementById("live-broadcast-banner");
  const badgeEl = document.getElementById("banner-badge-text");
  const textEl = document.getElementById("banner-main-text");
  if (!banner || !badgeEl || !textEl) return;

  badgeEl.textContent = badgeText;
  textEl.textContent = mainText;
  banner.classList.add("active");

  if (bannerTimeout) clearTimeout(bannerTimeout);
  bannerTimeout = setTimeout(() => banner.classList.remove("active"), 6500);
}

function closeBroadcastBanner() {
  const banner = document.getElementById("live-broadcast-banner");
  if (banner) banner.classList.remove("active");
}

function updateMarquee(text) {
  const marquee = document.getElementById("live-marquee-text");
  if (marquee) marquee.textContent = text;
}

function showToast(title, msg, isCancel = false) {
  const container = document.getElementById("toast-container");
  if (!container) return;

  const toast = document.createElement("div");
  toast.className = `toast-item ${isCancel ? 'cancel-toast' : ''}`;
  toast.innerHTML = `
    <div class="toast-icon">${isCancel ? '⚠️' : '📢'}</div>
    <div class="toast-content">
      <div class="toast-title">${title}</div>
      <div class="toast-msg">${msg}</div>
    </div>
  `;
  container.appendChild(toast);

  setTimeout(() => {
    if (toast.parentNode === container) container.removeChild(toast);
  }, 5000);
}

// 瀏覽器分頁標籤閃爍通知（切換到其他分頁或視窗也能一眼看到誰點餐）
let _titleFlashInterval = null;
const ORIGINAL_PAGE_TITLE = document.title || "智光高級商工職業學校資處科一仁午餐訂餐系統";

function flashPageTitle(message) {
  try {
    if (_titleFlashInterval) clearInterval(_titleFlashInterval);
    let showNotice = true;
    let count = 0;
    _titleFlashInterval = setInterval(() => {
      document.title = showNotice ? message : ORIGINAL_PAGE_TITLE;
      showNotice = !showNotice;
      count++;
      if (count > 24) { // 閃爍 12 秒後自動復原標題
        clearInterval(_titleFlashInterval);
        _titleFlashInterval = null;
        document.title = ORIGINAL_PAGE_TITLE;
      }
    }, 500);
  } catch (e) { }
}

window.addEventListener('focus', () => {
  if (_titleFlashInterval) {
    clearInterval(_titleFlashInterval);
    _titleFlashInterval = null;
    document.title = ORIGINAL_PAGE_TITLE;
  }
});

// =========================================================
// Service Worker 背景推播通知系統
// 即使網站關閉，仍可透過 Service Worker 背景 MQTT 收到通知！
// =========================================================
let swRegistration = null;
let pushNotifyEnabled = false;

/**
 * 初始化 Service Worker（頁面載入時自動執行）
 */
async function initServiceWorker() {
  if (!('serviceWorker' in navigator)) {
    console.log('[SW] 此瀏覽器不支援 Service Worker');
    return false;
  }
  try {
    swRegistration = await navigator.serviceWorker.register('/sw.js', { scope: '/' });
    console.log('[SW] 註冊成功:', swRegistration.scope);

    // 監聽 SW 發來的訊息
    navigator.serviceWorker.addEventListener('message', handleSwMessage);

    // 讀取使用者上次的推播設定
    const savedPushPref = localStorage.getItem('zg_push_notify_enabled_v1');
    if (savedPushPref === 'true') {
      // 上次有開啟，自動恢復
      await enablePushNotify(false); // false = 靜默恢復，不彈出 alert
    }
    updatePushNotifyBtn();
    return true;
  } catch (e) {
    console.warn('[SW] 註冊失敗:', e);
    return false;
  }
}

/**
 * 處理來自 Service Worker 的訊息（SW 收到新訂單時通知頁面）
 */
function handleSwMessage(event) {
  const msg = event.data;
  if (!msg) return;

  if (msg.type === 'SW_NEW_ORDER' && msg.order) {
    // SW 在背景收到新訂單，頁面已開啟時由頁面自己處理
    // （如果頁面可見，SW 不會發系統通知，改由頁面彈出 Toast/Banner）
    const order = msg.order;
    const deviceLabel = msg.deviceLabel || '同學';
    if (!AppState.orders[Number(order.seatId)]) {
      AppState.orders[Number(order.seatId)] = order;
      persistOrders();
      renderAllViews();
      triggerNewOrderEffects(order, deviceLabel);
    }
  } else if (msg.type === 'SW_ORDER_CANCELLED' && msg.seatId) {
    const sId = Number(msg.seatId);
    delete AppState.orders[sId];
    persistOrders();
    renderAllViews();
  } else if (msg.type === 'SW_ORDERS_RESET') {
    if (msg.resetEpoch) setResetEpoch(msg.resetEpoch);
    AppState.orders = {};
    AppState.activityFeed = [];
    persistOrders();
    renderAllViews();
  } else if (msg.type === 'SW_FOCUS_ORDER_TAB') {
    // 使用者點擊通知後，自動切換到點餐分頁
    switchTab('order');
  } else if (msg.type === 'SW_BG_MQTT_CONNECTED') {
    console.log('[SW] 背景 MQTT 已連線，即使關閉網站也能收到通知！');
    updatePushNotifyBtn();
  }
}

/**
 * 向 Service Worker 發送訊息
 */
function postToSW(data) {
  if (!navigator.serviceWorker || !navigator.serviceWorker.controller) return;
  navigator.serviceWorker.controller.postMessage(data);
}

/**
 * 請求通知權限並啟用背景推播
 */
async function enablePushNotify(showAlert = true) {
  if (!('Notification' in window)) {
    if (showAlert) alert('⚠️ 此瀏覽器不支援通知功能，請改用 Chrome 或 Safari 15+。');
    return false;
  }

  let permission = Notification.permission;
  if (permission === 'default') {
    // 首次請求授權
    permission = await Notification.requestPermission();
  }

  if (permission !== 'granted') {
    if (showAlert) {
      alert('❌ 通知權限被拒絕！\n\n請在瀏覽器網址列旁的鎖頭圖示 🔒 中，手動開啟「通知」權限，然後重新整理頁面。');
    }
    pushNotifyEnabled = false;
    localStorage.setItem('zg_push_notify_enabled_v1', 'false');
    updatePushNotifyBtn();
    return false;
  }

  pushNotifyEnabled = true;
  localStorage.setItem('zg_push_notify_enabled_v1', 'true');

  // 啟動 SW 背景 MQTT 監聽
  if (swRegistration) {
    postToSW({ type: 'SW_INIT_BACKGROUND_MQTT' });
  }

  // 立即發送一個測試通知，確認生效
  if (showAlert) {
    try {
      await swRegistration.showNotification('🔔 資處一仁訂餐通知已開啟！', {
        body: '從現在起，即使您關閉此網站，有同學點餐時您也會立即收到通知！ 🍱',
        icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍱</text></svg>',
        tag: 'sw-test-notify',
        requireInteraction: false,
        silent: false
      });
    } catch (e) {
      // 如果 SW 還沒安裝完成，用舊式 Notification
      new Notification('🔔 資處一仁訂餐通知已開啟！', {
        body: '從現在起，即使您關閉此網站，有同學點餐時您也會立即收到通知！ 🍱',
        tag: 'sw-test-notify'
      });
    }
  }

  updatePushNotifyBtn();
  return true;
}

/**
 * 停用背景推播
 */
function disablePushNotify() {
  pushNotifyEnabled = false;
  localStorage.setItem('zg_push_notify_enabled_v1', 'false');
  postToSW({ type: 'SW_STOP_BACKGROUND_MQTT' });
  updatePushNotifyBtn();
}

/**
 * 切換推播通知開關（綁定到按鈕 onclick）
 */
async function togglePushNotification() {
  if (pushNotifyEnabled) {
    disablePushNotify();
    showToast('🔕 背景通知已關閉', '關閉網站後將不再收到點餐推播', true);
  } else {
    const result = await enablePushNotify(true);
    if (result) {
      showToast('🔔 背景通知已開啟！', '即使關閉網站，有人點餐時也會馬上通知您！');
    }
  }
}
window.togglePushNotification = togglePushNotification;

/**
 * 更新推播通知按鈕狀態與外觀
 */
function updatePushNotifyBtn() {
  const btn = document.getElementById('push-notify-btn');
  if (!btn) return;

  const notifSupported = ('Notification' in window) && ('serviceWorker' in navigator);
  if (!notifSupported) {
    btn.style.display = 'none';
    return;
  }

  const permission = typeof Notification !== 'undefined' ? Notification.permission : 'default';

  if (pushNotifyEnabled && permission === 'granted') {
    btn.textContent = '🔔 關閉通知推播';
    btn.classList.add('notify-active');
    btn.style.opacity = '';
    btn.title = '點擊關閉：目前即使網站關閉也能收到新訂單通知';
  } else if (permission === 'denied') {
    btn.textContent = '🚫 通知已被封鎖';
    btn.classList.remove('notify-active');
    btn.style.opacity = '0.5';
    btn.title = '請在瀏覽器設定中手動開啟通知權限';
  } else {
    btn.textContent = '🔕 開啟關閉通知';
    btn.classList.remove('notify-active');
    btn.style.opacity = '';
    btn.style.background = '';
    btn.style.borderColor = '';
    btn.style.boxShadow = '';
    btn.title = '開啟後即使網站關閉也能收到新訂單通知';
  }
}


// 舊版相容：當頁面可見時的桌面通知（網站開著時的備援）
function requestNotificationPermission() {
  // 現在由 initServiceWorker 和 enablePushNotify 統一管理
  try {
    if (typeof Notification !== 'undefined' && Notification.permission === 'default' && pushNotifyEnabled) {
      Notification.requestPermission().catch(() => { });
    }
  } catch (e) { }
}

function triggerDesktopNotification(title, body) {
  // 優先走 SW Registration（在 SW 作用範圍內，所有通知都應透過 SW）
  try {
    if (swRegistration && typeof Notification !== 'undefined' && Notification.permission === 'granted') {
      swRegistration.showNotification(title, {
        body: body,
        icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍱</text></svg>',
        tag: 'lunch-order-' + Date.now(),
        requireInteraction: false
      }).catch(() => {});
      return;
    }
    if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
      const notif = new Notification(title, {
        body: body,
        icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍱</text></svg>',
        tag: 'lunch-order-' + Date.now()
      });
      setTimeout(() => { try { notif.close(); } catch (e) { } }, 6000);
    }
  } catch (e) { }
}

// =========================================================
// 畫面渲染與事件互動
// =========================================================

function renderMenuCatalog() {
  const container = document.getElementById("menu-items-grid");
  if (!container) return;
  container.innerHTML = "";

  MENU_ITEMS.forEach(item => {
    const card = document.createElement("div");
    card.className = `menu-card ${AppState.selectedMealId === item.id ? 'selected' : ''}`;
    card.id = `menu-card-${item.id}`;
    card.onclick = () => selectMenuItem(item.id);

    card.innerHTML = `
      <div class="menu-card-header">
        <div class="menu-icon">${item.icon}</div>
        <div class="menu-radio-check"></div>
      </div>
      <div>
        <div class="menu-name">${item.name}</div>
      </div>
      <div class="menu-price">$${item.price}<span>元</span></div>
    `;
    container.appendChild(card);
  });
}

function selectMenuItem(mealId) {
  AppState.selectedMealId = mealId;
  document.querySelectorAll(".menu-card").forEach(c => c.classList.remove("selected"));
  const activeCard = document.getElementById(`menu-card-${mealId}`);
  if (activeCard) activeCard.classList.add("selected");
  updateOrderSummaryBar();
}

function renderSeatOptions() {
  const select = document.getElementById("seat-select");
  if (!select) return;
  const currentVal = select.value;

  select.innerHTML = `<option value="">-- 請選擇座號 --</option>`;
  STUDENTS.forEach(student => {
    const isOrdered = !!AppState.orders[Number(student.id)];
    const option = document.createElement("option");
    option.value = String(student.id);
    option.textContent = `${student.id} 號 ${isOrdered ? '【已點餐】' : ''}`;
    select.appendChild(option);
  });

  if (currentVal) select.value = currentVal;
}

function handleSeatChange() {
  const seatSelect = document.getElementById("seat-select");
  const seatId = Number(seatSelect ? seatSelect.value : 0);
  const student = STUDENTS.find(s => Number(s.id) === seatId);

  const nameInput = document.getElementById("student-name-display");
  const passwordHint = document.getElementById("password-hint");
  const statusBox = document.getElementById("current-student-status");
  const cancelSection = document.getElementById("cancel-order-section");

  if (!student) {
    if (nameInput) nameInput.value = "";
    if (passwordHint) passwordHint.textContent = "提示：預設密碼為座號";
    if (statusBox) statusBox.style.display = "none";
    if (cancelSection) cancelSection.style.display = "none";
    updateOrderSummaryBar();
    return;
  }

  if (nameInput) nameInput.value = "";
  if (passwordHint) passwordHint.textContent = `提示：${student.id} 號同學的密碼即為座號「${student.id}」`;

  const order = AppState.orders[seatId];
  if (order) {
    if (statusBox) {
      statusBox.style.display = "flex";
      statusBox.innerHTML = `
        <div class="status-badge-row">
          <span style="font-weight: 700; color: #34d399;">已點餐狀態</span>
          <span class="status-tag ordered">已完成點餐</span>
        </div>
        <div style="font-size: 0.95rem; font-weight: 800; color: var(--text-main);">
          已點：${order.mealName}
        </div>
        <div style="font-size: 0.85rem; color: var(--text-muted); display: flex; justify-content: space-between;">
          <span>金額：$${order.price} 元</span>
          <span>時間：${order.time}</span>
        </div>
      `;
    }
    if (cancelSection) cancelSection.style.display = "flex";
  } else {
    if (statusBox) {
      statusBox.style.display = "flex";
      statusBox.innerHTML = `
        <div class="status-badge-row">
          <span style="font-weight: 700; color: var(--text-muted);">點餐狀態</span>
          <span class="status-tag unorder">尚未點餐</span>
        </div>
        <div style="font-size: 0.88rem; color: var(--text-muted);">
          一人限點一份餐點，請在右方選取餐點後點擊「送出訂單」。
        </div>
      `;
    }
    if (cancelSection) cancelSection.style.display = "none";
  }

  updateOrderSummaryBar();
}

function updateOrderSummaryBar() {
  const seatSelect = document.getElementById("seat-select");
  const seatId = Number(seatSelect ? seatSelect.value : 0);
  const student = STUDENTS.find(s => Number(s.id) === seatId);
  const meal = MENU_ITEMS.find(m => m.id === AppState.selectedMealId);

  const summarySeat = document.getElementById("summary-seat-name");
  const summaryMeal = document.getElementById("summary-meal-name");
  const summaryPrice = document.getElementById("summary-price-total");
  const submitBtn = document.getElementById("submit-order-btn");

  const isOrdered = seatId && AppState.orders[seatId];

  if (summarySeat) {
    summarySeat.textContent = student ? `${student.id} 號` : "尚未選擇座號";
  }

  if (summaryMeal) {
    summaryMeal.textContent = meal ? meal.name : "未選餐點";
  }

  if (summaryPrice) {
    summaryPrice.textContent = meal ? `$${meal.price} 元` : "$0 元";
  }

  if (submitBtn) {
    if (!student || !meal || isOrdered) {
      submitBtn.disabled = true;
      if (isOrdered) {
        submitBtn.innerHTML = `⚠️ 該座號已點過餐（一人限點一份）`;
      } else {
        submitBtn.innerHTML = `<span>🚀 送出訂單</span>`;
      }
    } else {
      submitBtn.disabled = false;
      submitBtn.innerHTML = `<span>🚀 確認送出訂單 ($${meal.price} 元)</span>`;
    }
  }
}

function handleSubmitOrder() {
  getAudioContext();
  requestNotificationPermission();
  const seatSelect = document.getElementById("seat-select");
  const seatId = Number(seatSelect ? seatSelect.value : 0);
  const passwordInput = document.getElementById("order-password");
  const password = passwordInput ? passwordInput.value.trim() : "";

  const student = STUDENTS.find(s => Number(s.id) === seatId);
  if (!student) {
    alert("請先選擇座號！");
    return;
  }

  if (AppState.orders[seatId]) {
    alert(`座號 ${student.id} 號 已經點過餐點囉！一人只能點一份餐點。\n如需更改，請先清除原有點餐。`);
    return;
  }

  const meal = MENU_ITEMS.find(m => m.id === AppState.selectedMealId);
  if (!meal) {
    alert("請先在右方選擇您要訂購的餐點！");
    return;
  }

  // 嚴格型態比對座號密碼
  if (String(password) !== String(student.id)) {
    alert(`訂餐密碼驗證錯誤！\n訂餐密碼即為您的座號：「${student.id}」，請輸入正確座號後再送出。`);
    if (passwordInput) {
      passwordInput.focus();
      passwordInput.select();
    }
    return;
  }

  const timeStr = getNowTime();
  const newOrder = {
    seatId: Number(student.id),
    mealId: String(meal.id),
    mealName: meal.name,
    price: Number(meal.price),
    time: timeStr,
    deviceType: DEVICE_LABEL,
    timestamp: Date.now()
  };

  AppState.orders[Number(student.id)] = newOrder;
  persistOrders();

  // 1. 立即觸發本機與廣播推播通知效應
  triggerNewOrderEffects(newOrder, '您');
  broadcastToAllDevices('ORDER_CREATED', newOrder);

  // 2. 向伺服器發送 POST /api/order，寫入 orders.json 持久化
  fetch(`${getServerBaseUrl()}/api/order`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ order: newOrder })
  }).then(r => r.json()).then(res => {
    if (res && res.success && res.orders) {
      applyOrdersUnionMerge(res.orders, '伺服器', true);
    }
  }).catch(e => console.warn('POST /api/order 靜默跳過:', e.message));

  renderAllViews();

  if (passwordInput) passwordInput.value = "";
}

function handleCancelOrder() {
  const seatSelect = document.getElementById("seat-select");
  const seatId = Number(seatSelect ? seatSelect.value : 0);
  const passwordInput = document.getElementById("cancel-password");
  const password = passwordInput ? passwordInput.value.trim() : "";

  const student = STUDENTS.find(s => Number(s.id) === seatId);
  if (!student) {
    alert("請先選擇要取消點餐的座號！");
    return;
  }

  if (!AppState.orders[seatId]) {
    alert(`座號 ${student.id} 號 目前尚未點餐，無需清除！`);
    return;
  }

  if (String(password) !== String(student.id)) {
    alert(`清除密碼錯誤！\n為防止他人不小心誤刪您的點餐，清除點餐密碼為您的座號「${student.id}」。`);
    if (passwordInput) {
      passwordInput.focus();
      passwordInput.select();
    }
    return;
  }

  const confirmed = confirm(`確認要清除【${student.id} 號】的點餐紀錄嗎？\n清除後該座號即可重新選取餐點。`);
  if (!confirmed) return;

  cancelOrder(seatId);

  if (passwordInput) passwordInput.value = "";
}

function selectStudentBySeatId(seatId) {
  const sId = Number(seatId);
  const seatSelect = document.getElementById("seat-select");
  if (seatSelect) {
    seatSelect.value = String(sId);
    handleSeatChange();
    switchTab('order');
    window.scrollTo({ top: 120, behavior: 'smooth' });
  }
}

// =========================================================
// 座位表即時對照圖渲染（6 排 31 人）
// =========================================================
function renderSeatingChart() {
  const container = document.getElementById("seating-classroom-grid");
  if (!container) return;

  // 首次渲染（容器為空）→ 全量建立 DOM
  const isFirstRender = container.children.length === 0;

  if (isFirstRender) {
    // 全量建立：建立完整 6 排 31 個課桌
    container.innerHTML = "";
    for (let r = 1; r <= 6; r++) {
      const rowStudents = STUDENTS.filter(s => Number(s.row) === r);
      const rowWrapper = document.createElement("div");
      rowWrapper.className = "seating-row-wrapper";

      const header = document.createElement("div");
      header.className = "row-header-label";
      header.innerHTML = `<span>第 ${r} 排</span> <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: normal;">(${rowStudents.length} 人)</span>`;
      rowWrapper.appendChild(header);

      const grid = document.createElement("div");
      grid.className = "seating-row-grid";

      for (let col = 0; col < 6; col++) {
        if (col < rowStudents.length) {
          const student = rowStudents[col];
          const sId = Number(student.id);
          const order = AppState.orders[sId];
          const desk = document.createElement("div");
          desk.className = `desk-card ${order ? 'ordered' : ''}`;
          desk.id = `desk-seat-${sId}`;
          desk.title = `點擊快速選擇 ${sId} 號`;
          desk.onclick = () => selectStudentBySeatId(sId);
          desk.innerHTML = _buildDeskInnerHTML(sId, order);
          grid.appendChild(desk);
        } else {
          const emptySlot = document.createElement("div");
          emptySlot.className = "desk-card empty-slot";
          emptySlot.innerHTML = `<span style="font-size: 0.8rem; color: #94a3b8;">走道 / 空位</span>`;
          grid.appendChild(emptySlot);
        }
      }

      rowWrapper.appendChild(grid);
      container.appendChild(rowWrapper);
    }
  } else {
    // 差異化更新：只更新有變動的座位，不重建整個 DOM
    STUDENTS.forEach(student => {
      const sId = Number(student.id);
      const desk = document.getElementById(`desk-seat-${sId}`);
      if (!desk) return;

      const order = AppState.orders[sId];
      const isOrdered = !!order;
      const wasOrdered = desk.classList.contains('ordered');

      // 只有狀態有變動才更新 DOM
      if (isOrdered !== wasOrdered || (isOrdered && desk.dataset.mealId !== String(order.mealId))) {
        desk.className = `desk-card ${isOrdered ? 'ordered' : ''}`;
        desk.dataset.mealId = isOrdered ? String(order.mealId) : '';
        desk.innerHTML = _buildDeskInnerHTML(sId, order);
      }
    });
  }
}

// 抽取課桌卡片 innerHTML 的輔助函式，避免重複 template literal
function _buildDeskInnerHTML(sId, order) {
  return `
    <div class="desk-top">
      <span class="desk-seat-num">${sId} 號</span>
      <span style="font-size: 0.78rem; font-weight: 700; color: ${order ? '#34d399' : '#94a3b8'};">
        ${order ? '✅ 已點' : '⏳ 待點'}
      </span>
    </div>
    <div class="desk-student-name">${sId} 號</div>
    ${order ? `
      <div class="desk-meal-info">
        <div class="desk-meal-name">${order.mealName}</div>
        <div class="desk-meal-price">$${order.price} 元</div>
      </div>
    ` : `
      <div class="desk-empty-meal">尚未點餐</div>
    `}
  `;
}



// =========================================================
// 後端管理統計面板渲染
// =========================================================
function renderAdminDashboard() {
  const orderedKeys = Object.keys(AppState.orders);
  const orderedCount = orderedKeys.length;
  const totalStudents = STUDENTS.length;
  const unorderCount = totalStudents - orderedCount;

  let totalAmount = 0;
  orderedKeys.forEach(key => {
    const o = AppState.orders[Number(key)];
    if (o) totalAmount += Number(o.price || 0);
  });

  const kpiOrdered = document.getElementById("kpi-ordered-count");
  const kpiAmount = document.getElementById("kpi-total-amount");
  const kpiPending = document.getElementById("kpi-pending-count");
  if (kpiOrdered) kpiOrdered.textContent = `${orderedCount} / ${totalStudents} 人`;
  if (kpiAmount) kpiAmount.textContent = `$${totalAmount.toLocaleString()} 元`;
  if (kpiPending) kpiPending.textContent = `${unorderCount} 人`;

  const printFooterOrdered = document.getElementById("print-footer-ordered");
  const printFooterTotal = document.getElementById("print-footer-total");
  if (printFooterOrdered) printFooterOrdered.textContent = `全班 31 人，已訂餐：${orderedCount} 人`;
  if (printFooterTotal) printFooterTotal.textContent = `總金額：$${totalAmount} 元`;

  // 餐點排行
  const mealStatList = document.getElementById("meal-stat-list");
  if (mealStatList) {
    mealStatList.innerHTML = "";
    const counts = {};
    MENU_ITEMS.forEach(m => counts[m.id] = 0);
    orderedKeys.forEach(k => {
      const o = AppState.orders[Number(k)];
      if (o && counts[o.mealId] !== undefined) counts[o.mealId]++;
    });

    const sortedMeals = [...MENU_ITEMS].sort((a, b) => counts[b.id] - counts[a.id]);

    sortedMeals.forEach(meal => {
      const count = counts[meal.id];
      const percent = orderedCount > 0 ? (count / orderedCount) * 100 : 0;
      const subtotal = count * meal.price;

      const row = document.createElement("div");
      row.className = "meal-stat-row";
      row.innerHTML = `
        <div class="meal-stat-labels">
          <span>${meal.icon} ${meal.name} ($${meal.price})</span>
          <span><strong>${count}</strong> 份 <span style="color: var(--text-muted); font-size: 0.8rem;">($${subtotal}元)</span></span>
        </div>
        <div class="meal-stat-bar-track">
          <div class="meal-stat-bar-fill" style="width: ${percent}%;"></div>
        </div>
      `;
      mealStatList.appendChild(row);
    });
  }

  // 未點餐名單
  const unorderContainer = document.getElementById("unorder-tags-container");
  const copyUnorderBtn = document.getElementById("copy-unorder-btn");
  if (unorderContainer) {
    unorderContainer.innerHTML = "";
    const unorderedStudents = STUDENTS.filter(s => !AppState.orders[Number(s.id)]);

    if (unorderedStudents.length === 0) {
      unorderContainer.innerHTML = `<span style="color: #34d399; font-weight: 700;">🎉 全班 31 位同學均已完成點餐！</span>`;
      if (copyUnorderBtn) copyUnorderBtn.style.display = "none";
    } else {
      unorderedStudents.forEach(s => {
        const tag = document.createElement("span");
        tag.className = "unorder-student-tag";
        tag.textContent = `${s.id}號`;
        tag.style.cursor = "pointer";
        tag.onclick = () => selectStudentBySeatId(Number(s.id));
        unorderContainer.appendChild(tag);
      });
      if (copyUnorderBtn) {
        copyUnorderBtn.style.display = "inline-flex";
        copyUnorderBtn.onclick = () => {
          const listText = unorderedStudents.map(s => `${s.id}號`).join("、");
          navigator.clipboard.writeText(`【資處一仁午餐未點餐同學提醒】\n共 ${unorderedStudents.length} 人尚未點餐：\n${listText}`);
          alert("已複製未點餐名單至剪貼簿！可直接貼在班級群組提醒同學。");
        };
      }
    }
  }

  // 明細表
  const tableBody = document.getElementById("admin-order-table-body");
  if (tableBody) {
    tableBody.innerHTML = "";
    STUDENTS.forEach(student => {
      const sId = Number(student.id);
      const order = AppState.orders[sId];
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td style="font-weight: 800;">${sId} 號</td>
        <td>第 ${student.row} 排</td>
        <td>${order ? `<span style="color: #34d399; font-weight: 800;">${order.mealName}</span>` : `<span style="color: var(--text-light);">尚未點餐</span>`}</td>
        <td style="font-weight: 700;">${order ? `$${order.price} 元` : '-'}</td>
        <td style="font-size: 0.85rem; color: var(--text-muted);">${order ? order.time : '-'}</td>
        <td>
          ${order ? `
            <button class="action-btn" style="padding: 4px 10px; font-size: 0.8rem; background: var(--danger-light); color: var(--danger-hover);" onclick="quickAdminCancel(${sId})">
              取消
            </button>
          ` : `
            <button class="action-btn" style="padding: 4px 10px; font-size: 0.8rem; background: var(--primary-light); color: var(--primary);" onclick="selectStudentBySeatId(${sId})">
              幫點餐
            </button>
          `}
        </td>
      `;
      tableBody.appendChild(tr);
    });
  }
}

function quickAdminCancel(seatId) {
  const sId = Number(seatId);
  const pwd = prompt(`【管理員取消驗證】\n請輸入座號「${sId}」以確認取消【${sId}號】的點餐：`);
  if (String(pwd) === String(sId)) {
    cancelOrder(sId);
  } else if (pwd !== null) {
    alert("密碼輸入錯誤，取消操作已終止！");
  }
}

function exportOrdersToCSV() {
  const now = new Date();
  const dateStr = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`;

  // 只保留：座號/編號, 訂餐內容, 價格
  let csvContent = "\uFEFF座號/編號,訂餐內容,價格\n";
  STUDENTS.forEach(s => {
    const sId = Number(s.id);
    const o = AppState.orders[sId];
    csvContent += `"${sId}","${o ? o.mealName : ''}","${o ? o.price : ''}"\n`;
  });

  csvContent += "\n--- 餐點品項數量總計 ---\n品項名稱,單價,總份數,小計金額\n";
  MENU_ITEMS.forEach(m => {
    const count = Object.values(AppState.orders).filter(o => o.mealId === m.id).length;
    if (count > 0) {
      csvContent += `"${m.name}","${m.price}","${count}","${count * m.price}"\n`;
    }
  });

  let totalAmount = 0;
  Object.values(AppState.orders).forEach(o => totalAmount += Number(o.price));
  csvContent += `\n"總計訂餐人數：${Object.keys(AppState.orders).length} 人","全班總金額：$${totalAmount} 元"\n`;

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", `資處一仁午餐訂單_${dateStr}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

// UI 點擊重置按鈕處理函式
function handleResetAllOrders() {
  const confirmText = prompt("⚠️ 警告：這將會清除全班 31 位同學的所有訂餐紀錄！\n請輸入重置密碼（0901）：");
  if (confirmText !== "0901") {
    if (confirmText !== null) alert("重置密碼錯誤（密碼為 0901），已取消重置操作。");
    return;
  }

  // 請操作者輸入座號供動態牆顯示
  const seatInput = prompt("請輸入您的座號（將顯示在全班動態牆上）：");
  const seatNum = seatInput ? seatInput.trim() : null;
  const seatId = Number(seatNum);

  // 座號必須為 1~31 的有效數字，否則不帶座號
  const validSeat = (!isNaN(seatId) && seatId >= 1 && seatId <= 31) ? seatId : null;

  resetSystem(validSeat);
  alert("已成功清除全班點餐紀錄並重設為純淨初始狀態！");
}

function printSeatingChart() {
  switchTab('seating');
  setTimeout(() => window.print(), 250);
}

function switchTab(tabId) {
  document.querySelectorAll(".tab-button").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.tab === tabId);
  });

  const orderContent = document.getElementById("order-tab-content");
  const seatingContent = document.getElementById("seating-tab-content");
  const adminContent = document.getElementById("admin-tab-content");

  if (orderContent) orderContent.style.display = tabId === 'order' ? 'block' : 'none';
  if (seatingContent) seatingContent.style.display = tabId === 'seating' ? 'block' : 'none';
  if (adminContent) adminContent.style.display = tabId === 'admin' ? 'block' : 'none';
}

// =========================================================
// renderAllViews：零延遲直接執行，移除 rAF 節流避免背景標籤延遲
// =========================================================
function renderAllViews() {
  renderSeatOptions();
  handleSeatChange();
  renderSeatingChart();
  renderAdminDashboard();
}
window.renderAllViews = renderAllViews;


function toggleActivityDrawer() {
  const drawer = document.getElementById("activity-drawer");
  if (drawer) drawer.classList.toggle("open");
}

function renderActivityFeed() {
  const list = document.getElementById("activity-list");
  if (!list) return;
  list.innerHTML = "";

  if (!AppState.activityFeed || AppState.activityFeed.length === 0) {
    list.innerHTML = `<div style="text-align: center; color: var(--text-muted); padding: 20px;">尚無點餐動態</div>`;
    return;
  }

  AppState.activityFeed.forEach(item => {
    const div = document.createElement("div");
    let actClass = '';
    if (item.type === 'cancel') actClass = 'cancel-act';
    else if (item.type === 'reset') actClass = 'reset-act';
    div.className = `activity-item ${actClass}`;
    div.innerHTML = `
      <div class="activity-item-time">⏱️ ${item.time}</div>
      <div class="activity-item-title">${item.title}</div>
      <div class="activity-item-desc">${item.desc}</div>
    `;
    list.appendChild(div);
  });
}

function toggleSound() {
  AppState.soundEnabled = !AppState.soundEnabled;
  const btn = document.getElementById("audio-toggle-btn");
  if (btn) {
    btn.innerHTML = AppState.soundEnabled ? '🔊 點餐音效：開' : '🔇 點餐音效：關';
  }
  if (AppState.soundEnabled) {
    playSuccessChime();
  }
}

// ============================================================
// 頁面載入時補執行今日下午一點自動清空（GitHub Pages 降級備援）
// 注意：在本地伺服器模式下，重置由 server.js 嚴格於平日 13:00 執行，
// 前端頁面關閉或重開時「絕對不自行清空訂單」，確保訂單永久留存！
// ============================================================
function checkAndAutoResetOnLoad() {
  // 關鍵防護：若在 Node.js 伺服器模式下，13:00 自動重置完全由 server.js 負責，前端開啟時絕不自行清空
  if (IS_LOCAL_SERVER) {
    console.log('[自動重置防護] 伺服器託管模式：訂單由 server.js 嚴格管理，開啟頁面時絕不清空');
    return;
  }

  const TW_OFFSET_MS = 8 * 60 * 60 * 1000;
  const nowUTC = Date.now();
  const nowTW = new Date(nowUTC + TW_OFFSET_MS);

  // 只在週一至週五觸發
  const day = nowTW.getUTCDay();
  if (day < 1 || day > 5) return;

  // 計算今天台灣時間 13:00 對應的 UTC timestamp
  const todayNoonUTC = Date.UTC(
    nowTW.getUTCFullYear(),
    nowTW.getUTCMonth(),
    nowTW.getUTCDate(),
    5, 0, 0  // UTC 05:00 = 台灣時間 13:00
  );

  const lastReset = getResetEpoch();

  // 關鍵修復：只有當 lastReset > 0（代表本機曾經記錄過舊重置）且早於今天 13:00、且現在已過今天 13:00 時，才執行跨天自動清空。
  // 新連線裝置 (lastReset === 0) 絕對不可抹除全班已有訂單！
  if (lastReset > 0 && nowUTC >= todayNoonUTC && lastReset < todayNoonUTC) {
    console.log('[載入自動清空] 偵測到本機上記錄的上次重置早於今日 13:00，執行本機跨日清空...');
    const resetAt = todayNoonUTC;
    setResetEpoch(resetAt);
    AppState.orders = {};
    AppState.activityFeed = [];
    persistOrders();
    renderAllViews();
  }
}

// 頁面初始化
document.addEventListener("DOMContentLoaded", async () => {
  loadLocalData();

  if (location.protocol.startsWith('http')) {
    await detectServerMode();
  }

  // 載入資料後檢查跨日排程清空（僅限 static / file:// 模式）
  checkAndAutoResetOnLoad();

  renderMenuCatalog();
  renderSeatOptions();
  renderSeatingChart();
  renderAdminDashboard();
  renderActivityFeed();
  initSyncEngine();

  if (MENU_ITEMS.length > 0) {
    selectMenuItem(MENU_ITEMS[0].id);
  }

  // 初始化 Service Worker 背景推播通知系統
  // 即使網站關閉，有人點餐時仍可透過 SW 背景 MQTT 收到系統通知！
  initServiceWorker().then(() => {
    updatePushNotifyBtn();
  });

  document.body.addEventListener('click', () => {
    getAudioContext();
  }, { once: true });

  if (location.protocol.startsWith('http')) {
    fetchServerOrders();
  }
});


// ============================================================
// 瀏覽器端排程（降級備援）
// 只在非本機伺服器模式時啟動（例如直接用 file:// 開啟頁面）
// 當用區域網路 node server.js 執行時，由伺服器端負責排程
// ============================================================
(function autoResetWeekdayScheduler() {
  // 如果是本地伺服器模式，由伺服器端負責排程；若是 GitHub Pages 則由瀏覽器端負責
  if (IS_LOCAL_SERVER) {
    console.log('[瀏覽器端自動重置] 連線到本地伺服器，13:00 自動重置交由伺服器負責');
    return;
  }

  const TW_OFFSET_MS = 8 * 60 * 60 * 1000;

  function msUntilNextWeekdayNoon() {
    const nowUTC = Date.now();
    const todayTW = new Date(nowUTC + TW_OFFSET_MS);
    // 台灣時間 13:00 = UTC 05:00
    // todayTW 是把 UTC + 8h 建出的物件，getUTC* 取到的就是台灣當天日期
    const todayResetUTC = Date.UTC(
      todayTW.getUTCFullYear(),
      todayTW.getUTCMonth(),
      todayTW.getUTCDate(),
      5, 0, 0  // UTC 05:00 = 台灣時間 13:00
    );
    let candidateUTC = nowUTC < todayResetUTC ? todayResetUTC : todayResetUTC + 86400000;
    for (let i = 0; i < 7; i++) {
      const day = new Date(candidateUTC + TW_OFFSET_MS).getUTCDay();
      if (day >= 1 && day <= 5) break;
      candidateUTC += 86400000;
    }
    return candidateUTC - nowUTC;
  }

  function runAutoReset() {
    const twNow = new Date(Date.now() + TW_OFFSET_MS);
    const day = twNow.getUTCDay();
    if (day >= 1 && day <= 5) {
      console.log('[瀏覽器端自動重置] 台灣時間 13:00，執行每日訂餐清空...');
      resetSystem();
      addActivityItem('reset', '⏰ 13:00 系統自動清空', '每日下午一點自動重置，所有座位已重新開放', getNowTime());
    }
    const delay = msUntilNextWeekdayNoon();
    setTimeout(runAutoReset, delay);
  }

  const firstDelay = msUntilNextWeekdayNoon();
  setTimeout(runAutoReset, firstDelay);
  console.log('[瀏覽器端自動重置（降級備援）] 週一至週五台灣時間 13:00 自動清空功能已啟用');
})();
// ============================================================

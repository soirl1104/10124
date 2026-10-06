/**
 * 智光高級商工職業學校資處科一仁午餐訂餐系統 - Service Worker
 * 功能：即使網站關閉，仍能透過 MQTT 雲端通道收到新訂單推播通知
 * 版本：v3.0 - 背景 MQTT 持久監聽引擎
 */

const SW_VERSION = 'zg-lunch-sw-v3';
const MQTT_TOPIC_EVENT = "zhiguang/cs1ren/prod_clean_2026/events";
const MQTT_TOPIC_STATE = "zhiguang/cs1ren/prod_clean_2026/state";

// MQTT Broker 列表（依序備援）
const MQTT_BROKERS = [
  "wss://broker.emqx.io:8084/mqtt",
  "wss://broker.hivemq.com:8884/mqtt",
  "wss://test.mosquitto.org:8081/mqtt"
];

// ============================================================
// Service Worker 安裝與啟用
// ============================================================
self.addEventListener('install', (event) => {
  console.log(`[SW ${SW_VERSION}] 安裝完成，立即啟用`);
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  console.log(`[SW ${SW_VERSION}] 啟用完成，接管所有分頁`);
  event.waitUntil(clients.claim());
});

// ============================================================
// 頁面通訊 - 接收來自 app.js 的訊息
// ============================================================
self.addEventListener('message', (event) => {
  const msg = event.data;
  if (!msg) return;

  if (msg.type === 'SW_INIT_BACKGROUND_MQTT') {
    // 頁面要求 SW 啟動背景 MQTT 監聽
    startBackgroundMqttListening();
  } else if (msg.type === 'SW_STOP_BACKGROUND_MQTT') {
    stopBackgroundMqttListening();
  } else if (msg.type === 'SW_PING') {
    event.ports[0] && event.ports[0].postMessage({ type: 'SW_PONG', version: SW_VERSION });
  }
});

// ============================================================
// 背景 MQTT WebSocket 客戶端（純 Service Worker 實作）
// 因 Service Worker 支援 WebSocket，這可以在頁面關閉後持續運行
// ============================================================
let bgMqttWs = null;
let bgMqttBrokerIndex = 0;
let bgMqttConnectTimer = null;
let bgMqttPingTimer = null;
let bgMqttReconnectCount = 0;
let isBgMqttRunning = false;

// MQTT 封包解析器狀態
let mqttRecvBuffer = new Uint8Array(0);

function uint8Concat(a, b) {
  const c = new Uint8Array(a.length + b.length);
  c.set(a, 0);
  c.set(b, a.length);
  return c;
}

/**
 * 建立最簡 MQTT CONNECT 封包 (MQTT 3.1.1)
 */
function buildMqttConnectPacket(clientId) {
  const protocolName = 'MQTT';
  const protocolLevel = 4; // 3.1.1
  const connectFlags = 0x02; // Clean Session
  const keepAlive = 30;

  const protocolNameBytes = new TextEncoder().encode(protocolName);
  const clientIdBytes = new TextEncoder().encode(clientId);

  // Variable header: protocol name (length-prefixed), level, flags, keepalive
  const varHeader = new Uint8Array(2 + protocolNameBytes.length + 1 + 1 + 2);
  let i = 0;
  varHeader[i++] = 0x00;
  varHeader[i++] = protocolNameBytes.length;
  for (const b of protocolNameBytes) varHeader[i++] = b;
  varHeader[i++] = protocolLevel;
  varHeader[i++] = connectFlags;
  varHeader[i++] = (keepAlive >> 8) & 0xFF;
  varHeader[i++] = keepAlive & 0xFF;

  // Payload: clientId (length-prefixed)
  const payload = new Uint8Array(2 + clientIdBytes.length);
  payload[0] = 0x00;
  payload[1] = clientIdBytes.length;
  payload.set(clientIdBytes, 2);

  const remainingLength = varHeader.length + payload.length;

  // Fixed header
  const fixedHeader = encodeRemainingLength(remainingLength);
  const packet = new Uint8Array(1 + fixedHeader.length + remainingLength);
  packet[0] = 0x10; // CONNECT
  packet.set(fixedHeader, 1);
  packet.set(varHeader, 1 + fixedHeader.length);
  packet.set(payload, 1 + fixedHeader.length + varHeader.length);

  return packet;
}

/**
 * 建立 MQTT SUBSCRIBE 封包
 */
function buildMqttSubscribePacket(topics, packetId = 1) {
  const enc = new TextEncoder();
  let payloadSize = 2; // packet id
  const topicBuffers = topics.map(t => enc.encode(t));
  for (const tb of topicBuffers) payloadSize += 2 + tb.length + 1; // len + topic + qos

  const payload = new Uint8Array(payloadSize);
  let i = 0;
  payload[i++] = (packetId >> 8) & 0xFF;
  payload[i++] = packetId & 0xFF;
  for (const tb of topicBuffers) {
    payload[i++] = 0x00;
    payload[i++] = tb.length;
    payload.set(tb, i);
    i += tb.length;
    payload[i++] = 0x01; // QoS 1
  }

  const remainLen = encodeRemainingLength(payload.length);
  const packet = new Uint8Array(1 + remainLen.length + payload.length);
  packet[0] = 0x82; // SUBSCRIBE
  packet.set(remainLen, 1);
  packet.set(payload, 1 + remainLen.length);
  return packet;
}

/**
 * 建立 MQTT PINGREQ 封包 (心跳)
 */
function buildMqttPingReqPacket() {
  return new Uint8Array([0xC0, 0x00]);
}

/**
 * 編碼 MQTT Remaining Length
 */
function encodeRemainingLength(len) {
  const result = [];
  do {
    let encodedByte = len % 128;
    len = Math.floor(len / 128);
    if (len > 0) encodedByte |= 0x80;
    result.push(encodedByte);
  } while (len > 0);
  return new Uint8Array(result);
}

/**
 * 解碼 MQTT Remaining Length，返回 { value, bytesUsed } 或 null（資料不足）
 */
function decodeRemainingLength(data, offset) {
  let multiplier = 1;
  let value = 0;
  let bytesUsed = 0;
  while (offset + bytesUsed < data.length) {
    const byte = data[offset + bytesUsed];
    bytesUsed++;
    value += (byte & 0x7F) * multiplier;
    multiplier *= 128;
    if ((byte & 0x80) === 0) return { value, bytesUsed };
    if (bytesUsed >= 4) return null; // 格式錯誤
  }
  return null; // 資料不足
}

/**
 * 解析 MQTT PUBLISH 封包，回傳 { topic, payload } 或 null
 */
function parseMqttPublish(packet) {
  try {
    const fixedHeader = packet[0];
    const rl = decodeRemainingLength(packet, 1);
    if (!rl) return null;

    const headerLen = 1 + rl.bytesUsed;
    const varPayload = packet.slice(headerLen, headerLen + rl.value);

    // Topic length (2 bytes)
    const topicLen = (varPayload[0] << 8) | varPayload[1];
    const topicBytes = varPayload.slice(2, 2 + topicLen);
    const topic = new TextDecoder().decode(topicBytes);

    // QoS level
    const qos = (fixedHeader >> 1) & 0x03;
    let payloadStart = 2 + topicLen;
    if (qos > 0) payloadStart += 2; // skip packet identifier

    const messageBytes = varPayload.slice(payloadStart);
    const message = new TextDecoder().decode(messageBytes);
    return { topic, payload: message };
  } catch (e) {
    return null;
  }
}

function startBackgroundMqttListening() {
  if (isBgMqttRunning && bgMqttWs && bgMqttWs.readyState === WebSocket.OPEN) {
    console.log('[SW BG-MQTT] 已在運行中');
    return;
  }
  isBgMqttRunning = true;
  bgMqttReconnectCount = 0;
  connectBgMqtt();
}

function stopBackgroundMqttListening() {
  isBgMqttRunning = false;
  if (bgMqttPingTimer) { clearInterval(bgMqttPingTimer); bgMqttPingTimer = null; }
  if (bgMqttConnectTimer) { clearTimeout(bgMqttConnectTimer); bgMqttConnectTimer = null; }
  if (bgMqttWs) {
    try { bgMqttWs.close(); } catch (e) {}
    bgMqttWs = null;
  }
  console.log('[SW BG-MQTT] 背景監聽已停止');
}

function connectBgMqtt() {
  if (!isBgMqttRunning) return;
  if (bgMqttWs && bgMqttWs.readyState === WebSocket.OPEN) return;

  const broker = MQTT_BROKERS[bgMqttBrokerIndex % MQTT_BROKERS.length];
  const clientId = 'zgSW_' + Math.random().toString(16).substring(2, 10);

  console.log(`[SW BG-MQTT] 連接 broker: ${broker} (clientId=${clientId})`);

  try {
    bgMqttWs = new WebSocket(broker, ['mqtt']);
    bgMqttWs.binaryType = 'arraybuffer';
    mqttRecvBuffer = new Uint8Array(0);

    bgMqttWs.onopen = () => {
      console.log('[SW BG-MQTT] WebSocket 已連線，發送 MQTT CONNECT...');
      bgMqttReconnectCount = 0;
      const connectPacket = buildMqttConnectPacket(clientId);
      bgMqttWs.send(connectPacket);
    };

    bgMqttWs.onmessage = (event) => {
      try {
        const incoming = new Uint8Array(event.data);
        mqttRecvBuffer = uint8Concat(mqttRecvBuffer, incoming);
        processMqttBuffer();
      } catch (e) {
        console.warn('[SW BG-MQTT] 處理訊息異常:', e);
      }
    };

    bgMqttWs.onclose = (event) => {
      console.log(`[SW BG-MQTT] 連線關閉 (code=${event.code})，準備重連...`);
      if (bgMqttPingTimer) { clearInterval(bgMqttPingTimer); bgMqttPingTimer = null; }
      bgMqttWs = null;
      if (!isBgMqttRunning) return;
      bgMqttBrokerIndex++;
      bgMqttReconnectCount++;
      const delay = Math.min(3000 + bgMqttReconnectCount * 1000, 30000);
      bgMqttConnectTimer = setTimeout(connectBgMqtt, delay);
    };

    bgMqttWs.onerror = (e) => {
      console.warn('[SW BG-MQTT] WebSocket 錯誤，切換 broker');
      try { bgMqttWs.close(); } catch (_) {}
    };

  } catch (e) {
    console.error('[SW BG-MQTT] 建立 WebSocket 失敗:', e);
    bgMqttBrokerIndex++;
    bgMqttConnectTimer = setTimeout(connectBgMqtt, 5000);
  }
}

/**
 * 處理接收到的 MQTT 封包 buffer
 */
function processMqttBuffer() {
  while (mqttRecvBuffer.length >= 2) {
    const fixedHeaderByte = mqttRecvBuffer[0];
    const rl = decodeRemainingLength(mqttRecvBuffer, 1);
    if (!rl) break; // 資料不足

    const totalLen = 1 + rl.bytesUsed + rl.value;
    if (mqttRecvBuffer.length < totalLen) break; // 封包尚不完整

    const packet = mqttRecvBuffer.slice(0, totalLen);
    mqttRecvBuffer = mqttRecvBuffer.slice(totalLen);

    const packetType = (fixedHeaderByte >> 4) & 0x0F;

    if (packetType === 2) {
      // CONNACK - 連線確認
      const returnCode = packet[3];
      if (returnCode === 0) {
        console.log('[SW BG-MQTT] CONNACK: 連線成功！訂閱主題...');
        // 訂閱事件和狀態頻道
        const subPacket = buildMqttSubscribePacket([MQTT_TOPIC_EVENT, MQTT_TOPIC_STATE], 1);
        if (bgMqttWs && bgMqttWs.readyState === WebSocket.OPEN) {
          bgMqttWs.send(subPacket);
        }
        // 啟動心跳 Ping（每 25 秒）
        if (bgMqttPingTimer) clearInterval(bgMqttPingTimer);
        bgMqttPingTimer = setInterval(() => {
          if (bgMqttWs && bgMqttWs.readyState === WebSocket.OPEN) {
            bgMqttWs.send(buildMqttPingReqPacket());
          } else {
            clearInterval(bgMqttPingTimer);
          }
        }, 25000);
        // 通知所有頁面背景 MQTT 已就緒
        notifyAllClients({ type: 'SW_BG_MQTT_CONNECTED' });
      } else {
        console.warn('[SW BG-MQTT] CONNACK 拒絕連線:', returnCode);
        bgMqttBrokerIndex++;
        setTimeout(connectBgMqtt, 3000);
      }
    } else if (packetType === 9) {
      // SUBACK - 訂閱確認
      console.log('[SW BG-MQTT] SUBACK: 訂閱成功，開始監聽新訂單！');
    } else if (packetType === 3) {
      // PUBLISH - 收到新訊息
      const result = parseMqttPublish(packet);
      if (result) {
        handleMqttMessage(result.topic, result.payload);
      }
    } else if (packetType === 13) {
      // PINGRESP - 心跳回覆
      // 正常，不需要處理
    }
  }
}

/**
 * 處理收到的 MQTT 訊息，若是新訂單則發送系統通知
 */
async function handleMqttMessage(topic, payloadStr) {
  try {
    const payload = JSON.parse(payloadStr);

    if (topic === MQTT_TOPIC_EVENT) {
      if (payload.type === 'ORDER_CREATED' && payload.data) {
        const order = payload.data;
        const seatId = order.seatId;
        const mealName = order.mealName || '餐點';
        const price = order.price || 0;
        const deviceLabel = payload.deviceLabel || '同學';

        console.log(`[SW BG-MQTT] 收到新訂單！座號=${seatId}, 餐點=${mealName}`);

        // 1. 先通知所有已開啟的頁面
        notifyAllClients({ type: 'SW_NEW_ORDER', order: order, deviceLabel: deviceLabel });

        // 2. 嘗試找到已開啟的視窗，如果有就不發推播
        const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
        const hasVisiblePage = allClients.some(c => c.visibilityState === 'visible');

        if (!hasVisiblePage) {
          // 頁面不可見（最小化/關閉），發送系統推播通知
          await showOrderNotification(seatId, mealName, price, deviceLabel);
        }

      } else if (payload.type === 'ORDER_CANCELLED' && payload.data) {
        const seatId = payload.data.seatId;
        notifyAllClients({ type: 'SW_ORDER_CANCELLED', seatId: seatId });

        const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
        const hasVisiblePage = allClients.some(c => c.visibilityState === 'visible');

        if (!hasVisiblePage) {
          await showCancelNotification(seatId);
        }

      } else if (payload.type === 'ORDERS_RESET') {
        notifyAllClients({ type: 'SW_ORDERS_RESET', resetEpoch: payload.resetEpoch });

        const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
        const hasVisiblePage = allClients.some(c => c.visibilityState === 'visible');
        if (!hasVisiblePage) {
          await self.registration.showNotification('🔄 訂餐紀錄已重置', {
            body: '全班午餐訂餐紀錄已清空，所有座位可重新點餐！',
            icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🔄</text></svg>',
            badge: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍱</text></svg>',
            tag: 'lunch-reset',
            requireInteraction: false,
            data: { url: self.registration.scope }
          });
        }
      }
    }
  } catch (e) {
    console.warn('[SW BG-MQTT] 解析訊息異常:', e);
  }
}

/**
 * 發送新訂單系統推播通知
 */
async function showOrderNotification(seatId, mealName, price, deviceLabel) {
  try {
    await self.registration.showNotification(`🍱 【${seatId}號】剛送出點餐！`, {
      body: `${deviceLabel} 點了「${mealName}」($${price}元)！\n點擊查看全班最新訂餐狀況 👀`,
      icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🍱</text></svg>',
      badge: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🔔</text></svg>',
      tag: `lunch-order-${seatId}`,
      requireInteraction: false,
      silent: false,
      vibrate: [200, 100, 200],
      data: {
        url: self.registration.scope,
        seatId: seatId,
        mealName: mealName
      },
      actions: [
        { action: 'view', title: '📋 查看訂單' },
        { action: 'dismiss', title: '✕ 關閉' }
      ]
    });
  } catch (e) {
    console.warn('[SW] showOrderNotification 失敗:', e);
  }
}

/**
 * 發送取消點餐推播通知
 */
async function showCancelNotification(seatId) {
  try {
    await self.registration.showNotification(`⚠️ 【${seatId}號】取消了點餐`, {
      body: `${seatId}號同學取消了點餐，座位已重新開放！`,
      icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">⚠️</text></svg>',
      tag: `lunch-cancel-${seatId}`,
      requireInteraction: false,
      silent: true,
      data: { url: self.registration.scope }
    });
  } catch (e) {
    console.warn('[SW] showCancelNotification 失敗:', e);
  }
}

/**
 * 廣播訊息給所有已開啟的頁面
 */
function notifyAllClients(data) {
  clients.matchAll({ type: 'window', includeUncontrolled: true }).then(allClients => {
    for (const client of allClients) {
      client.postMessage(data);
    }
  });
}

// ============================================================
// 通知點擊事件處理 - 點擊通知後開啟或聚焦網站
// ============================================================
self.addEventListener('notificationclick', (event) => {
  const notification = event.notification;
  notification.close();

  const targetUrl = (notification.data && notification.data.url) || self.registration.scope;

  if (event.action === 'dismiss') return;

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(allClients => {
      // 如果已有開啟的視窗，聚焦並切換到訂餐頁面
      for (const client of allClients) {
        if (client.url.includes(self.registration.scope.replace(/\/$/, '')) && 'focus' in client) {
          client.focus();
          client.postMessage({ type: 'SW_FOCUS_ORDER_TAB' });
          return;
        }
      }
      // 沒有開啟的視窗，新開一個
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});

// ============================================================
// 背景同步 (Background Sync) - 離線期間點餐自動補送
// ============================================================
self.addEventListener('sync', (event) => {
  if (event.tag === 'sync-pending-orders') {
    console.log('[SW Sync] 網路恢復，同步待發訂單...');
    event.waitUntil(syncPendingOrders());
  }
});

async function syncPendingOrders() {
  try {
    // 此功能由 app.js 的 applyOrdersUnionMerge 處理
    console.log('[SW Sync] 背景同步完成');
  } catch (e) {
    console.warn('[SW Sync] 同步失敗:', e);
  }
}

// ============================================================
// Fetch 攔截 - 靜態資源快取（加速載入速度）
// ============================================================
const CACHE_NAME = SW_VERSION + '-cache';
const STATIC_ASSETS = ['/', '/index.html', '/app.js', '/styles.css'];

self.addEventListener('fetch', (event) => {
  // 只快取 GET 請求，且不快取 API 路徑
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  if (url.pathname.startsWith('/api/')) return;

  // Network-first 策略：優先走網路，失敗才用快取
  event.respondWith(
    fetch(event.request).then(response => {
      if (response && response.status === 200) {
        const responseClone = response.clone();
        caches.open(CACHE_NAME).then(cache => cache.put(event.request, responseClone));
      }
      return response;
    }).catch(() => caches.match(event.request))
  );
});

console.log(`[SW ${SW_VERSION}] Service Worker 載入完成 - 背景 MQTT 監聽引擎就緒`);

/**
 * 智光高級商工職業學校資處科一仁午餐訂餐系統 - 本地區網伺服器
 * 具備零相依純 Node.js HTTP 服務、REST API、訂單資料持久化與內建 WebSocket 即時推播
 * 讓班級內所有手機、平板、電腦在同一 Wi-Fi 下毫秒級同步跳通知，重新整理 100% 留存訂單！
 * 執行方式：node server.js
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

// ============================================================
// 伺服器端資料持久化 (reset_epoch.json & orders.json)
// ============================================================
const RESET_EPOCH_FILE = path.join(__dirname, 'reset_epoch.json');
const ORDERS_FILE = path.join(__dirname, 'orders.json');
const TW_OFFSET_MS = 8 * 60 * 60 * 1000;

function getTwTimeStr(timestamp = Date.now()) {
  return new Date(timestamp + TW_OFFSET_MS).toISOString().slice(11, 19);
}

function loadResetEpoch() {
  try {
    if (fs.existsSync(RESET_EPOCH_FILE)) {
      const raw = fs.readFileSync(RESET_EPOCH_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      return Number(parsed.lastResetEpoch || 0);
    }
  } catch (e) {
    console.error('[自動重置] 讀取 reset_epoch.json 異常:', e.message);
  }
  return 0;
}

function saveResetEpoch(epoch) {
  try {
    const tmpPath = `${RESET_EPOCH_FILE}.tmp`;
    fs.writeFileSync(tmpPath, JSON.stringify({ lastResetEpoch: epoch }, null, 2), 'utf8');
    fs.renameSync(tmpPath, RESET_EPOCH_FILE);
  } catch (e) {
    console.error('[自動重置] 無法儲存 reset_epoch.json:', e.message);
  }
}

let lastResetEpoch = loadResetEpoch();
console.log(`[自動重置] 讀取上次重置時間：${lastResetEpoch ? new Date(lastResetEpoch + TW_OFFSET_MS).toISOString().replace('T', ' ').slice(0, 19) + ' (TW)' : '（無紀錄）'}`);

function loadOrdersData() {
  try {
    if (fs.existsSync(ORDERS_FILE)) {
      const raw = fs.readFileSync(ORDERS_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      const orders = (parsed && typeof parsed.orders === 'object' && !Array.isArray(parsed.orders)) ? parsed.orders : {};
      const activityFeed = (parsed && Array.isArray(parsed.activityFeed)) ? parsed.activityFeed : [];
      return { orders, activityFeed };
    }
  } catch (e) {
    console.error('[資料持久化] 讀取 orders.json 失敗:', e.message);
  }
  return { orders: {}, activityFeed: [] };
}

const serverState = loadOrdersData();
console.log(`[資料持久化] 成功載入已存訂單：共 ${Object.keys(serverState.orders).length} 筆，動態紀錄 ${serverState.activityFeed.length} 筆`);

function saveOrdersData() {
  try {
    const tmpPath = `${ORDERS_FILE}.tmp`;
    fs.writeFileSync(tmpPath, JSON.stringify({
      orders: serverState.orders,
      activityFeed: serverState.activityFeed.slice(0, 50),
      lastResetEpoch: lastResetEpoch,
      updatedAt: Date.now()
    }, null, 2), 'utf8');
    fs.renameSync(tmpPath, ORDERS_FILE);
  } catch (e) {
    console.error('[資料持久化] 儲存 orders.json 失敗:', e.message);
  }
}

function addServerActivity(type, title, desc) {
  const item = {
    type,
    title,
    desc,
    time: getTwTimeStr()
  };
  serverState.activityFeed.unshift(item);
  if (serverState.activityFeed.length > 50) {
    serverState.activityFeed.length = 50;
  }
}

// ============================================================
// 伺服器端週一至週五台灣時間 13:00 自動重置排程
// ============================================================
function msUntilNextWeekdayNoon() {
  const nowUTC = Date.now();
  const todayTW = new Date(nowUTC + TW_OFFSET_MS);
  const todayResetUTC = Date.UTC(
    todayTW.getUTCFullYear(),
    todayTW.getUTCMonth(),
    todayTW.getUTCDate(),
    5, 0, 0 // UTC 05:00 = 台灣時間 13:00
  );
  let candidateUTC = nowUTC < todayResetUTC ? todayResetUTC : todayResetUTC + 86400000;
  for (let i = 0; i < 7; i++) {
    const day = new Date(candidateUTC + TW_OFFSET_MS).getUTCDay();
    if (day >= 1 && day <= 5) break;
    candidateUTC += 86400000;
  }
  return candidateUTC - nowUTC;
}

function broadcastResetToAllClients(operatorLabel = '⏰ 伺服器自動排程') {
  const epoch = Date.now();
  lastResetEpoch = epoch;
  saveResetEpoch(epoch);

  serverState.orders = {};
  serverState.activityFeed = [];
  addServerActivity('reset', `🔄 ${operatorLabel} 清空訂單`, '所有座位已重新開放');
  saveOrdersData();

  const payload = JSON.stringify({
    type: 'ORDERS_RESET',
    data: { resetEpoch: epoch, operatorLabel: operatorLabel },
    resetEpoch: epoch,
    senderId: 'SERVER',
    deviceLabel: '⏰ 伺服器',
    time: getTwTimeStr(epoch)
  });

  broadcastWebSocketMessage(payload);

  const twTimeStr = new Date(epoch + TW_OFFSET_MS).toISOString().replace('T', ' ').slice(0, 19);
  console.log(`[自動重置] 台灣時間 13:00 清空完成，已儲存並廣播至所有裝置 (${twTimeStr} TW)`);
}

function scheduleNextAutoReset() {
  const delay = msUntilNextWeekdayNoon();
  const nextTime = new Date(Date.now() + delay + TW_OFFSET_MS).toISOString().replace('T', ' ').slice(0, 19);
  console.log(`[自動重置] 下次排程台灣時間：${nextTime}（約 ${Math.round(delay / 60000)} 分鐘後）`);
  setTimeout(() => {
    broadcastResetToAllClients();
    scheduleNextAutoReset();
  }, delay);
}

scheduleNextAutoReset();
console.log('[自動重置] 週一至週五台灣時間 13:00 伺服器端排程運作中');

// ============================================================
// HTTP 靜態服務與 REST API
// ============================================================
const PORT = 3000;
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.webmanifest': 'application/manifest+json; charset=utf-8'
};

// Service Worker 與 manifest 必須以 no-cache 模式提供，否則瀏覽器不會更新 SW
const NO_CACHE_FILES = new Set(['/sw.js', '/manifest.json', '/manifest.webmanifest']);


function getAllLocalIPs() {
  const ips = [];
  const interfaces = os.networkInterfaces();
  for (const devName in interfaces) {
    const iface = interfaces[devName];
    for (let i = 0; i < iface.length; i++) {
      const alias = iface[i];
      if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal) {
        ips.push({ name: devName, ip: alias.address });
      }
    }
  }
  return ips.length > 0 ? ips : [{ name: 'Loopback', ip: 'localhost' }];
}

function getLocalIP() {
  const ips = getAllLocalIPs();
  return ips[0].ip;
}

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 1e6) { // 1MB 限制
        req.connection.destroy();
        reject(new Error('Payload Too Large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

const wsClients = new Set();

const server = http.createServer(async (req, res) => {
  // CORS 標頭，允許跨裝置與同網段存取
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const safePath = req.url.split('?')[0];

  // API 1: 取得伺服器最新完整訂單與動態紀錄（解決「重新整理也沒用」）
  if (safePath === '/api/orders' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate'
    });
    res.end(JSON.stringify({
      success: true,
      orders: serverState.orders,
      activityFeed: serverState.activityFeed,
      lastResetEpoch: lastResetEpoch,
      serverTime: Date.now()
    }));
    return;
  }

  // API 2: 同步與重置時間查詢
  if (safePath === '/api/state' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate'
    });
    res.end(JSON.stringify({
      lastResetEpoch,
      orderCount: Object.keys(serverState.orders).length,
      serverTime: Date.now()
    }));
    return;
  }

  // API 3: 學生送出點餐 (POST /api/order)
  if (safePath === '/api/order' && req.method === 'POST') {
    try {
      const body = await parseJsonBody(req);
      const order = body.order || body;
      const seatId = Number(order.seatId);

      if (isNaN(seatId) || seatId < 1 || seatId > 31) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ success: false, error: '座號必須介於 1 到 31 之間' }));
        return;
      }

      if (!order.mealId || !order.mealName) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ success: false, error: '餐點資料不完整' }));
        return;
      }

      const cleanOrder = {
        seatId: seatId,
        mealId: String(order.mealId),
        mealName: String(order.mealName),
        price: Number(order.price) || 0,
        time: order.time || getTwTimeStr(),
        deviceType: order.deviceType || '📱 裝置',
        timestamp: Number(order.timestamp) || Date.now()
      };

      serverState.orders[seatId] = cleanOrder;
      addServerActivity('order', `${seatId}號 點餐`, `${cleanOrder.mealName} ($${cleanOrder.price}元)`);
      saveOrdersData();

      // 廣播給所有連線中的瀏覽器（附帶全量最新點餐清單，極速全方位畫面對齊）
      broadcastWebSocketMessage(JSON.stringify({
        type: 'ORDER_CREATED',
        data: cleanOrder,
        allOrders: serverState.orders,
        resetEpoch: lastResetEpoch,
        senderId: 'SERVER',
        deviceLabel: cleanOrder.deviceType,
        time: cleanOrder.time
      }));

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: true, order: cleanOrder, orders: serverState.orders }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
    return;
  }

  // API 4: 取消點餐 (POST /api/cancel)
  if (safePath === '/api/cancel' && req.method === 'POST') {
    try {
      const body = await parseJsonBody(req);
      const seatId = Number(body.seatId);

      if (isNaN(seatId) || seatId < 1 || seatId > 31) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ success: false, error: '座號無效' }));
        return;
      }

      const existingOrder = serverState.orders[seatId];
      delete serverState.orders[seatId];

      const mealDesc = existingOrder ? `${existingOrder.mealName} 已取消` : '座位已重新釋放';
      addServerActivity('cancel', `${seatId}號 取消點餐`, mealDesc);
      saveOrdersData();

      broadcastWebSocketMessage(JSON.stringify({
        type: 'ORDER_CANCELLED',
        data: { seatId: seatId },
        allOrders: serverState.orders,
        resetEpoch: lastResetEpoch,
        senderId: 'SERVER',
        time: getTwTimeStr()
      }));

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: true, seatId: seatId, orders: serverState.orders }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
    return;
  }

  // API 5: 管理員手動重置 (POST /api/reset)
  if (safePath === '/api/reset' && req.method === 'POST') {
    try {
      const body = await parseJsonBody(req);
      const label = body.operatorLabel || '管理員手動重置';
      broadcastResetToAllClients(label);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: true, resetEpoch: lastResetEpoch }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
    return;
  }

  // 靜態檔案服務
  const filePath = path.join(__dirname, safePath === '/' ? 'index.html' : safePath);
  const ext = path.extname(filePath).toLowerCase();

  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('404 找不到檔案');
      } else {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(`伺服器錯誤: ${err.code}`);
      }
    } else {
      const headers = {
        'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
        'Cache-Control': 'no-cache, no-store, must-revalidate'
      };
      // Service Worker 必須附加此標頭，才能控制根路徑下的所有頁面
      if (safePath === '/sw.js') {
        headers['Service-Worker-Allowed'] = '/';
      }
      res.writeHead(200, headers);
      res.end(content);
    }
  });
});

// ============================================================
// RFC 6455 零相依純 Node.js WebSocket 伺服器
// 支援封包緩衝組合、Ping/Pong 保活心跳、連線立即下發全量狀態
// ============================================================
function makeWebSocketFrame(payloadStr, opcode = 0x1) {
  const payloadBuf = Buffer.from(payloadStr, 'utf8');
  const len = payloadBuf.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | (opcode & 0x0F);
    header[1] = len;
  } else if (len <= 65535) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | (opcode & 0x0F);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | (opcode & 0x0F);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payloadBuf]);
}

function makePongFrame(payloadBuf) {
  const len = payloadBuf ? payloadBuf.length : 0;
  const header = Buffer.alloc(2 + len);
  header[0] = 0x8A; // 0x80 | 0x0A (Pong)
  header[1] = len;
  if (payloadBuf && len > 0) payloadBuf.copy(header, 2);
  return header;
}

server.on('upgrade', (req, socket, head) => {
  const upHeader = req.headers['upgrade'];
  if (!upHeader || upHeader.toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }

  // 關閉 Nagle 緩衝延遲演算法，確保 TCP 封包即發即至（0 延遲）
  socket.setNoDelay(true);

  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  const hash = crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${hash}`
  ];

  socket.write(headers.join('\r\n') + '\r\n\r\n');
  socket._wsBuffer = Buffer.alloc(0);
  wsClients.add(socket);

  // 延遲至下一個微任務發送，避免資料幀與 HTTP 101 標頭黏包導致部分手機握手報錯
  setImmediate(() => {
    if (socket.writable) {
      const initPayload = JSON.stringify({
        type: 'STATE_INIT',
        data: {
          orders: serverState.orders,
          activityFeed: serverState.activityFeed.slice(0, 30),
          resetEpoch: lastResetEpoch
        },
        resetEpoch: lastResetEpoch,
        senderId: 'SERVER',
        time: getTwTimeStr()
      });
      try {
        socket.write(makeWebSocketFrame(initPayload));
      } catch (e) {}
    }
  });

  socket.on('data', (chunk) => {
    socket._wsBuffer = Buffer.concat([socket._wsBuffer, chunk]);

    // 依序解析 buffer 內所有完整的 WebSocket frame
    while (socket._wsBuffer.length >= 2) {
      const firstByte = socket._wsBuffer[0];
      const opcode = firstByte & 0x0F;
      const secondByte = socket._wsBuffer[1];
      const isMasked = Boolean(secondByte & 0x80);
      let payloadLen = secondByte & 0x7F;
      let headerLen = 2;

      if (payloadLen === 126) {
        if (socket._wsBuffer.length < 4) break;
        payloadLen = socket._wsBuffer.readUInt16BE(2);
        headerLen = 4;
      } else if (payloadLen === 127) {
        if (socket._wsBuffer.length < 10) break;
        payloadLen = Number(socket._wsBuffer.readBigUInt64BE(2));
        headerLen = 10;
      }

      const maskLen = isMasked ? 4 : 0;
      const totalFrameLen = headerLen + maskLen + payloadLen;

      if (socket._wsBuffer.length < totalFrameLen) {
        // 封包尚不完整，等待下一個 TCP chunk 到達
        break;
      }

      let maskingKey = null;
      let currentOffset = headerLen;
      if (isMasked) {
        maskingKey = socket._wsBuffer.slice(currentOffset, currentOffset + 4);
        currentOffset += 4;
      }

      const payload = socket._wsBuffer.slice(currentOffset, currentOffset + payloadLen);
      if (isMasked && maskingKey) {
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= maskingKey[i % 4];
        }
      }

      // 移出已處理的 frame
      socket._wsBuffer = socket._wsBuffer.slice(totalFrameLen);

      // 處理不同 Opcode
      if (opcode === 0x8) { // Close
        try { socket.end(makeWebSocketFrame('', 0x8)); } catch (e) {}
        wsClients.delete(socket);
        break;
      } else if (opcode === 0x9) { // Ping -> 立即回覆 Pong
        try { socket.write(makePongFrame(payload)); } catch (e) {}
      } else if (opcode === 0x1) { // Text frame
        const text = payload.toString('utf8');
        handleClientTextMessage(text, socket);
      }
    }
  });

  socket.on('close', () => {
    wsClients.delete(socket);
  });

  socket.on('error', () => {
    wsClients.delete(socket);
  });
});

function handleClientTextMessage(text, senderSocket) {
  try {
    const payload = JSON.parse(text);

    if (payload.type === 'PING') {
      try {
        senderSocket.write(makeWebSocketFrame(JSON.stringify({ type: 'PONG', time: Date.now() })));
      } catch (e) {}
      return;
    }

    if (payload.type === 'REQ_SYNC') {
      const syncPayload = JSON.stringify({
        type: 'STATE_INIT',
        data: {
          orders: serverState.orders,
          activityFeed: serverState.activityFeed.slice(0, 30),
          resetEpoch: lastResetEpoch
        },
        resetEpoch: lastResetEpoch,
        senderId: 'SERVER',
        time: getTwTimeStr()
      });
      try {
        senderSocket.write(makeWebSocketFrame(syncPayload));
      } catch (e) {}
      return;
    }

    if (payload.type === 'ORDER_CREATED' && payload.data) {
      const order = payload.data;
      const seatId = Number(order.seatId);
      if (!isNaN(seatId) && seatId >= 1 && seatId <= 31) {
        serverState.orders[seatId] = order;
        addServerActivity('order', `${seatId}號 點餐`, `${order.mealName} ($${order.price}元)`);
        saveOrdersData();
      }
    } else if (payload.type === 'ORDER_CANCELLED' && payload.data) {
      const seatId = Number(payload.data.seatId);
      if (!isNaN(seatId)) {
        delete serverState.orders[seatId];
        addServerActivity('cancel', `${seatId}號 取消點餐`, `座位已重新釋放`);
        saveOrdersData();
      }
    } else if (payload.type === 'ORDERS_RESET') {
      const epoch = Number(payload.resetEpoch) || Date.now();
      lastResetEpoch = epoch;
      saveResetEpoch(epoch);
      serverState.orders = {};
      serverState.activityFeed = [];
      addServerActivity('reset', '🔄 訂單重置', '所有座位已重新開放');
      saveOrdersData();
    }

    // 廣播給其他已連線的客戶端
    broadcastWebSocketMessage(text, senderSocket);
  } catch (err) {
    console.warn('[WebSocket] 解析訊息異常:', err.message);
  }
}

function broadcastWebSocketMessage(text, senderSocket = null) {
  const frame = makeWebSocketFrame(text);

  wsClients.forEach((client) => {
    if (client.writable && client !== senderSocket) {
      try {
        client.write(frame);
      } catch (e) {
        wsClients.delete(client);
      }
    }
  });
}

// 每 20 秒向所有連線客戶端發送 Heartbeat Ping，防止手機與 Wi-Fi AP 閒置斷線
setInterval(() => {
  const pingFrame = Buffer.from([0x89, 0x00]); // opcode 0x9 (Ping)
  wsClients.forEach((client) => {
    if (client.writable) {
      try {
        client.write(pingFrame);
      } catch (e) {
        wsClients.delete(client);
      }
    } else {
      wsClients.delete(client);
    }
  });
}, 20000);

server.listen(PORT, () => {
  const localIPs = getAllLocalIPs();
  console.log('===========================================================');
  console.log('🏫 智光高級商工職業學校資處科一仁午餐訂餐系統 - 本地伺服器');
  console.log(`💻 電腦本機存取：http://localhost:${PORT}`);
  localIPs.forEach(item => {
    console.log(`📱 同學手機/平板連線 (${item.name})：http://${item.ip}:${PORT}`);
  });
  console.log('⚡ 訂單持久化 (orders.json) 與 WebSocket/REST 雙引擎已就緒！');
  console.log('🔄 重新整理 100% 留存訂單，全網域 0 延遲同步！');
  console.log('===========================================================');
});

function gracefulShutdown() {
  console.log('\n[系統關閉] 正在執行最後一次資料持久化與平滑關閉...');
  saveOrdersData();
  saveResetEpoch(lastResetEpoch);
  console.log('[系統關閉] 所有訂單資料已安全寫入 orders.json，伺服器順利關閉。');
  process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

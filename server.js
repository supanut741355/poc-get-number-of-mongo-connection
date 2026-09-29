require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const { createMetrics } = require('./metrics');

const {
  MONGODB_URI,
  DB_NAME = 'demo',
  APP_NAME = 'my-express-api',
  MAX_POOL_SIZE = '20',
  MIN_POOL_SIZE = '0',
  MAX_IDLE_TIME_MS = '0', // 0 = ไม่ปิด connection ที่ idle (default ของ driver)
  PORT = '3000',
  STATS_LOG_INTERVAL_MS = '10000',
} = process.env;

if (!MONGODB_URI) {
  console.error('Missing MONGODB_URI in .env');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// 1) สร้าง MongoClient เองจาก driver ที่ mongoose bundle มา (mongoose.mongo)
//    เพื่อผูก pool event listener ได้ "ก่อน" connect จะไม่พลาด event แรก ๆ
//    แล้วค่อยส่ง client ให้ mongoose ใช้ผ่าน setClient()
// ---------------------------------------------------------------------------
const client = new mongoose.mongo.MongoClient(MONGODB_URI, {
  dbName: DB_NAME,
  appName: APP_NAME,               // ใช้แยก app ใน $currentOp / Atlas
  maxPoolSize: Number(MAX_POOL_SIZE),
  minPoolSize: Number(MIN_POOL_SIZE),
  maxIdleTimeMS: Number(MAX_IDLE_TIME_MS),
});

// ---------------------------------------------------------------------------
// 2) Mongoose model
// ---------------------------------------------------------------------------
const itemSchema = new mongoose.Schema(
  {
    name: { type: String, trim: true },
    qty: { type: Number, default: 0 },
  },
  { timestamps: true, strict: false } // strict:false = รับ field อื่นได้เหมือนเดิม
);
const Item = mongoose.model('Item', itemSchema, 'items');

// ---------------------------------------------------------------------------
// 3) นับ connection จาก CMAP events แยกตาม server (node ของ replica set)
//    หมายเหตุ: ไม่รวม monitoring connection (~2 ต่อ node) ที่ driver ใช้ทำ heartbeat
// ---------------------------------------------------------------------------
const pools = new Map(); // address -> { open, inUse, created, closed }
const heartbeatServers = new Set();

function pool(address) {
  if (!pools.has(address)) {
    pools.set(address, { open: 0, inUse: 0, created: 0, closed: 0 });
  }
  return pools.get(address);
}

// ตัวแปรกลางเก็บจำนวน connection ของ process นี้ (รวมทุก node)
const connectionCount = {
  current: 0,       // connection ที่เปิดอยู่ตอนนี้
  inUse: 0,         // connection ที่กำลังถูกใช้งานอยู่ตอนนี้
  peak: 0,          // ค่าสูงสุดของ current ตั้งแต่ start (หรือ reset ล่าสุด)
  peakInUse: 0,     // ค่าสูงสุดของ inUse
  totalCreated: 0,  // จำนวน connection ที่เคยเปิดทั้งหมด
  totalClosed: 0,   // จำนวน connection ที่เคยปิดทั้งหมด
  startedAt: new Date(),
  peakAt: null,
  lastUpdated: null,
};

function touch() { connectionCount.lastUpdated = new Date(); }

// Prometheus metrics (อ่านค่าจาก pools / connectionCount ตอน scrape)
const metrics = createMetrics({
  appName: APP_NAME,
  maxPoolSize: Number(MAX_POOL_SIZE),
  pools,
  connectionCount,
});
metrics.attach(client); // ผูกก่อน client.connect()

client.on('connectionPoolCreated', (e) => {
  pool(e.address);
  console.log(`[pool] created for ${e.address}`);
});
client.on('connectionCreated', (e) => {
  console.log('call this when add load')
  const p = pool(e.address);
  p.open++; p.created++;
  connectionCount.current++;
  connectionCount.totalCreated++;
  if (connectionCount.current > connectionCount.peak) {
    connectionCount.peak = connectionCount.current;
    connectionCount.peakAt = new Date();
  }
  touch();
});
client.on('connectionClosed', (e) => {
  const p = pool(e.address);
  p.open = Math.max(0, p.open - 1); p.closed++;
  connectionCount.current = Math.max(0, connectionCount.current - 1);
  connectionCount.totalClosed++;
  touch();
  console.log(`[pool] closed conn #${e.connectionId} on ${e.address} (${e.reason})`);
});
client.on('connectionCheckedOut', (e) => {
  pool(e.address).inUse++;
  connectionCount.inUse++;
  connectionCount.peakInUse = Math.max(connectionCount.peakInUse, connectionCount.inUse);
  touch();
});
client.on('connectionCheckedIn', (e) => {
  const p = pool(e.address);
  p.inUse = Math.max(0, p.inUse - 1);
  connectionCount.inUse = Math.max(0, connectionCount.inUse - 1);
  touch();
});
client.on('connectionPoolCleared', (e) => {
  console.warn(`[pool] cleared for ${e.address}`);
});
client.on('serverHeartbeatSucceeded', (e) => heartbeatServers.add(e.connectionId));

// mongoose connection events
mongoose.connection.on('disconnected', () => console.warn('[mongoose] disconnected'));
mongoose.connection.on('reconnected', () => console.log('[mongoose] reconnected'));
mongoose.connection.on('error', (err) => console.error('[mongoose] error:', err.message));

function poolStats() {
  const servers = {};
  let totalOpen = 0, totalInUse = 0;
  for (const [addr, p] of pools) {
    servers[addr] = { ...p };
    totalOpen += p.open;
    totalInUse += p.inUse;
  }
  const nodes = pools.size || heartbeatServers.size;
  return {
    appName: APP_NAME,
    pid: process.pid,
    maxPoolSizePerNode: Number(MAX_POOL_SIZE),
    totals: {
      poolConnectionsOpen: totalOpen,
      poolConnectionsInUse: totalInUse,
      // ประมาณการ connection ที่ Atlas เห็นจาก process นี้ (pool + monitoring ~2/node)
      estimatedTotalIncludingMonitoring: totalOpen + nodes * 2,
    },
    servers,
  };
}

// ---------------------------------------------------------------------------
// 4) Express app
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());

// async error wrapper (Express 4 ไม่จับ error จาก async handler ให้เอง)
const wrap = (fn) => (req, res, next) => fn(req, res, next).catch(next);

app.get('/health', async (_req, res) => {
  try {
    await mongoose.connection.db.admin().ping();
    res.json({ ok: true, mongooseState: mongoose.connection.readyState });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Prometheus scrape endpoint
app.get('/metrics', wrap(async (_req, res) => {
  res.set('Content-Type', metrics.register.contentType);
  res.send(await metrics.register.metrics());
}));

// จำนวน connection จากมุมมองของ driver (process นี้)
app.get('/stats/pool', (_req, res) => res.json(poolStats()));

// GET /connections            -> ค่าจากตัวแปร connectionCount
// GET /connections?detail=true -> เพิ่มรายละเอียดแยกตาม node
app.get('/connections', (req, res) => {
  const body = {
    appName: APP_NAME,
    pid: process.pid,
    maxPoolSizePerNode: Number(MAX_POOL_SIZE),
    ...connectionCount,
  };
  if (req.query.detail === 'true') {
    body.byServer = Object.fromEntries(
      [...pools].map(([addr, p]) => [addr, { current: p.open, inUse: p.inUse }])
    );
  }
  res.json(body);
});

// GET /connections/count -> ตัวเลขเดียว (เหมาะกับ script / monitoring)
app.get('/connections/count', (_req, res) => {
  res.type('text/plain').send(String(connectionCount.current));
});

// POST /connections/reset-peak -> reset ค่า peak ให้เริ่มวัดใหม่
app.post('/connections/reset-peak', (_req, res) => {
  connectionCount.peak = connectionCount.current;
  connectionCount.peakInUse = connectionCount.inUse;
  connectionCount.peakAt = new Date();
  res.json({ ok: true, peak: connectionCount.peak, peakInUse: connectionCount.peakInUse });
});

// จำนวน connection จากมุมมองของ server (ต้องใช้ user ที่มีสิทธิ์ เช่น atlasAdmin,
// ใช้ไม่ได้บน M0/M2/M5 และเห็นเฉพาะ node ที่ query นี้วิ่งไปถึง)
app.get('/stats/server', async (_req, res) => {
  try {
    const admin = mongoose.connection.client.db('admin');
    const byApp = await admin.aggregate([
      { $currentOp: { allUsers: true, idleConnections: true } },
      { $group: { _id: { appName: '$appName', client: '$client' }, count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ]).toArray();
    const status = await admin.command({ serverStatus: 1 });
    res.json({ host: status.host, connections: status.connections, byApp });
  } catch (err) {
    res.status(500).json({ error: err.message, hint: 'Needs admin privileges / dedicated tier (M10+)' });
  }
});

app.get('/items', wrap(async (_req, res) => {
  const items = await Item.find().limit(50).lean();
  res.json(items);
}));

app.post('/items', wrap(async (req, res) => {
  const item = await Item.create(req.body);
  res.status(201).json(item);
}));

app.get('/items/:id', wrap(async (req, res) => {
  if (!mongoose.isValidObjectId(req.params.id)) return res.status(400).json({ error: 'bad id' });
  const item = await Item.findById(req.params.id).lean();
  item ? res.json(item) : res.status(404).json({ error: 'not found' });
}));

// ยิง query พร้อมกัน n ตัว เพื่อดู pool โตขึ้น (สูงสุด maxPoolSize ต่อ node)
app.get('/load', wrap(async (req, res) => {
  const n = Math.min(Number(req.query.n) || 50, 500);
  const before = poolStats().totals;
  const t0 = Date.now();
  await Promise.all(Array.from({ length: n }, () => Item.countDocuments({})));
  res.json({ queries: n, ms: Date.now() - t0, before, after: poolStats().totals });
}));

// error handler
app.use((err, _req, res, _next) => {
  console.error(err);
  const status = err.name === 'ValidationError' || err.name === 'CastError' ? 400 : 500;
  res.status(status).json({ error: err.message });
});

// ---------------------------------------------------------------------------
// 5) start / shutdown
// ---------------------------------------------------------------------------
let server, statsTimer;

async function start() {
  await client.connect();
  mongoose.connection.setClient(client); // ให้ mongoose ใช้ client ตัวเดียวกัน
  await mongoose.connection.db.admin().ping();
  console.log(`Connected via mongoose ${mongoose.version} (db=${DB_NAME}, appName=${APP_NAME}, ` +
    `maxPoolSize=${MAX_POOL_SIZE}, maxIdleTimeMS=${MAX_IDLE_TIME_MS})`);

  server = app.listen(Number(PORT), () => console.log(`Listening on :${PORT}`));

  const interval = Number(STATS_LOG_INTERVAL_MS);
  if (interval > 0) {
    statsTimer = setInterval(() => {
      const { totals } = poolStats();
      console.log(`[stats] open=${totals.poolConnectionsOpen} inUse=${totals.poolConnectionsInUse} ` +
        `peak=${connectionCount.peak} est.total=${totals.estimatedTotalIncludingMonitoring}`);
    }, interval);
    statsTimer.unref();
  }
}

async function shutdown(signal) {
  console.log(`${signal} received, shutting down...`);
  clearInterval(statsTimer);
  server?.close();
  await mongoose.connection.close(); // ปิด client และทุก connection ใน pool
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start().catch((err) => {
  console.error('Failed to start:', err);
  process.exit(1);
});
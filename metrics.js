const client = require('prom-client');

/**
 * สร้าง Prometheus registry + custom metrics สำหรับ MongoDB connection pool
 *
 * @param {object}   opts
 * @param {string}   opts.appName          ใส่เป็น default label "app" ให้ทุก metric
 * @param {number}   opts.maxPoolSize      ค่า maxPoolSize ต่อ node
 * @param {Map}      opts.pools            address -> { open, inUse, created, closed } (อ่านตอน scrape)
 * @param {object}   opts.connectionCount  ตัวแปรกลาง { current, inUse, peak, peakInUse, ... }
 */
function createMetrics({ appName, maxPoolSize, pools, connectionCount }) {
  const register = new client.Registry();
  register.setDefaultLabels({ app: appName });

  // metric มาตรฐานของ Node.js process (cpu, memory, event loop lag, gc ...)
  client.collectDefaultMetrics({ register });

  // ---------------------------------------------------------------------------
  // Gauges: อ่านค่าล่าสุดตอน Prometheus มา scrape (collect callback)
  // ---------------------------------------------------------------------------
  new client.Gauge({
    name: 'mongodb_pool_connections',
    help: 'Open connections in the MongoDB driver pool (excludes monitoring connections)',
    labelNames: ['server'],
    registers: [register],
    collect() {
      this.reset(); // ล้าง label ของ node ที่หายไปแล้ว
      for (const [server, p] of pools) this.set({ server }, p.open);
    },
  });

  new client.Gauge({
    name: 'mongodb_pool_connections_in_use',
    help: 'Connections currently checked out of the pool (running an operation)',
    labelNames: ['server'],
    registers: [register],
    collect() {
      this.reset();
      for (const [server, p] of pools) this.set({ server }, p.inUse);
    },
  });

  new client.Gauge({
    name: 'mongodb_pool_connections_peak',
    help: 'Highest total open pool connections since start or last reset',
    registers: [register],
    collect() { this.set(connectionCount.peak); },
  });

  new client.Gauge({
    name: 'mongodb_pool_connections_in_use_peak',
    help: 'Highest total in-use pool connections since start or last reset',
    registers: [register],
    collect() { this.set(connectionCount.peakInUse); },
  });

  new client.Gauge({
    name: 'mongodb_pool_max_size',
    help: 'Configured maxPoolSize per server',
    registers: [register],
    collect() { this.set(maxPoolSize); },
  });

  // ---------------------------------------------------------------------------
  // Counters / Histograms: อัปเดตจาก driver events
  // ---------------------------------------------------------------------------
  const created = new client.Counter({
    name: 'mongodb_pool_connections_created_total',
    help: 'Total pool connections created',
    labelNames: ['server'],
    registers: [register],
  });

  const closed = new client.Counter({
    name: 'mongodb_pool_connections_closed_total',
    help: 'Total pool connections closed, by reason (idle, stale, error, poolClosed ...)',
    labelNames: ['server', 'reason'],
    registers: [register],
  });

  const cleared = new client.Counter({
    name: 'mongodb_pool_cleared_total',
    help: 'Times the pool was cleared (e.g. failover, network error)',
    labelNames: ['server'],
    registers: [register],
  });

  const checkoutFailed = new client.Counter({
    name: 'mongodb_pool_checkout_failed_total',
    help: 'Failed connection checkouts, by reason (timeout, connectionError, poolClosed)',
    labelNames: ['server', 'reason'],
    registers: [register],
  });

  const checkoutWait = new client.Histogram({
    name: 'mongodb_pool_checkout_wait_seconds',
    help: 'Time an operation waited to get a connection from the pool',
    labelNames: ['server'],
    buckets: [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [register],
  });

  const establish = new client.Histogram({
    name: 'mongodb_connection_establish_seconds',
    help: 'Time to establish a new connection (TCP + TLS + handshake + auth)',
    labelNames: ['server'],
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
    registers: [register],
  });

  /** ผูก listener กับ MongoClient (เรียกก่อน client.connect()) */
  function attach(mongoClient) {
    mongoClient.on('connectionCreated', (e) => created.inc({ server: e.address }));
    mongoClient.on('connectionClosed', (e) =>
      closed.inc({ server: e.address, reason: e.reason || 'unknown' }));
    mongoClient.on('connectionPoolCleared', (e) => cleared.inc({ server: e.address }));
    mongoClient.on('connectionCheckOutFailed', (e) =>
      checkoutFailed.inc({ server: e.address, reason: e.reason || 'unknown' }));
    mongoClient.on('connectionCheckedOut', (e) => {
      if (typeof e.durationMS === 'number') checkoutWait.observe({ server: e.address }, e.durationMS / 1000);
    });
    mongoClient.on('connectionReady', (e) => {
      if (typeof e.durationMS === 'number') establish.observe({ server: e.address }, e.durationMS / 1000);
    });
  }

  return { register, attach };
}

module.exports = { createMetrics };
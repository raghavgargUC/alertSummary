require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const axios = require('axios');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3002;

const MONGO_URL = process.env.MONGO_URL;
const DB_NAME = process.env.DB_NAME;
const ALERT_API_BASE = process.env.ALERT_API_BASE;
const API_KEY = process.env.API_KEY;

if (!MONGO_URL || !DB_NAME || !ALERT_API_BASE || !API_KEY) {
  console.error('Missing required environment variables. Check your .env file.');
  process.exit(1);
}

let db;

async function connectDB() {
  const client = new MongoClient(MONGO_URL, { connectTimeoutMS: 10000, serverSelectionTimeoutMS: 10000 });
  await client.connect();
  db = client.db(DB_NAME);
  console.log(`Connected to MongoDB: ${MONGO_URL}/${DB_NAME}`);
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// GET /api/signals
// Returns all active signals with per-severity alert counts
app.get('/api/signals', async (req, res) => {
  try {
    const signals = await db
      .collection('signals_master')
      .find({ isActive: true })
      .sort({ name: 1 })
      .toArray();

    const signalsWithCounts = await Promise.all(
      signals.map(async (signal) => {
        const signalIdStr = signal._id.toString();
        const counts = await db
          .collection('alert_overview')
          .aggregate([
            { $match: { signalId: signalIdStr } },
            { $group: { _id: '$severity', count: { $sum: 1 } } },
          ])
          .toArray();

        const severityCounts = { CRITICAL: 0, WARNING: 0, LOW: 0 };
        counts.forEach(({ _id, count }) => {
          if (_id && _id in severityCounts) severityCounts[_id] = count;
        });

        const total = severityCounts.CRITICAL + severityCounts.WARNING + severityCounts.LOW;

        return {
          _id: signalIdStr,
          signalCode: signal.signalCode,
          name: signal.name,
          description: signal.description,
          module: signal.module,
          uiModule: signal.uiModule,
          severityCounts,
          total,
        };
      })
    );

    // Sort by total alerts descending so busiest signals appear first
    signalsWithCounts.sort((a, b) => b.total - a.total);

    res.json({ data: signalsWithCounts });
  } catch (err) {
    console.error('[/api/signals]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/alerts?signalId=X&severity=Y
// Returns alerts filtered by signalId and/or severity.
// For CRITICAL, also returns a summary with distinct tenant/facility counts.
app.get('/api/alerts', async (req, res) => {
  try {
    const { signalId, severity } = req.query;
    const query = {};
    if (signalId) query.signalId = signalId;
    if (severity) query.severity = severity;

    const rawAlerts = await db
      .collection('alert_overview')
      .find(query)
      .sort({ updatedAt: -1 })
      .limit(200)
      .toArray();

    let summary = null;
    if (severity === 'CRITICAL' && rawAlerts.length > 0) {
      const tenants = new Set(rawAlerts.map((a) => a.tenantCode).filter(Boolean));
      const facilities = new Set(rawAlerts.map((a) => a.facilityCode).filter(Boolean));
      summary = { tenantCount: tenants.size, facilityCount: facilities.size };
    }

    const alerts = rawAlerts.map((a) => ({
      _id: a._id.toString(),
      tenantCode: a.tenantCode,
      facilityCode: a.facilityCode,
      signalId: a.signalId,
      fingerprint: a.fingerprint,
      severity: a.severity,
      status: a.status,
      createdAt: a.createdAt,
      updatedAt: a.updatedAt,
      template: a.template ?? {},
      label: a.label ?? null,
      isSubscribed: a.isSubscribed,
    }));

    res.json({ data: alerts, summary });
  } catch (err) {
    console.error('[/api/alerts]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/alerts/:fingerprint/history
// Proxies to the upstream alert history API
app.get('/api/alerts/:fingerprint/history', async (req, res) => {
  try {
    const { fingerprint } = req.params;
    const upstream = `${ALERT_API_BASE}/alerts/${fingerprint}/history`;
    const response = await axios.get(upstream, {
      headers: { 'x-api-key': API_KEY },
      timeout: 15000,
    });
    res.json(response.data);
  } catch (err) {
    console.error('[/api/history]', err.message);
    const status = err.response?.status || 500;
    res.status(status).json({ error: err.message });
  }
});

connectDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Alert Dashboard → http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error('MongoDB connection failed:', err.message);
    process.exit(1);
  });

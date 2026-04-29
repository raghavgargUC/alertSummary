require('dotenv').config();
const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
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

// Collect fingerprint strings from signal_subscription_config documents (schema may vary).
function fingerprintsFromSubscriptionDocs(docs) {
  const set = new Set();
  for (const doc of docs) {
    if (doc.fingerprint != null && doc.fingerprint !== '') set.add(String(doc.fingerprint).trim());
    if (Array.isArray(doc.fingerprints)) {
      for (const f of doc.fingerprints) {
        if (f != null && f !== '') set.add(String(f).trim());
      }
    }
    if (doc.alertFingerprint != null && doc.alertFingerprint !== '') {
      set.add(String(doc.alertFingerprint).trim());
    }
    const nested = doc.subscription || doc.config;
    if (nested && nested.fingerprint != null && nested.fingerprint !== '') {
      set.add(String(nested.fingerprint).trim());
    }
  }
  return set;
}

async function subscribedFingerprintsForSignal(signalIdStr, signalObjectId) {
  const docs = await db
    .collection('signal_subscription_config')
    .find({
      $or: [{ signalId: String(signalIdStr) }, { signalId: signalObjectId }],
    })
    .toArray();
  return fingerprintsFromSubscriptionDocs(docs);
}

function objectIdIfValid(s) {
  if (s == null || s === '') return null;
  try {
    if (ObjectId.isValid(s) && String(new ObjectId(s)) === String(s)) return new ObjectId(s);
  } catch {
    /* ignore */
  }
  return null;
}

// GET /api/signals
// Returns all active signals with per-severity alert counts (alert_overview rows whose
// fingerprint appears in signal_subscription_config for that signal).
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
        const fpSet = await subscribedFingerprintsForSignal(signalIdStr, signal._id);
        const fpList = [...fpSet];

        let counts = [];
        if (fpList.length > 0) {
          counts = await db
            .collection('alert_overview')
            .aggregate([
              { $match: { signalId: signalIdStr, fingerprint: { $in: fpList } } },
              { $group: { _id: '$severity', count: { $sum: 1 } } },
            ])
            .toArray();
        }

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

// GET /api/signals/:signalId/subscription-fingerprints
// Fingerprints registered in signal_subscription_config for this signal (for orphan detection).
app.get('/api/signals/:signalId/subscription-fingerprints', async (req, res) => {
  try {
    const { signalId } = req.params;
    const docs = await db
      .collection('signal_subscription_config')
      .find({
        $or: [{ signalId: String(signalId) }, { signalId }],
      })
      .toArray();
    const fingerprints = [...fingerprintsFromSubscriptionDocs(docs)];
    res.json({ data: fingerprints });
  } catch (err) {
    console.error('[/api/signals/subscription-fingerprints]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/alerts?signalId=X&severity=Y
// Returns alerts filtered by signalId and/or severity.
// When signalId is set, only alert_overview rows whose fingerprint is in
// signal_subscription_config for that signal are returned (same scope as /api/signals counts).
// For CRITICAL, also returns a summary with distinct tenant/facility counts.
app.get('/api/alerts', async (req, res) => {
  try {
    const { signalId, severity } = req.query;
    const query = {};
    if (signalId) {
      query.signalId = signalId;
      const fpSet = await subscribedFingerprintsForSignal(signalId, objectIdIfValid(signalId));
      const fpList = [...fpSet];
      if (fpList.length > 0) {
        query.fingerprint = { $in: fpList };
      } else {
        query._id = { $exists: false };
      }
    }
    if (severity) query.severity = severity;

    let cursor = db
      .collection('alert_overview')
      .find(query)
      .sort({ updatedAt: -1 });

    if (severity) {
      cursor = cursor.limit(200);
    } else if (signalId) {
      cursor = cursor.limit(5000);
    } else {
      cursor = cursor.limit(200);
    }

    const rawAlerts = await cursor.toArray();

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

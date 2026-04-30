/**
 * Alert / Signal Impact Dashboard — backend.
 *
 * Architecture
 * ────────────
 * Two MongoDB collections drive the dashboard:
 *
 *   signals_master              — catalog (one doc per signal type, ~10 rows)
 *   signal_subscription_config  — coverage table: which (tenant, facility, signal)
 *                                 combinations are subscribed (~50k rows). The
 *                                 `fingerprint` field is the latest fingerprint
 *                                 emitted for that combo.
 *   alert_overview              — current alert state per (tenant, facility, signal)
 *                                 (~12k rows). Status = FIRING / RESOLVED / SUPPRESSED;
 *                                 severity = CRITICAL / WARNING / LOW.
 *
 * "Active issue" definition
 *   A row in alert_overview with status=FIRING, severity in {CRITICAL, WARNING},
 *   AND a corresponding ACTIVE (isSubscribed:true) subscription. Inactive
 *   subscriptions represent customers who opted out — their alerts shouldn't
 *   count toward live business impact.
 *
 * Performance notes (these shape every aggregation in this file)
 * ────────────────
 *   - `signal_subscription_config` has a compound unique index on
 *     (tenantCode, facilityCode, signalId). It does NOT have an index on
 *     `fingerprint`. We always join on the indexed compound key.
 *   - `alert_overview.isSubscribed` is a mirror of subscription_config and can
 *     lag by up to one poll cycle. We use it ONLY for the bulk LOW (cleared-
 *     state) count on /api/signals, where a strict lookup over 12k+ rows would
 *     take ~14s. Every "active issue" path uses the strict $lookup.
 */

'use strict';

require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const axios = require('axios');
const path = require('path');

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3002;
const MONGO_URL = process.env.MONGO_URL;
const DB_NAME = process.env.DB_NAME;
const ALERT_API_BASE = process.env.ALERT_API_BASE;
const API_KEY = process.env.API_KEY;

const REQUIRED_ENV = { MONGO_URL, DB_NAME, ALERT_API_BASE, API_KEY };
for (const [k, v] of Object.entries(REQUIRED_ENV)) {
  if (!v) {
    console.error(`Missing required env var: ${k}. Check your .env file.`);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MongoDB connection
// ─────────────────────────────────────────────────────────────────────────────

/** @type {import('mongodb').Db} */
let db;

async function connectDB() {
  const client = new MongoClient(MONGO_URL, {
    connectTimeoutMS: 10_000,
    serverSelectionTimeoutMS: 10_000,
  });
  await client.connect();
  db = client.db(DB_NAME);
  console.log(`Connected to MongoDB: ${MONGO_URL}/${DB_NAME}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain constants & aggregation building blocks
// ─────────────────────────────────────────────────────────────────────────────

/** Match clause for "currently in alarm" alert_overview rows. */
const ACTIVE_MATCH = Object.freeze({
  status: 'FIRING',
  severity: { $in: ['CRITICAL', 'WARNING'] },
});

/**
 * Aggregation stages that filter `alert_overview` rows down to those whose
 * (tenant, facility, signal) combination has an ACTIVE subscription. Joins
 * via the indexed compound key — see file header for index notes.
 *
 * Use as `[ ...activeSubscriptionFilter() ]` inside a pipeline.
 */
function activeSubscriptionFilter() {
  return [
    { $lookup: {
        from: 'signal_subscription_config',
        let: { t: '$tenantCode', f: '$facilityCode', s: '$signalId' },
        pipeline: [
          { $match: {
              $expr: { $and: [
                { $eq: ['$tenantCode', '$$t'] },
                { $eq: ['$facilityCode', '$$f'] },
                { $eq: ['$signalId',    '$$s'] },
              ] },
              isSubscribed: true,
          } },
          { $limit: 1 },
          { $project: { _id: 1 } },
        ],
        as: '_sub',
    } },
    { $match: { _sub: { $ne: [] } } },
    { $project: { _sub: 0 } },
  ];
}

/** $cond expression: 1 if severity == sev, else 0. Useful inside $sum. */
const sumWhereSeverity = (sev) => ({ $cond: [{ $eq: ['$severity', sev] }, 1, 0] });

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Collect the set of fingerprints from `signal_subscription_config` documents.
 * Tolerates schema drift: subscription docs may carry the fingerprint under
 * any of `fingerprint`, `fingerprints[]`, `alertFingerprint`, or
 * `subscription.fingerprint` / `config.fingerprint`.
 *
 * Used by the "orphan fingerprint detection" endpoint, which intentionally
 * ignores `isSubscribed` so the UI can flag both stale and opted-out rows.
 */
function fingerprintsFromSubscriptionDocs(docs) {
  const set = new Set();
  const add = (v) => { if (v != null && v !== '') set.add(String(v).trim()); };
  for (const d of docs) {
    add(d.fingerprint);
    if (Array.isArray(d.fingerprints)) d.fingerprints.forEach(add);
    add(d.alertFingerprint);
    add((d.subscription || d.config)?.fingerprint);
  }
  return set;
}

/**
 * Map of signalId (string) → { name, module, signalCode, isActive }.
 * Tiny collection (~10 docs); cached per request via the route wrapper would
 * be nice but premature given current volumes.
 */
async function loadSignalNameMap() {
  const docs = await db.collection('signals_master').find({}).toArray();
  const map = new Map();
  for (const d of docs) {
    map.set(String(d._id), {
      name: d.name,
      module: d.module,
      signalCode: d.signalCode,
      isActive: d.isActive,
    });
  }
  return map;
}

/** Compare two timestamps (ISO strings or Dates), return the newer one. */
function newerTimestamp(a, b) {
  if (!a) return b;
  if (!b) return a;
  return new Date(a) > new Date(b) ? a : b;
}

/** Project alert_overview row → public DTO. */
function alertToDTO(a) {
  return {
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
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Express setup + route helpers
// ─────────────────────────────────────────────────────────────────────────────

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

/**
 * Wraps an async route handler so any thrown / rejected error is logged and
 * returned as 500 JSON, removing per-route try/catch boilerplate.
 */
function asyncRoute(label, handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (err) {
      console.error(`[${label}]`, err.message);
      const status = err.statusCode || err.response?.status || 500;
      res.status(status).json({ error: err.message });
    }
  };
}

/** Parse a non-negative int from a query string, with bounds. */
function clampInt(value, fallback, max) {
  const n = parseInt(value, 10);
  if (Number.isNaN(n)) return fallback;
  return Math.min(Math.max(n, 0), max);
}

/** Escape a user-supplied string so it can be used inside a RegExp safely. */
function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ═════════════════════════════════════════════════════════════════════════════
// Signals & alerts
// ═════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/signals
 * Issues-tab card grid: every active signal with its CRIT/WARN/LOW counts.
 *
 * Counts are split for performance:
 *   - CRIT/WARN (alarms): strict $lookup — must be accurate.
 *   - LOW       (cleared): mirrored alert_overview.isSubscribed — fast but
 *     can lag by one poll cycle.
 *
 * Both sub-queries run in a single $facet round-trip.
 */
app.get('/api/signals', asyncRoute('/api/signals', async (_req, res) => {
  const [signals, facetResults] = await Promise.all([
    db.collection('signals_master').find({ isActive: true }).sort({ name: 1 }).toArray(),
    db.collection('alert_overview').aggregate([
      { $facet: {
          strict: [
            { $match: ACTIVE_MATCH },
            ...activeSubscriptionFilter(),
            { $group: { _id: { signalId: '$signalId', severity: '$severity' }, count: { $sum: 1 } } },
          ],
          low: [
            { $match: { severity: 'LOW', isSubscribed: true } },
            { $group: { _id: '$signalId', count: { $sum: 1 } } },
          ],
      } },
    ]).toArray(),
  ]);

  const facet = facetResults[0] || { strict: [], low: [] };
  const countsBySignal = new Map(); // signalId → { CRITICAL, WARNING, LOW }
  const ensure = (sid) => {
    if (!countsBySignal.has(sid)) countsBySignal.set(sid, { CRITICAL: 0, WARNING: 0, LOW: 0 });
    return countsBySignal.get(sid);
  };

  for (const r of facet.strict) {
    const bucket = ensure(String(r._id.signalId));
    if (r._id.severity in bucket) bucket[r._id.severity] = r.count;
  }
  for (const r of facet.low) {
    ensure(String(r._id)).LOW = r.count;
  }

  const data = signals
    .map((s) => {
      const sc = countsBySignal.get(String(s._id)) || { CRITICAL: 0, WARNING: 0, LOW: 0 };
      return {
        _id: String(s._id),
        signalCode: s.signalCode,
        name: s.name,
        description: s.description,
        module: s.module,
        uiModule: s.uiModule,
        severityCounts: sc,
        total: sc.CRITICAL + sc.WARNING + sc.LOW,
      };
    })
    .sort((a, b) => b.total - a.total);

  res.json({ data });
}));

/**
 * GET /api/signals/:signalId/subscription-fingerprints
 * Returns every fingerprint registered for this signal — including inactive
 * subscriptions. Used by the Alert Situation view to flag orphan fingerprints
 * (alerts whose fingerprint does not appear in any subscription doc).
 */
app.get('/api/signals/:signalId/subscription-fingerprints', asyncRoute(
  '/api/signals/:signalId/subscription-fingerprints',
  async (req, res) => {
    const { signalId } = req.params;
    const docs = await db.collection('signal_subscription_config')
      .find({ $or: [{ signalId: String(signalId) }, { signalId }] })
      .toArray();
    res.json({ data: [...fingerprintsFromSubscriptionDocs(docs)] });
  }
));

/**
 * GET /api/alerts?signalId=&severity=
 * Paginated alert_overview rows scoped to a signal and/or severity.
 * When signalId is set, results are restricted to active subscriptions via $lookup.
 *
 * For severity=CRITICAL, also returns a small summary {tenantCount, facilityCount}
 * used by the severity drilldown header.
 */
app.get('/api/alerts', asyncRoute('/api/alerts', async (req, res) => {
  const { signalId, severity } = req.query;

  const stages = [];
  const match = {};
  if (signalId) match.signalId = signalId;
  if (severity) match.severity = severity;
  if (Object.keys(match).length) stages.push({ $match: match });

  if (signalId) stages.push(...activeSubscriptionFilter());

  stages.push({ $sort: { updatedAt: -1 } });
  // Severity-scoped lists are short (UI shows ~200); whole-signal dumps cap higher
  // because the Alert Situation view fetches up to 5k rows for trend charts.
  const limit = severity ? 200 : (signalId ? 5000 : 200);
  stages.push({ $limit: limit });

  const rawAlerts = await db.collection('alert_overview').aggregate(stages).toArray();

  let summary = null;
  if (severity === 'CRITICAL' && rawAlerts.length > 0) {
    summary = {
      tenantCount: new Set(rawAlerts.map((a) => a.tenantCode).filter(Boolean)).size,
      facilityCount: new Set(rawAlerts.map((a) => a.facilityCode).filter(Boolean)).size,
    };
  }

  res.json({ data: rawAlerts.map(alertToDTO), summary });
}));

/** Fetch a single fingerprint's history from the upstream API. */
async function fetchUpstreamHistory(fingerprint) {
  const url = `${ALERT_API_BASE}/alerts/${fingerprint}/history`;
  const r = await axios.get(url, { headers: { 'x-api-key': API_KEY }, timeout: 15_000 });
  return r.data;
}

/**
 * Run an async worker across a list of items with a concurrency cap.
 * Equivalent to `Promise.all(items.map(worker))` but never has more than
 * `concurrency` workers in flight at once.
 *
 * Returns an array aligned to `items`; each entry is `{ ok, value }` or
 * `{ ok: false, error }` so one failure doesn't kill the rest.
 */
async function pMapWithLimit(items, concurrency, worker) {
  const results = new Array(items.length);
  let next = 0;
  async function runner() {
    while (true) {
      const idx = next++;
      if (idx >= items.length) return;
      try {
        results[idx] = { ok: true, value: await worker(items[idx], idx) };
      } catch (err) {
        results[idx] = { ok: false, error: err.message };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, runner));
  return results;
}

/**
 * GET /api/alerts/:fingerprint/history
 * Proxy to the upstream alert history API. Used by the per-alert drawer.
 */
app.get('/api/alerts/:fingerprint/history', asyncRoute('/api/alerts/:fingerprint/history',
  async (req, res) => {
    const data = await fetchUpstreamHistory(req.params.fingerprint);
    res.json(data);
  }
));

/**
 * POST /api/alerts/histories
 * Batched fetch of upstream histories for many fingerprints.
 *
 * Browser HTTP/1.1 caps parallel connections to a single origin at ~6, so the
 * Alert Situation view (50+ fingerprints) used to take ~9 sequential rounds.
 * Server-side we fan out with a higher concurrency limit and return one
 * combined response — typically 3–5× faster end-to-end and just one HTTP
 * round-trip from the browser.
 *
 * Body:    { "fingerprints": ["fp1", "fp2", …] }
 * Returns: { "histories": { "fp1": [...], "fp2": [...] }, "errors": { "fp3": "msg" } }
 */
app.post('/api/alerts/histories', asyncRoute('/api/alerts/histories', async (req, res) => {
  const fps = Array.isArray(req.body?.fingerprints) ? req.body.fingerprints : [];
  if (fps.length === 0) {
    return res.json({ histories: {}, errors: {} });
  }
  // Cap input to a sane upper bound to avoid hammering upstream.
  const list = [...new Set(fps.filter((s) => typeof s === 'string' && s.length > 0))].slice(0, 500);

  const results = await pMapWithLimit(list, 20, fetchUpstreamHistory);

  const histories = {};
  const errors = {};
  list.forEach((fp, i) => {
    const r = results[i];
    if (r?.ok) histories[fp] = r.value;
    else errors[fp] = r?.error || 'unknown error';
  });
  res.json({ histories, errors });
}));

// ═════════════════════════════════════════════════════════════════════════════
// Impact analysis (business-facing)
// ═════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/impact/overview
 * Headline KPIs for the Impact landing page.
 * Coverage totals (totalSubscriptions, totalCustomers, totalSites) count only
 * `signal_subscription_config` rows with isSubscribed:true.
 */
app.get('/api/impact/overview', asyncRoute('/api/impact/overview', async (_req, res) => {
  const overview = db.collection('alert_overview');
  const subs = db.collection('signal_subscription_config');
  /** Coverage / denominators: only subscribed rows (same scope as activeSubscriptionFilter). */
  const SUBSCRIBED = { isSubscribed: true };

  const [activeFacet, totalCustomersArr, totalSitesArr, totalSignals, totalSubscriptions] = await Promise.all([
    overview.aggregate([
      { $match: ACTIVE_MATCH },
      ...activeSubscriptionFilter(),
      { $facet: {
          agg: [
            { $group: { _id: null,
                instances: { $sum: 1 },
                customers: { $addToSet: '$tenantCode' },
                sites:     { $addToSet: { t: '$tenantCode', f: '$facilityCode' } },
                signals:   { $addToSet: '$signalId' },
            } },
          ],
          bySev: [
            { $group: { _id: '$severity', n: { $sum: 1 } } },
          ],
      } },
    ]).toArray(),
    subs.distinct('tenantCode', SUBSCRIBED),
    subs.aggregate([
      { $match: SUBSCRIBED },
      { $group: { _id: { t: '$tenantCode', f: '$facilityCode' } } },
      { $count: 'n' },
    ]).toArray(),
    db.collection('signals_master').countDocuments({ isActive: true }),
    subs.countDocuments(SUBSCRIBED),
  ]);

  const agg = activeFacet[0]?.agg?.[0] ?? { instances: 0, customers: [], sites: [], signals: [] };
  const sev = Object.fromEntries((activeFacet[0]?.bySev ?? []).map((r) => [r._id, r.n]));

  res.json({ data: {
    instances:          agg.instances,
    customersAffected:  agg.customers.length,
    sitesAffected:      agg.sites.length,
    signalsFiring:      agg.signals.length,
    criticalCount:      sev.CRITICAL || 0,
    warningCount:       sev.WARNING  || 0,
    totalCustomers:     totalCustomersArr.length,
    totalSites:         totalSitesArr[0]?.n || 0,
    totalSignals,
    totalSubscriptions,
  } });
}));

/**
 * GET /api/impact/top-customers?limit=10
 * Customers ranked by # active issues, broken down by severity.
 */
app.get('/api/impact/top-customers', asyncRoute('/api/impact/top-customers', async (req, res) => {
  const limit = clampInt(req.query.limit, 10, 100);
  const rows = await db.collection('alert_overview').aggregate([
    { $match: ACTIVE_MATCH },
    ...activeSubscriptionFilter(),
    { $group: {
        _id: '$tenantCode',
        instances:   { $sum: 1 },
        critical:    { $sum: sumWhereSeverity('CRITICAL') },
        warning:     { $sum: sumWhereSeverity('WARNING')  },
        signalIds:   { $addToSet: '$signalId' },
        sites:       { $addToSet: '$facilityCode' },
        lastUpdated: { $max: '$updatedAt' },
    } },
    { $project: {
        _id: 0,
        tenantCode: '$_id',
        instances: 1, critical: 1, warning: 1, lastUpdated: 1,
        signalsAffected: { $size: '$signalIds' },
        sitesAffected:   { $size: '$sites' },
    } },
    { $sort: { critical: -1, instances: -1 } },
    { $limit: limit },
  ]).toArray();
  res.json({ data: rows });
}));

/**
 * GET /api/impact/top-issues?limit=10
 * Issues (signals) ranked by # customers affected, with name/module joined in.
 */
app.get('/api/impact/top-issues', asyncRoute('/api/impact/top-issues', async (req, res) => {
  const limit = clampInt(req.query.limit, 10, 100);
  const nameMap = await loadSignalNameMap();

  const rows = await db.collection('alert_overview').aggregate([
    { $match: ACTIVE_MATCH },
    ...activeSubscriptionFilter(),
    { $group: {
        _id: '$signalId',
        instances:   { $sum: 1 },
        critical:    { $sum: sumWhereSeverity('CRITICAL') },
        warning:     { $sum: sumWhereSeverity('WARNING')  },
        customers:   { $addToSet: '$tenantCode' },
        sites:       { $addToSet: { tenant: '$tenantCode', facility: '$facilityCode' } },
        lastUpdated: { $max: '$updatedAt' },
    } },
    { $project: {
        _id: 0,
        signalId: '$_id',
        instances: 1, critical: 1, warning: 1, lastUpdated: 1,
        customersAffected: { $size: '$customers' },
        sitesAffected:     { $size: '$sites' },
    } },
    { $sort: { customersAffected: -1, critical: -1 } },
    { $limit: limit },
  ]).toArray();

  const data = rows.map((r) => {
    const meta = nameMap.get(String(r.signalId)) ?? {};
    return {
      ...r,
      name:       meta.name       ?? '(unknown)',
      module:     meta.module     ?? null,
      signalCode: meta.signalCode ?? null,
    };
  });
  res.json({ data });
}));

/**
 * GET /api/impact/categories
 * Impact roll-up by signals_master.module — useful for "which product area is loudest".
 */
app.get('/api/impact/categories', asyncRoute('/api/impact/categories', async (_req, res) => {
  const nameMap = await loadSignalNameMap();
  const rows = await db.collection('alert_overview').aggregate([
    { $match: ACTIVE_MATCH },
    ...activeSubscriptionFilter(),
    { $group: {
        _id: '$signalId',
        instances: { $sum: 1 },
        customers: { $addToSet: '$tenantCode' },
    } },
  ]).toArray();

  const byModule = new Map();
  for (const r of rows) {
    const moduleName = nameMap.get(String(r._id))?.module || 'unknown';
    if (!byModule.has(moduleName)) {
      byModule.set(moduleName, { module: moduleName, instances: 0, customers: new Set(), signals: 0 });
    }
    const bucket = byModule.get(moduleName);
    bucket.instances += r.instances;
    r.customers.forEach((c) => bucket.customers.add(c));
    bucket.signals += 1;
  }

  const data = [...byModule.values()]
    .map(({ module, instances, customers, signals }) => ({
      module, instances,
      customersAffected: customers.size,
      signalsAffected:   signals,
    }))
    .sort((a, b) => b.instances - a.instances);

  res.json({ data });
}));

// ═════════════════════════════════════════════════════════════════════════════
// Customers
// ═════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/customers
 * Every customer with their subscription footprint and current issue load.
 */
app.get('/api/customers', asyncRoute('/api/customers', async (_req, res) => {
  const [subRows, activeRows] = await Promise.all([
    db.collection('signal_subscription_config').aggregate([
      { $group: {
          _id: '$tenantCode',
          sites:         { $addToSet: '$facilityCode' },
          signals:       { $addToSet: '$signalId' },
          subscriptions: { $sum: 1 },
      } },
      { $project: {
          _id: 0, tenantCode: '$_id',
          sites:             { $size: '$sites' },
          signalsSubscribed: { $size: '$signals' },
          subscriptions: 1,
      } },
    ]).toArray(),
    db.collection('alert_overview').aggregate([
      { $match: ACTIVE_MATCH },
      ...activeSubscriptionFilter(),
      { $group: {
          _id: '$tenantCode',
          issuesFiring: { $sum: 1 },
          critical:     { $sum: sumWhereSeverity('CRITICAL') },
          lastUpdated:  { $max: '$updatedAt' },
      } },
    ]).toArray(),
  ]);

  const activeMap = new Map(
    activeRows.map((r) => [r._id, { issuesFiring: r.issuesFiring, critical: r.critical, lastUpdated: r.lastUpdated }])
  );

  const data = subRows
    .map((r) => ({
      ...r,
      issuesFiring: activeMap.get(r.tenantCode)?.issuesFiring ?? 0,
      critical:     activeMap.get(r.tenantCode)?.critical     ?? 0,
      lastUpdated:  activeMap.get(r.tenantCode)?.lastUpdated  ?? null,
    }))
    .sort((a, b) =>
      (b.critical - a.critical)
      || (b.issuesFiring - a.issuesFiring)
      || a.tenantCode.localeCompare(b.tenantCode)
    );

  res.json({ data });
}));

/**
 * GET /api/customers/:tenantCode
 * Drilldown: subscription footprint + active issues grouped by signal and by site.
 */
app.get('/api/customers/:tenantCode', asyncRoute('/api/customers/:tenantCode', async (req, res) => {
  const { tenantCode } = req.params;
  const nameMap = await loadSignalNameMap();

  const [subs, activeAlerts] = await Promise.all([
    db.collection('signal_subscription_config').find({ tenantCode }).toArray(),
    db.collection('alert_overview').aggregate([
      { $match: { tenantCode, ...ACTIVE_MATCH } },
      ...activeSubscriptionFilter(),
      { $sort: { updatedAt: -1 } },
    ]).toArray(),
  ]);

  if (subs.length === 0 && activeAlerts.length === 0) {
    return res.status(404).json({ error: `No data for tenant '${tenantCode}'` });
  }

  // Subscription footprint
  const sites = new Set();
  const subscribedSignals = new Set();
  for (const s of subs) {
    if (s.facilityCode) sites.add(s.facilityCode);
    if (s.signalId)     subscribedSignals.add(String(s.signalId));
  }

  // Generic group-by-key utility for the two breakdowns below.
  // Each bucket: { critical, warning, lastUpdated, ... } + a Set seeded by `setKey`.
  const groupAlerts = (keyFn, setKey) => {
    const acc = new Map();
    for (const a of activeAlerts) {
      const k = keyFn(a);
      if (!acc.has(k)) acc.set(k, { instances: 0, critical: 0, warning: 0, [setKey]: new Set(), lastUpdated: null });
      const bucket = acc.get(k);
      bucket.instances += 1;
      if (a.severity === 'CRITICAL') bucket.critical += 1;
      if (a.severity === 'WARNING')  bucket.warning  += 1;
      bucket.lastUpdated = newerTimestamp(bucket.lastUpdated, a.updatedAt);
      // Caller decides what gets added to the Set
      if (setKey === 'sites'   && a.facilityCode) bucket[setKey].add(a.facilityCode);
      if (setKey === 'signals' && a.signalId)     bucket[setKey].add(String(a.signalId));
    }
    return acc;
  };

  const bySignal = groupAlerts((a) => String(a.signalId), 'sites');
  const issues = [...bySignal.entries()]
    .map(([signalId, b]) => {
      const meta = nameMap.get(signalId) ?? {};
      return {
        signalId,
        name:           meta.name       ?? '(unknown)',
        module:         meta.module     ?? null,
        signalCode:     meta.signalCode ?? null,
        instances:      b.instances,
        critical:       b.critical,
        warning:        b.warning,
        sitesAffected:  b.sites.size,
        lastUpdated:    b.lastUpdated,
      };
    })
    .sort((a, b) => (b.critical - a.critical) || (b.instances - a.instances));

  const bySite = groupAlerts((a) => a.facilityCode || '—', 'signals');
  const sitesData = [...bySite.entries()]
    .map(([facilityCode, b]) => ({
      facilityCode,
      instances:        b.instances,
      critical:         b.critical,
      warning:          b.warning,
      signalsAffected:  b.signals.size,
      lastUpdated:      b.lastUpdated,
    }))
    .sort((a, b) => (b.critical - a.critical) || (b.instances - a.instances));

  res.json({
    data: {
      tenantCode,
      summary: {
        sites:              sites.size,
        signalsSubscribed:  subscribedSignals.size,
        subscriptions:      subs.length,
        issuesFiring:       activeAlerts.length,
        critical:           activeAlerts.filter((a) => a.severity === 'CRITICAL').length,
        warning:            activeAlerts.filter((a) => a.severity === 'WARNING').length,
        sitesAffected:      new Set(activeAlerts.map((a) => a.facilityCode).filter(Boolean)).size,
      },
      issues,
      sites: sitesData,
    },
  });
}));

/**
 * GET /api/signals/:signalId/customers
 * For an issue: every currently-affected customer, with sites expanded.
 */
app.get('/api/signals/:signalId/customers', asyncRoute('/api/signals/:signalId/customers',
  async (req, res) => {
    const { signalId } = req.params;
    const rows = await db.collection('alert_overview').aggregate([
      { $match: { ...ACTIVE_MATCH, signalId } },
      ...activeSubscriptionFilter(),
      { $sort: { severity: 1, updatedAt: -1 } },
    ]).toArray();

    const byTenant = new Map();
    for (const a of rows) {
      const t = a.tenantCode || '—';
      if (!byTenant.has(t)) {
        byTenant.set(t, { tenantCode: t, instances: 0, critical: 0, warning: 0, sites: [], lastUpdated: null });
      }
      const b = byTenant.get(t);
      b.instances += 1;
      if (a.severity === 'CRITICAL') b.critical += 1;
      if (a.severity === 'WARNING')  b.warning  += 1;
      b.sites.push({
        facilityCode: a.facilityCode,
        severity:     a.severity,
        fingerprint:  a.fingerprint,
        updatedAt:    a.updatedAt,
        heading:      a.template?.heading || null,
      });
      b.lastUpdated = newerTimestamp(b.lastUpdated, a.updatedAt);
    }

    const data = [...byTenant.values()]
      .sort((a, b) => (b.critical - a.critical) || (b.instances - a.instances));
    res.json({ data });
  }
));

// ═════════════════════════════════════════════════════════════════════════════
// Subscriptions
// ═════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/subscriptions/overview
 * Coverage KPIs. Every "covered" count is scoped to active subscriptions
 * (isSubscribed:true) — inactive rows represent customers who opted out.
 */
app.get('/api/subscriptions/overview', asyncRoute('/api/subscriptions/overview', async (_req, res) => {
  const subs = db.collection('signal_subscription_config');
  const ACTIVE = { isSubscribed: true };

  const [
    totalSubs,
    activeSubs,
    customersCovered,
    siteRows,
    issuesWithSubs,
    perCustomerCounts,
    totalSignals,
  ] = await Promise.all([
    subs.countDocuments({}),
    subs.countDocuments(ACTIVE),
    subs.distinct('tenantCode', ACTIVE),
    subs.aggregate([
      { $match: ACTIVE },
      { $group: { _id: { t: '$tenantCode', f: '$facilityCode' } } },
      { $count: 'n' },
    ]).toArray(),
    subs.distinct('signalId', ACTIVE),
    subs.aggregate([
      { $match: ACTIVE },
      { $group: { _id: '$tenantCode', n: { $sum: 1 } } },
      { $sort: { n: -1 } },
    ]).toArray(),
    db.collection('signals_master').countDocuments({ isActive: true }),
  ]);

  // median / avg / max of subscriptions-per-customer
  const counts = perCustomerCounts.map((r) => r.n).sort((a, b) => a - b);
  const len = counts.length;
  const median = len === 0 ? 0
    : len % 2 === 1 ? counts[(len - 1) / 2]
    : Math.round((counts[len / 2 - 1] + counts[len / 2]) / 2);
  const avg = len === 0 ? 0 : Math.round(counts.reduce((s, n) => s + n, 0) / len);
  const max = len === 0 ? 0 : counts[len - 1];

  res.json({ data: {
    totalSubscriptions:    totalSubs,
    activeSubscriptions:   activeSubs,
    inactiveSubscriptions: totalSubs - activeSubs,
    customersCovered:      customersCovered.length,
    sitesCovered:          siteRows[0]?.n || 0,
    issuesWithSubscribers: issuesWithSubs.length,
    totalSignals,
    avgSubsPerCustomer:    avg,
    medianSubsPerCustomer: median,
    maxSubsPerCustomer:    max,
  } });
}));

/**
 * GET /api/subscriptions/by-issue
 * Per signal: counts of customers / sites / subscriptions. `customers` and
 * `sites` reflect ACTIVE subscribers; `subscriptions` and `activeSubs` show
 * raw row counts so drift is visible.
 */
app.get('/api/subscriptions/by-issue', asyncRoute('/api/subscriptions/by-issue', async (_req, res) => {
  const nameMap = await loadSignalNameMap();
  const rows = await db.collection('signal_subscription_config').aggregate([
    { $group: {
        _id: '$signalId',
        subscriptions:    { $sum: 1 },
        activeSubs:       { $sum: { $cond: ['$isSubscribed', 1, 0] } },
        // $$REMOVE drops the value from the $addToSet so only active rows contribute.
        activeCustomers:  { $addToSet: { $cond: ['$isSubscribed', '$tenantCode', '$$REMOVE'] } },
        activeSites:      { $addToSet: { $cond: ['$isSubscribed', { t: '$tenantCode', f: '$facilityCode' }, '$$REMOVE'] } },
    } },
    { $project: {
        _id: 0, signalId: '$_id',
        subscriptions: 1, activeSubs: 1,
        customers: { $size: '$activeCustomers' },
        sites:     { $size: '$activeSites' },
    } },
    { $sort: { customers: -1 } },
  ]).toArray();

  const data = rows.map((r) => {
    const meta = nameMap.get(String(r.signalId)) ?? {};
    return {
      ...r,
      name:       meta.name       ?? '(unknown)',
      module:     meta.module     ?? null,
      signalCode: meta.signalCode ?? null,
      isActive:   meta.isActive !== false,
    };
  });
  res.json({ data });
}));

/**
 * GET /api/subscriptions/list?tenant=&facility=&signal=&active=&q=&limit=&skip=
 * Filterable, paginated listing of raw subscription rows.
 */
app.get('/api/subscriptions/list', asyncRoute('/api/subscriptions/list', async (req, res) => {
  const { tenant, facility, signal, active, q } = req.query;
  const limit = clampInt(req.query.limit, 100, 500);
  const skip  = clampInt(req.query.skip,  0,   1_000_000);

  const match = {};
  if (tenant)   match.tenantCode   = tenant;
  if (facility) match.facilityCode = facility;
  if (signal)   match.signalId     = signal;
  if (active === 'true')  match.isSubscribed = true;
  if (active === 'false') match.isSubscribed = false;
  if (q) {
    const re = new RegExp(escapeRegex(q), 'i');
    match.$or = [{ tenantCode: re }, { facilityCode: re }, { fingerprint: re }];
  }

  const coll = db.collection('signal_subscription_config');
  const [rows, total] = await Promise.all([
    coll.find(match).sort({ tenantCode: 1, facilityCode: 1 }).skip(skip).limit(limit).toArray(),
    coll.countDocuments(match),
  ]);

  const nameMap = await loadSignalNameMap();
  const data = rows.map((r) => {
    const meta = nameMap.get(String(r.signalId)) ?? {};
    return {
      _id:           String(r._id),
      tenantCode:    r.tenantCode,
      facilityCode:  r.facilityCode,
      signalId:      String(r.signalId),
      signalName:    meta.name   ?? null,
      signalModule:  meta.module ?? null,
      fingerprint:   r.fingerprint || null,
      isSubscribed:  r.isSubscribed !== false,
      config:        r.config || null,
      updatedAt:     r.updatedAt || null,
    };
  });
  res.json({ data, total, skip, limit });
}));

/**
 * GET /api/subscriptions/matrix
 * Customer × issue grid of subscription counts. Active subscriptions only.
 */
app.get('/api/subscriptions/matrix', asyncRoute('/api/subscriptions/matrix', async (_req, res) => {
  const nameMap = await loadSignalNameMap();
  const issues = [...nameMap.entries()]
    .filter(([, m]) => m.isActive !== false)
    .map(([id, m]) => ({ signalId: id, name: m.name, module: m.module, signalCode: m.signalCode }));

  const rows = await db.collection('signal_subscription_config').aggregate([
    { $match: { isSubscribed: true } },
    { $group: { _id: { tenant: '$tenantCode', signal: '$signalId' }, n: { $sum: 1 } } },
  ]).toArray();

  const byTenant = new Map();
  for (const r of rows) {
    const t = r._id.tenant;
    const s = String(r._id.signal);
    if (!byTenant.has(t)) byTenant.set(t, { tenantCode: t, totals: {}, total: 0, issuesCovered: 0 });
    const bucket = byTenant.get(t);
    bucket.totals[s] = r.n;
    bucket.total += r.n;
  }

  const customers = [...byTenant.values()]
    .map((c) => ({ ...c, issuesCovered: Object.keys(c.totals).length }))
    .sort((a, b) => b.total - a.total);

  res.json({ data: { issues, customers } });
}));

// ═════════════════════════════════════════════════════════════════════════════
// Server bootstrap
// ═════════════════════════════════════════════════════════════════════════════

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

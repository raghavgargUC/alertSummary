#!/usr/bin/env node
/**
 * Like export-alert-timeline-csv.js, but only rows where ALL:
 *   - signal_subscription_config.isSubscribed === true (loaded first; optional CLI filters apply here)
 *   - matching alert_overview for same (tenantCode, facilityCode, signalId) with
 *     isSubscribed === true and updatedAt < script start time (indexed $lookup per subscription)
 *
 * (There is no alert_overview.is_active field; “active” here is subscribed + snapshot cutoff.)
 *
 * Requires .env: MONGO_URL, DB_NAME, ALERT_API_BASE, API_KEY
 *
 * Usage:
 *   node scripts/export-alert-timeline-csv-subscribed.js
 *   node scripts/export-alert-timeline-csv-subscribed.js --out=./report.csv
 *   node scripts/export-alert-timeline-csv-subscribed.js --tenant=curefit --facility=BLR_01 --signal=...
 *
 * Filters (all optional): --tenant= --facility= --signal=
 */

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URL = process.env.MONGO_URL;
const DB_NAME = process.env.DB_NAME;
const ALERT_API_BASE = process.env.ALERT_API_BASE;
const API_KEY = process.env.API_KEY;

function log(...args) {
  console.log('[export-csv-subscribed]', ...args);
}

function parseArgs() {
  const opts = {
    tenant: null,
    facility: null,
    signal: null,
    out: path.join(process.cwd(), 'subscribed_alerts_timeline.csv'),
  };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith('--out=')) opts.out = path.resolve(a.slice(6));
    else if (a.startsWith('--tenant=')) opts.tenant = a.slice(9) || null;
    else if (a.startsWith('--facility=')) opts.facility = a.slice(11) || null;
    else if (a.startsWith('--signal=')) opts.signal = a.slice(9) || null;
  }
  return opts;
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

function csvEscape(val) {
  if (val == null) return '';
  const s = String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toIso(ts) {
  if (ts == null) return '';
  const d = ts instanceof Date ? ts : new Date(ts);
  return Number.isNaN(d.getTime()) ? String(ts) : d.toISOString();
}

async function fetchAlertHistory(fingerprint) {
  const upstream = `${ALERT_API_BASE}/alerts/${encodeURIComponent(fingerprint)}/history`;
  const response = await axios.get(upstream, {
    headers: { 'x-api-key': API_KEY },
    timeout: 15000,
  });
  const body = response.data;
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.data)) return body.data;
  return [];
}

function buildMatch(opts) {
  const match = {};
  if (opts.tenant) match.tenantCode = opts.tenant;
  if (opts.facility) match.facilityCode = opts.facility;
  if (opts.signal) {
    const oid = objectIdIfValid(opts.signal);
    match.$or = [{ signalId: String(opts.signal) }, ...(oid ? [{ signalId: oid }] : [])];
  }
  return match;
}

/**
 * Start from signal_subscription_config (isSubscribed + CLI filters), $lookup alert_overview
 * on compound key with isSubscribed + updatedAt snapshot — one server-side join pass.
 */
function overviewFromActiveSubscriptionsPipeline(subBaseMatch, updatedBefore) {
  return [
    { $match: { ...subBaseMatch, isSubscribed: true } },
    {
      $lookup: {
        from: 'alert_overview',
        let: { t: '$tenantCode', f: '$facilityCode', s: '$signalId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$tenantCode', '$$t'] },
                  { $eq: ['$facilityCode', '$$f'] },
                  { $eq: ['$signalId', '$$s'] },
                ],
              },
              isSubscribed: true,
              updatedAt: { $lt: updatedBefore },
            },
          },
        ],
        as: '_ov',
      },
    },
    { $match: { _ov: { $ne: [] } } },
    { $unwind: '$_ov' },
    { $replaceRoot: { newRoot: '$_ov' } },
    { $sort: { _id: 1 } },
  ];
}

async function main() {
  if (!MONGO_URL || !DB_NAME || !ALERT_API_BASE || !API_KEY) {
    console.error('Missing env: MONGO_URL, DB_NAME, ALERT_API_BASE, API_KEY');
    process.exit(1);
  }

  const scriptStart = new Date();

  const opts = parseArgs();
  const match = buildMatch(opts);
  const subMatch = { isSubscribed: true, ...match };

  log('filters:', {
    tenantCode: opts.tenant ?? '(any)',
    facilityCode: opts.facility ?? '(any)',
    signalId: opts.signal ?? '(any)',
    signal_subscription_config_isSubscribed: true,
    alert_overview_updatedAt_lt: scriptStart.toISOString(),
    alert_overview_isSubscribed: true,
  });
  log('output:', opts.out);

  const client = new MongoClient(MONGO_URL, { connectTimeoutMS: 10000, serverSelectionTimeoutMS: 10000 });
  await client.connect();
  log(`connected MongoDB → ${DB_NAME}`);
  const db = client.db(DB_NAME);

  const subscribedConfigCount = await db.collection('signal_subscription_config').countDocuments(subMatch);
  log(`signal_subscription_config (isSubscribed=true, filters): ${subscribedConfigCount} document(s)`);

  const overviewRows = await db
    .collection('signal_subscription_config')
    .aggregate(overviewFromActiveSubscriptionsPipeline(match, scriptStart), { allowDiskUse: true })
    .toArray();

  log(`alert_overview rows joined from subscriptions (sort _id asc): ${overviewRows.length}`);

  const signalIdStrs = [...new Set(overviewRows.map((r) => String(r.signalId ?? '')).filter(Boolean))];
  const nameBySignalId = new Map();
  if (signalIdStrs.length > 0) {
    const oids = signalIdStrs.map(objectIdIfValid).filter(Boolean);
    const masters = await db
      .collection('signals_master')
      .find({
        $or: [{ _id: { $in: oids } }, { _id: { $in: signalIdStrs } }],
      })
      .toArray();
    for (const m of masters) {
      nameBySignalId.set(m._id.toString(), m.name ?? '');
    }
    log(`signals_master names resolved: ${masters.length} / ${signalIdStrs.length} distinct signalId(s)`);
  }

  await client.close();
  log('MongoDB connection closed');

  const historyCache = new Map();
  const rows = [];
  let rowsWithoutFingerprint = 0;
  let historyFetchErrors = 0;
  let historyCacheReuses = 0;

  const totalRows = overviewRows.length;
  const processStarted = Date.now();
  let lastPrintedPct = -1;

  if (totalRows === 0) {
    log('progress 100% (0/0), 0 row(s) left (~0% remaining), ETA ~—');
  }

  for (let i = 0; i < overviewRows.length; i++) {
    const a = overviewRows[i];
    const sid = String(a.signalId ?? '');
    const signalName = nameBySignalId.get(sid) ?? '';
    const fp = a.fingerprint != null && String(a.fingerprint).trim() !== '' ? String(a.fingerprint).trim() : null;

    let history = [];
    if (fp) {
      if (!historyCache.has(fp)) {
        try {
          const h = await fetchAlertHistory(fp);
          historyCache.set(fp, h);
        } catch (e) {
          console.error('[export-csv-subscribed] history failed', fp, e.message);
          historyFetchErrors += 1;
          historyCache.set(fp, null);
        }
      } else {
        historyCacheReuses += 1;
      }
      const h = historyCache.get(fp);
      history = h === null ? [] : h;
    } else {
      rowsWithoutFingerprint += 1;
    }

    const base = [a.tenantCode ?? '', a.facilityCode ?? '', signalName, sid];

    if (history.length > 0) {
      const sorted = [...history].sort(
        (x, y) => new Date(x.lastReceived || 0) - new Date(y.lastReceived || 0)
      );
      for (const entry of sorted) {
        rows.push([
          ...base,
          (entry.severity ?? '').toString(),
          a.status ?? '',
          toIso(entry.lastReceived),
        ]);
      }
    } else {
      rows.push([...base, a.severity ?? '', a.status ?? '', toIso(a.updatedAt ?? a.createdAt)]);
    }

    const done = i + 1;
    const pct = totalRows === 0 ? 100 : Math.min(100, Math.floor((done / totalRows) * 100));
    if (pct !== lastPrintedPct || done === totalRows) {
      lastPrintedPct = pct;
      const left = totalRows - done;
      const leftPct = totalRows === 0 ? 0 : Math.max(0, 100 - pct);
      let eta = '—';
      if (done > 0 && left > 0) {
        const elapsed = Date.now() - processStarted;
        const msPerRow = elapsed / done;
        eta = `${Math.ceil((msPerRow * left) / 1000)}s`;
      }
      log(
        `progress ${pct}% (${done}/${totalRows}), ${left} row(s) left (~${leftPct}% remaining), ETA ~${eta}`
      );
    }
  }

  const header = [
    'tenant_code',
    'facility_code',
    'signal_master_name',
    'signal_id',
    'severity',
    'status',
    'timestamp',
  ];
  const lines = [header.map(csvEscape).join(',')];
  for (const r of rows) lines.push(r.map(csvEscape).join(','));
  const csv = lines.join('\r\n');

  fs.writeFileSync(opts.out, csv, 'utf8');
  const uniqueFp = historyCache.size;
  let apiCalls = 0;
  for (const v of historyCache.values()) if (v !== null) apiCalls += 1;
  log('summary:', {
    subscribedConfigDocuments: subscribedConfigCount,
    overviewRows: overviewRows.length,
    uniqueFingerprints: uniqueFp,
    historyApiCalls: apiCalls,
    historyCacheReuses,
    historyFetchErrors,
    rowsWithoutFingerprint,
    csvDataRows: rows.length,
    file: opts.out,
  });
  if (historyCacheReuses > 0) {
    log(
      `history cache: skipped ${historyCacheReuses} redundant API call(s) (same fingerprint on multiple overview rows)`
    );
  }
  log(`done → ${rows.length} data rows (+ header)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

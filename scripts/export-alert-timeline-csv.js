#!/usr/bin/env node
/**
 * Reads alert_overview (sorted by _id ascending), fetches timeline per fingerprint
 * from the alert history API, joins signal name from signals_master, writes CSV.
 *
 * Requires .env: MONGO_URL, DB_NAME, ALERT_API_BASE, API_KEY
 *
 * Usage:
 *   node scripts/export-alert-timeline-csv.js
 *   node scripts/export-alert-timeline-csv.js --out=./report.csv
 *   node scripts/export-alert-timeline-csv.js --tenant=curefit --facility=BLR_01 --signal=69d8a7f4f021bd58b2f8255e
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
  console.log('[export-csv]', ...args);
}

function parseArgs() {
  const opts = { tenant: null, facility: null, signal: null, out: path.join(process.cwd(), 'alert_timeline.csv') };
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
  log(`GET history → ${fingerprint}`);
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

async function main() {
  if (!MONGO_URL || !DB_NAME || !ALERT_API_BASE || !API_KEY) {
    console.error('Missing env: MONGO_URL, DB_NAME, ALERT_API_BASE, API_KEY');
    process.exit(1);
  }

  const opts = parseArgs();
  const match = buildMatch(opts);

  log('filters:', {
    tenantCode: opts.tenant ?? '(any)',
    facilityCode: opts.facility ?? '(any)',
    signalId: opts.signal ?? '(any)',
  });
  log('output:', opts.out);

  const client = new MongoClient(MONGO_URL, { connectTimeoutMS: 10000, serverSelectionTimeoutMS: 10000 });
  await client.connect();
  log(`connected MongoDB → ${DB_NAME}`);
  const db = client.db(DB_NAME);

  const overviewRows = await db
    .collection('alert_overview')
    .find(match)
    .sort({ _id: 1 })
    .toArray();

  log(`alert_overview rows (sort _id asc): ${overviewRows.length}`);

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

  // Multiple alert_overview documents can share the same fingerprint; history is identical per
  // fingerprint. Cache = one HTTP GET per unique fingerprint instead of N GETs for N rows.
  const historyCache = new Map();
  const rows = [];
  let rowsWithoutFingerprint = 0;
  let historyFetchErrors = 0;
  let historyCacheReuses = 0;

  for (const a of overviewRows) {
    const sid = String(a.signalId ?? '');
    const signalName = nameBySignalId.get(sid) ?? '';
    const fp = a.fingerprint != null && String(a.fingerprint).trim() !== '' ? String(a.fingerprint).trim() : null;

    let history = [];
    if (fp) {
      if (!historyCache.has(fp)) {
        try {
          const h = await fetchAlertHistory(fp);
          historyCache.set(fp, h);
          log(`cached history for fingerprint ${fp} → ${h.length} point(s)`);
        } catch (e) {
          console.error('[export-csv] history failed', fp, e.message);
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

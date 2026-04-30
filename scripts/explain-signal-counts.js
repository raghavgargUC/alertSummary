#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const { MongoClient } = require('mongodb');

const SIGNAL_ID = '69d8a7f4f021bd58b2f8255e'; // SLA BREACH RISK

(async () => {
  const client = new MongoClient(process.env.MONGO_URL, { serverSelectionTimeoutMS: 8000 });
  await client.connect();
  const db = client.db(process.env.DB_NAME);

  console.log(`\nSignal: SLA BREACH RISK  (signalId=${SIGNAL_ID})\n`);

  // 1) Raw alert_overview rows for this signal — no other filter
  console.log('── 1) alert_overview rows for this signal, by severity (no other filter) ──');
  const raw = await db.collection('alert_overview').aggregate([
    { $match: { signalId: SIGNAL_ID } },
    { $group: { _id: '$severity', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();
  console.table(raw);

  // 2) Same, but also break down by status
  console.log('── 2) ... also broken down by status ──');
  const rawByStatus = await db.collection('alert_overview').aggregate([
    { $match: { signalId: SIGNAL_ID } },
    { $group: { _id: { sev: '$severity', status: '$status' }, count: { $sum: 1 } } },
    { $sort: { '_id.sev': 1, '_id.status': 1 } },
  ]).toArray();
  console.table(rawByStatus.map(r => ({ severity: r._id.sev, status: r._id.status, count: r.count })));

  // 3) Subscription config for this signal
  console.log('── 3) signal_subscription_config rows for this signal ──');
  const subTotal = await db.collection('signal_subscription_config').countDocuments({ signalId: SIGNAL_ID });
  const subActive = await db.collection('signal_subscription_config').countDocuments({ signalId: SIGNAL_ID, isSubscribed: true });
  const subInactive = await db.collection('signal_subscription_config').countDocuments({ signalId: SIGNAL_ID, isSubscribed: false });
  const subFpDistinct = (await db.collection('signal_subscription_config').distinct('fingerprint', { signalId: SIGNAL_ID })).filter(Boolean).length;
  console.table([
    { kind: 'total subscription rows', count: subTotal },
    { kind: 'isSubscribed = true',     count: subActive },
    { kind: 'isSubscribed = false',    count: subInactive },
    { kind: 'distinct fingerprint values (non-null)', count: subFpDistinct },
  ]);

  // 4) The exact filter /api/signals applies — fingerprint must appear in subscription_config for this signal
  console.log('── 4) THE API LOGIC: alert_overview where fingerprint ∈ subscription_config(this signal), by severity ──');
  const fpDocs = await db.collection('signal_subscription_config').find({ signalId: SIGNAL_ID }).toArray();
  const fpSet = new Set();
  for (const d of fpDocs) {
    if (d.fingerprint) fpSet.add(String(d.fingerprint).trim());
    if (Array.isArray(d.fingerprints)) for (const f of d.fingerprints) if (f) fpSet.add(String(f).trim());
    if (d.alertFingerprint) fpSet.add(String(d.alertFingerprint).trim());
    const nested = d.subscription || d.config;
    if (nested?.fingerprint) fpSet.add(String(nested.fingerprint).trim());
  }
  const apiResult = await db.collection('alert_overview').aggregate([
    { $match: { signalId: SIGNAL_ID, fingerprint: { $in: [...fpSet] } } },
    { $group: { _id: '$severity', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();
  console.table(apiResult);
  console.log(`fingerprints in $in set: ${fpSet.size}`);

  // 5) Orphans — alert_overview rows for this signal whose fingerprint is NOT in subscription_config
  console.log('── 5) Orphan alert_overview rows (fingerprint NOT in subscription_config), by severity ──');
  const orphans = await db.collection('alert_overview').aggregate([
    { $match: { signalId: SIGNAL_ID, $or: [
        { fingerprint: { $nin: [...fpSet] } },
        { fingerprint: null },
        { fingerprint: '' },
    ] } },
    { $group: { _id: '$severity', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();
  console.table(orphans);

  // 6) Active-only check — what the Impact tab counts (FIRING + CRITICAL/WARNING + fingerprint in GLOBAL subscription set)
  console.log('── 6) IMPACT TAB LOGIC for this signal: status=FIRING ∧ sev∈{CRIT,WARN} ∧ fp ∈ subscription_config (global) ──');
  const allSubFps = await db.collection('signal_subscription_config').find({ fingerprint: { $ne: null } }, { projection: { fingerprint: 1 } }).toArray();
  const globalFpSet = new Set(allSubFps.map(d => String(d.fingerprint).trim()).filter(Boolean));
  const impactResult = await db.collection('alert_overview').aggregate([
    { $match: {
        signalId: SIGNAL_ID,
        status: 'FIRING',
        severity: { $in: ['CRITICAL', 'WARNING'] },
        fingerprint: { $in: [...globalFpSet] },
    } },
    { $group: { _id: '$severity', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();
  console.table(impactResult);
  console.log(`global fingerprint set size: ${globalFpSet.size}`);

  await client.close();
})().catch((e) => { console.error(e); process.exit(1); });

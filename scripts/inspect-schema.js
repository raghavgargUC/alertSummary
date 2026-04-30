#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const { MongoClient } = require('mongodb');

(async () => {
  const client = new MongoClient(process.env.MONGO_URL, { serverSelectionTimeoutMS: 8000 });
  await client.connect();
  const db = client.db(process.env.DB_NAME);

  for (const coll of ['signals_master', 'signal_subscription_config', 'alert_overview']) {
    const sample = await db.collection(coll).findOne({});
    const count = await db.collection(coll).estimatedDocumentCount();
    console.log(`\n── ${coll} (≈${count} docs) ──`);
    console.log(JSON.stringify(sample, null, 2));
  }

  // For alert_overview, also check distinct severities, statuses, and date range
  console.log('\n── alert_overview field overview ──');
  const sevs = await db.collection('alert_overview').distinct('severity');
  const statuses = await db.collection('alert_overview').distinct('status');
  const minMaxCreated = await db.collection('alert_overview').aggregate([
    { $group: { _id: null, min: { $min: '$createdAt' }, max: { $max: '$createdAt' } } },
  ]).toArray();
  const minMaxUpdated = await db.collection('alert_overview').aggregate([
    { $group: { _id: null, min: { $min: '$updatedAt' }, max: { $max: '$updatedAt' } } },
  ]).toArray();
  console.log('severities:', sevs);
  console.log('statuses:', statuses);
  console.log('createdAt range:', minMaxCreated[0]);
  console.log('updatedAt range:', minMaxUpdated[0]);

  await client.close();
})().catch((e) => { console.error(e); process.exit(1); });

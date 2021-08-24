/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */

require('dotenv').config();

const { MongoClient } = require('mongodb');
const { Storage } = require('@google-cloud/storage');

const MONGO_CLIENT = new MongoClient(process.env.URI);

const STORAGE_CLIENT = new Storage();
const BUCKET = STORAGE_CLIENT.bucket('vnbi-data-lake');

// const NO_ROWS = 66514149;
const NO_ROWS = 15000;
const ROWS_PER_PART = 5000;

const main = async () => {
  await MONGO_CLIENT.connect();
  const db = MONGO_CLIENT.db('bigdata_2021');
  const col = db.collection('vnphonefull');
  const parts = [];
  for (let i = 0; i < NO_ROWS; i += ROWS_PER_PART) {
    parts.push(i);
  }
  for (const part of parts) {
    const rawDocs = await col.find().limit(ROWS_PER_PART).skip(part).toArray();
    const file = BUCKET.file(
      `data/venesa/vnphonefull2/vnphonefull.${part}.json`,
    );
    await file.save(rawDocs.map(JSON.stringify).join('\n'));
    console.log(part);
  }
  await MONGO_CLIENT.close();
};

main();

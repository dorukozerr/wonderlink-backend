import { config } from 'dotenv';

config();

import { join } from 'path';
import { homedir } from 'os';

import { BigQuery } from '@google-cloud/bigquery';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

import { users, sessions } from '../db/schema';

const BIGQUERY_PROJECT_ID = process.env.BIGQUERY_PROJECT_ID;
const BIGQUERY_CREDENTIALS_JSON = process.env.BIGQUERY_CREDENTIALS_JSON;
const POSTGRESQL_DATABASE_URL = process.env.POSTGRESQL_DATABASE_URL;

if (
  !BIGQUERY_PROJECT_ID ||
  !BIGQUERY_CREDENTIALS_JSON ||
  !POSTGRESQL_DATABASE_URL
) {
  throw new Error('Missing env vars');
}

const bq = new BigQuery({
  projectId: process.env.BIGQUERY_PROJECT_ID,
  keyFilename: join(homedir(), BIGQUERY_CREDENTIALS_JSON)
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const db = drizzle(pool);

const generate_users = async () => {
  const [tables] = await bq
    .dataset(process.env.BIGQUERY_DATASET_NAME ?? '')
    .getTables();

  const tableDetails = tables
    // When I filter by id includes event, I saw some tables with events_intraday_<date> to ignore them I moved to filtering them by length
    // .filter((table) => table.id.startsWith('events'))
    .filter((table) => table.id?.length == 15)
    .map((table) => ({
      tableId: table.id,
      type: table.metadata.type,
      creationTime: table.metadata.creationTime
    }));

  const table = bq
    .dataset(process.env.BIGQUERY_DATASET_NAME ?? '')
    .table(tableDetails[0].tableId ?? '');

  const [rows] = await table.getRows();

  console.log('test end', rows.length);
};

generate_users();

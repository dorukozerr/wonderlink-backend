import { join } from 'path';
import { homedir } from 'os';

import { BigQuery } from '@google-cloud/bigquery';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

const BIGQUERY_PROJECT_ID = process.env.BIGQUERY_PROJECT_ID;
const BIGQUERY_CREDENTIALS_JSON = process.env.BIGQUERY_CREDENTIALS_JSON;
const POSTGRESQL_DATABASE_NAME = process.env.POSTGRESQL_DATABASE_NAME;
const POSTGRESQL_DATABASE_PASSWORD = process.env.POSTGRESQL_DATABASE_PASSWORD;

if (
  !BIGQUERY_PROJECT_ID ||
  !BIGQUERY_CREDENTIALS_JSON ||
  !POSTGRESQL_DATABASE_NAME ||
  !POSTGRESQL_DATABASE_PASSWORD
) {
  throw new Error('Missing env vars detected');
}

const bq = new BigQuery({
  projectId: process.env.BIGQUERY_PROJECT_ID,
  keyFilename: join(homedir(), BIGQUERY_CREDENTIALS_JSON)
});

const pool = new Pool({
  database: POSTGRESQL_DATABASE_NAME,
  password: POSTGRESQL_DATABASE_PASSWORD
});

const db = drizzle(pool);

export { bq, db };

import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

const POSTGRESQL_DATABASE_NAME = process.env.POSTGRESQL_DATABASE_NAME;
const POSTGRESQL_DATABASE_PASSWORD = process.env.POSTGRESQL_DATABASE_PASSWORD;

if (!POSTGRESQL_DATABASE_NAME || !POSTGRESQL_DATABASE_PASSWORD) {
  throw new Error('Missing env vars detected');
}

const pool = new Pool({
  database: POSTGRESQL_DATABASE_NAME,
  password: POSTGRESQL_DATABASE_PASSWORD
});

export const db = drizzle(pool);

import { config } from 'dotenv';

config();

import { join } from 'path';
import { homedir } from 'os';
import { writeFileSync } from 'fs';

import { BigQuery } from '@google-cloud/bigquery';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

import { users, sessions } from '../src/db/schemas';

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

const ConsoleColors = {
  Reset: '\x1b[0m',
  Blue: '\x1b[34m',
  Green: '\x1b[32m',
  Yellow: '\x1b[33m',
  Red: '\x1b[31m',
  Magenta: '\x1b[35m'
} as const;

type LogType = 'info' | 'success' | 'warning' | 'error' | 'debug';

const logStyles = {
  info: ConsoleColors.Blue,
  success: ConsoleColors.Green,
  warning: ConsoleColors.Yellow,
  error: ConsoleColors.Red,
  debug: ConsoleColors.Magenta
};

let logOutput = 'Wonderlink BigQuery data processing\n';

const logOperation = (log: string, type: LogType = 'info') => {
  const timestamp = new Date().toISOString();
  const coloredLog = `${logStyles[type]}[${type.toUpperCase()}] => ${log}${ConsoleColors.Reset}`;
  const plainLog = `[${timestamp}] [${type.toUpperCase()}] ${log}\n`;

  logOutput += plainLog;

  console.log(coloredLog);
};

const process_bq_data = async () => {
  try {
    logOperation('=> process_bq_data script invoked');
    logOperation('=> starting to fetching all available tables');

    const [tables] = await bq
      .dataset(process.env.BIGQUERY_DATASET_NAME ?? '')
      .getTables();

    logOperation(
      `=> all available tables fetched, total table count ${tables.length}`,
      'success'
    );
    logOperation('=> starting to filtering tables');

    const filteredTables = tables
      .filter(
        (table) => table.id?.startsWith('events') && table.id?.length === 15
      )
      .map((table) => ({ tableId: table.id }));

    logOperation(
      `=> tables are filtered, total count of event tables ${filteredTables.length}`,
      'success'
    );

    for (const { tableId } of filteredTables.slice(0, 5)) {
      logOperation(`starting to fetching all data of table => ${tableId}`);

      if (!tableId) {
        logOperation('tableId is missing from table record', 'error');

        return;
      }

      const table = bq
        .dataset(process.env.BIGQUERY_DATASET_NAME ?? '')
        .table(tableId);

      const [rows] = await table.getRows();

      logOperation(`=> table => ${tableId} data fetched`, 'success');
      logOperation(`starting to filtering events data of table => ${tableId}`);

      const userRecords = rows
        .filter((row) => row.event_name === 'first_open')
        .map((row) => ({
          user_pseudo_id: row.user_pseudo_id,
          install_date: row.event_date,
          install_timestamp: Math.floor(
            row.user_properties.filter(
              (prop: { key: string }) => prop.key === 'first_open_time'
            )[0].value.int_value
          ),
          platform: row.platform,
          country: row.geo.country
        }));

      const sessionRecords = rows
        .filter((row) => row.event_name === 'session_start')
        .map((row) => ({
          session_id: String(
            row.event_params.filter(
              (param: { key: string }) => param.key === 'ga_session_id'
            )[0].value.int_value
          ),
          user_pseudo_id: row.user_pseudo_id,
          session_date: row.event_date,
          session_timestamp: row.event_timestamp / 1000
        }));

      logOperation(
        `table => ${tableId} data filtered, extracted user records ${userRecords.length}, extracted sessionRecords ${sessionRecords.length}`,
        'success'
      );
    }
  } catch (error) {
    logOperation(`process_bq_data error => ${error}`, 'error');
  } finally {
    writeFileSync(
      `process_bq_data_script_logs_${new Date().toISOString()}`,
      logOutput
    );
  }
};

process_bq_data();

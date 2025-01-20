import { config } from 'dotenv';

config();

import { join } from 'path';
import { homedir } from 'os';
import { createWriteStream } from 'fs';

import { eq } from 'drizzle-orm';
import { BigQuery } from '@google-cloud/bigquery';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

import { users, sessions } from '../db/schemas';

const BIGQUERY_PROJECT_ID = process.env.BIGQUERY_PROJECT_ID;
const BIGQUERY_CREDENTIALS_JSON = process.env.BIGQUERY_CREDENTIALS_JSON;
const BIGQUERY_DATASET_NAME = process.env.BIGQUERY_DATASET_NAME;
const POSTGRESQL_DATABASE_URL = process.env.POSTGRESQL_DATABASE_URL;

if (
  !BIGQUERY_PROJECT_ID ||
  !BIGQUERY_CREDENTIALS_JSON ||
  !BIGQUERY_DATASET_NAME ||
  !POSTGRESQL_DATABASE_URL
) {
  throw new Error('Missing env vars');
}

const bq = new BigQuery({
  projectId: BIGQUERY_PROJECT_ID,
  keyFilename: join(homedir(), BIGQUERY_CREDENTIALS_JSON)
});

const pool = new Pool({
  connectionString: POSTGRESQL_DATABASE_URL
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

const logFile = createWriteStream(
  `process_bq_data_script_logs_${new Date().toISOString()}.txt`,
  { flags: 'a' }
);

type LogType = 'info' | 'success' | 'warning' | 'error' | 'debug';

const logStyles = {
  info: ConsoleColors.Blue,
  success: ConsoleColors.Green,
  warning: ConsoleColors.Yellow,
  error: ConsoleColors.Red,
  debug: ConsoleColors.Magenta
};

const logProcessing = (log: string, type: LogType = 'info') => {
  const timestamp = new Date().toISOString();
  const coloredLog = `${logStyles[type]}[${type.toUpperCase()}] ${log}${ConsoleColors.Reset}`;
  const plainLog = `[${timestamp}] [${type.toUpperCase()}] ${log}\n`;

  logFile.write(plainLog);
  console.log(coloredLog);
};

const process_bq_data = async () => {
  try {
    logProcessing('=> process_bq_data script invoked');
    logProcessing('=> starting to fetching all available tables');

    const [tables] = await bq.dataset(BIGQUERY_DATASET_NAME).getTables();

    logProcessing(
      `=> all available tables fetched, total table count ${tables.length}`,
      'success'
    );
    logProcessing('=> starting to filtering tables');

    const filteredTables = tables
      .filter(
        (table) => table.id?.startsWith('events') && table.id?.length === 15
      )
      .map((table) => ({ tableId: table.id }));

    logProcessing(
      `=> tables are filtered, total count of event tables ${filteredTables.length}`,
      'success'
    );

    let loopIndex = 1;

    for (const { tableId } of filteredTables) {
      const beforeMem = process.memoryUsage().heapUsed / 1024 / 1024;
      logProcessing(
        `Memory usage before GC: ${beforeMem.toFixed(2)} MB`,
        'debug'
      );

      if (gc) gc();

      const afterMem = process.memoryUsage().heapUsed / 1024 / 1024;
      logProcessing(
        `Memory usage after GC: ${afterMem.toFixed(2)} MB`,
        'debug'
      );

      logProcessing(
        `=> starting to fetching all data of table => ${tableId} ${loopIndex}/${filteredTables.length}`
      );

      loopIndex += 1;

      if (!tableId) {
        logProcessing('=> tableId is missing from table record', 'error');

        return;
      }

      const table = bq.dataset(BIGQUERY_DATASET_NAME).table(tableId);

      const [rows] = await table.getRows({
        selectedFields:
          'event_name,user_pseudo_id,user_properties,event_date,platform,geo,event_params,event_date,event_timestamp'
      });

      logProcessing(`=> table => ${tableId} data fetched`, 'success');
      logProcessing(
        `=> starting to filtering events data of table => ${tableId}`
      );

      const userRecords = rows
        .filter((row) => row.event_name === 'first_open')
        .map((row) => ({
          user_pseudo_id: row.user_pseudo_id,
          install_date: row.event_date,
          install_timestamp: row.user_properties.filter(
            (prop: { key: string }) => prop.key === 'first_open_time'
          )[0].value.int_value,
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
          session_timestamp: row.event_timestamp,
          country: row.geo.country,
          platform: row.platform
        }));

      logProcessing(
        `=> table => ${tableId} data filtered, extracted user records ${userRecords.length}, extracted sessionRecords ${sessionRecords.length}`,
        'success'
      );
      logProcessing('=> starting db operations on filtered user records');

      for (const user of userRecords) {
        const matchedUserRecorcds = await db
          .select()
          .from(users)
          .where(eq(users.user_pseudo_id, user.user_pseudo_id));

        if (matchedUserRecorcds.length === 0) {
          logProcessing(
            `=> starting the insert operation for user - ${user.user_pseudo_id}`
          );

          await db.insert(users).values(user);

          logProcessing(
            `=> user - ${user.user_pseudo_id} inserted to database`,
            'success'
          );
        } else {
          logProcessing(
            `=> user - ${user.user_pseudo_id} exists in database, moving on to next user`,
            'error'
          );
        }
      }

      for (const session of sessionRecords) {
        const matchedSessionRecord = await db
          .select()
          .from(sessions)
          .where(eq(sessions.session_id, session.session_id));

        const userRecord = await db
          .select()
          .from(users)
          .where(eq(users.user_pseudo_id, session.user_pseudo_id));

        if (userRecord.length === 0) {
          logProcessing(
            `=> user - ${session.user_pseudo_id} does not exists in users table but has a session record skipping the insert operation for this session record`,
            'error'
          );
        } else {
          if (matchedSessionRecord.length === 0) {
            logProcessing(
              `=> inserting session - ${session.session_id} into database`
            );

            const sessionInsertData = {
              session_id: session.session_id,
              user_pseudo_id: session.user_pseudo_id,
              session_date: session.session_date,
              session_timestamp: session.session_timestamp
            };

            await db.insert(sessions).values(sessionInsertData);

            logProcessing(
              `=> session - ${session.session_id} inserted succesfully`,
              'success'
            );
            logProcessing('checking for data mismatch in user record');

            if (userRecord.length > 0) {
              const user = userRecord[0];
              const updates: { platform?: string; country?: string } = {};

              if (session.platform !== user.platform) {
                logProcessing(
                  `=> platform mismatch detected for user ${session.user_pseudo_id}. Old: ${user.platform}, New: ${session.platform}`,
                  'warning'
                );
                updates.platform = session.platform;
              }

              if (session.country !== user.country) {
                logProcessing(
                  `=> country mismatch detected for user ${session.user_pseudo_id}. Old: ${user.country}, New: ${session.country}`,
                  'warning'
                );
                updates.country = session.country;
              }

              if (Object.keys(updates).length > 0) {
                await db
                  .update(users)
                  .set(updates)
                  .where(eq(users.user_pseudo_id, session.user_pseudo_id));

                logProcessing(
                  `=> user ${session.user_pseudo_id} record updated with new platform/country`,
                  'success'
                );
              } else {
                logProcessing(
                  `=> user ${session.user_pseudo_id} has no mismatch`,
                  'success'
                );
              }
            } else {
              logProcessing(
                `=> user ${session.user_pseudo_id} not found in database, skipping the insert operation of this session`,
                'error'
              );
            }
          } else {
            // This should never happen but added this check in any case, might add a
            // logic to generate a new session id and insert with it later
            logProcessing(
              `=> session - ${session.session_id} exists in database`,
              'error'
            );
          }
        }

        logProcessing(
          `user records - ${userRecords.length}, session records - ${sessionRecords.length}`,
          'debug'
        );
      }
    }
  } catch (error) {
    logProcessing(`=> process_bq_data error => ${error}`, 'error');
  } finally {
    logFile.end();
  }
};

process_bq_data();

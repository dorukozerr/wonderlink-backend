import { config } from 'dotenv';

config();

import { join } from 'path';
import { homedir } from 'os';
import { createWriteStream } from 'fs';

import { eq } from 'drizzle-orm';
import { BigQuery, Table } from '@google-cloud/bigquery';
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

    let loopIndex = 0;

    const processTableData = async (table: Table, tableId: string) => {
      let userPageToken: string | undefined;
      let processedRows = 0;
      const maxResults = 1000 * 1000;
      const sessionEvents = [];

      do {
        const [rows, metadata] = await table.getRows({
          pageToken: userPageToken,
          maxResults
        });

        processedRows += rows.length;

        const userRecords = rows
          .filter((row) => row.event_name === 'first_open')
          .map((row) => ({
            user_pseudo_id: row?.user_pseudo_id,
            install_date: row?.event_date,
            install_timestamp: row?.user_properties
              ? row.user_properties.filter(
                  (prop: { key: string }) => prop.key === 'first_open_time'
                )[0]?.value
                ? row.user_properties.filter(
                    (prop: { key: string }) => prop.key === 'first_open_time'
                  )[0].value?.int_value
                : null
              : null,
            platform: row?.platform,
            country: row?.geo?.country
          }));

        sessionEvents.push(
          ...rows
            .filter((row) => row.event_name === 'session_start')
            .map((row) => ({
              session_id: row?.event_params
                ? String(
                    row.event_params.filter(
                      (param: { key: string }) => param.key === 'ga_session_id'
                    )[0]?.value
                      ? row.event_params.filter(
                          (param: { key: string }) =>
                            param.key === 'ga_session_id'
                        )[0]?.value?.int_value
                      : null
                  )
                : null,
              user_pseudo_id: row?.user_pseudo_id,
              session_date: row?.event_date,
              session_timestamp: row?.event_timestamp,
              country: row?.geo?.country,
              platform: row?.platform
            }))
        );

        if (userRecords.length > 0) {
          for (const user of userRecords) {
            if (!user.install_timestamp) {
              logProcessing(
                'install timestamp is null for this user reecord moving on',
                'error'
              );
              continue;
            }

            const matchedUserRecords = await db
              .select()
              .from(users)
              .where(eq(users.user_pseudo_id, user.user_pseudo_id));

            if (matchedUserRecords.length === 0) {
              try {
                await db.insert(users).values(user);
              } catch (error) {
                logProcessing(
                  `=> user - ${user.user_pseudo_id} could not insert into database, error - ${error}`,
                  'error'
                );
              }

              logProcessing(
                `=> user - ${user.user_pseudo_id} inserted to database, table - ${loopIndex}/${filteredTables.slice(72, filteredTables.length).length}`,
                'success'
              );
            } else {
              logProcessing(
                `=> user - ${user.user_pseudo_id} exists in database, moving on to next user`,
                'error'
              );
            }
          }
        }

        userPageToken = metadata
          ? metadata.pageToken
            ? metadata?.pageToken
            : undefined
          : undefined;
      } while (userPageToken);

      if (sessionEvents.length > 0) {
        for (const session of sessionEvents) {
          if (!session.session_id) {
            logProcessing(
              'session id is null for this session reecord moving on',
              'error'
            );
            continue;
          }

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
              `=> user - ${session.user_pseudo_id} does not exist in users table but has a session record skipping the insert operation for this session record`,
              'error'
            );
            continue;
          }

          if (matchedSessionRecord.length === 0) {
            const sessionInsertData = {
              session_id: session.session_id,
              user_pseudo_id: session.user_pseudo_id,
              session_date: session.session_date,
              session_timestamp: session.session_timestamp
            };

            try {
              await db.insert(sessions).values(sessionInsertData);
            } catch (error) {
              logProcessing(
                `=> user - ${session.session_id} could not insert into database, error - ${error}`,
                'error'
              );
            }

            logProcessing(
              `=> session - ${session.session_id} inserted successfully, table ${loopIndex}/${filteredTables.slice(72, filteredTables.length).length}`,
              'success'
            );

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
              try {
                await db
                  .update(users)
                  .set(updates)
                  .where(eq(users.user_pseudo_id, session.user_pseudo_id));
              } catch (error) {
                logProcessing(
                  `=> user - ${session.user_pseudo_id} could not updated, error - ${error}`,
                  'error'
                );
              }

              logProcessing(
                `=> user ${session.user_pseudo_id} record updated with new platform/country`,
                'success'
              );
            }
          } else {
            logProcessing(
              `=> session - ${session.session_id} exists in database, table - ${loopIndex}/${filteredTables.slice(72, filteredTables.length).length}`,
              'error'
            );
          }
        }
      }

      logProcessing(
        `=> table - ${tableId}, total rows processed - ${processedRows.toLocaleString('tr-TR')}`,
        'debug'
      );
    };

    for (const { tableId } of filteredTables.slice(72, filteredTables.length)) {
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

      loopIndex += 1;

      logProcessing(
        `=> starting to process table - ${tableId} - ${loopIndex}/${filteredTables.slice(72, filteredTables.length).length}`
      );

      if (!tableId) {
        logProcessing('=> tableId is missing from table record', 'error');
        continue;
      }

      const tableProcessingStart = Date.now();

      const table = bq.dataset(BIGQUERY_DATASET_NAME).table(tableId);

      await processTableData(table, tableId);

      const tableProcessingEnd = Date.now() - tableProcessingStart;

      logProcessing(
        `=> ${tableId} - Time: ${Math.floor(tableProcessingEnd / 60000)}m ${((tableProcessingEnd % 60000) / 1000).toFixed(1)}s`,
        'debug'
      );
    }

    logProcessing('Sucessfully processed all data', 'success');
  } catch (error) {
    logProcessing(`=> process_bq_data error => ${error}`, 'error');
  } finally {
    logFile.end();
  }
};

process_bq_data();

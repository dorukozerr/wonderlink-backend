import { z } from 'zod';

import { router, publicProcedure } from '../trpc';
import { bq } from '../db';

export const helloRouter = router({
  greeting: publicProcedure
    .input(
      z.object({
        name: z.string().optional()
      })
    )
    .query(async ({ input }) => ({
      greeting: `Hello ${input?.name ?? 'world'}!`
    })),
  test: publicProcedure.query(async () => {
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
        session_id: row.event_params.filter(
          (param: { key: string }) => param.key === 'ga_session_id'
        )[0].value.int_value,
        user_pseudo_id: row.user_pseudo_id,
        session_date: row.event_date,
        session_timestamp: row.event_timestamp / 1000
      }));

    return { userRecords, sessionRecords };
  })
});

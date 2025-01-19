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

    return {
      tableDetails
    };
  })
});

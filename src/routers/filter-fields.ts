import { sql } from 'drizzle-orm';

import { router, publicProcedure } from '../trpc';
import { users, sessions } from '../db/schemas';
import { db } from '../db';

export const filterFieldsRouter = router({
  getUniqueFilters: publicProcedure.query(async () => {
    const platformResult = await db.execute(
      sql`SELECT DISTINCT platform FROM ${users} ORDER BY platform ASC`
    );

    const countryResult = await db.execute(
      sql`SELECT DISTINCT country FROM ${users} ORDER BY country ASC`
    );

    return {
      platforms: platformResult.rows.map((row) => row.platform),
      countries: countryResult.rows.map((row) => row.country)
    } as { platforms: string[]; countries: string[] };
  }),
  getSessionDateRange: publicProcedure.query(async () => {
    const result = await db.execute(
      sql`SELECT
        MIN(session_date) as earliest_date,
        MAX(session_date) as latest_date
        FROM ${sessions}`
    );

    return {
      earliestDate: result.rows[0].earliest_date,
      latestDate: result.rows[0].latest_date
    } as { earliestDate: Date; latestDate: Date };
  })
});

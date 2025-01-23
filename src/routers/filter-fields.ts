import { sql } from 'drizzle-orm';

import { router, publicProcedure } from '../trpc';
import { users } from '../db/schemas';
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
  })
});

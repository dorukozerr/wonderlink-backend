import { z } from 'zod';
import { sql } from 'drizzle-orm';

import { router, publicProcedure } from '../trpc';
import { users, sessions } from '../db/schemas';
import { db } from '../db';

export const analyticsRouter = router({
  retentionMetrics: publicProcedure
    .input(
      z.object({
        date: z.object({
          from: z.string().datetime().optional(),
          to: z.string().datetime().optional()
        }),
        platforms: z.array(z.string()),
        countries: z.array(z.string())
      })
    )
    .query(async ({ input: { date, platforms, countries } }) => {
      const dateFilter = sql.empty();

      if (date.from) {
        dateFilter.append(sql` AND u.install_date >= ${date.from}`);
      }

      if (date.to) {
        dateFilter.append(sql` AND u.install_date <= ${date.to}`);
      }

      const platformArray =
        platforms.length > 0
          ? sql`ARRAY[${sql.join(
              platforms.map((p) => sql`${p}`),
              sql`, `
            )}]`
          : null;

      const countryArray =
        countries.length > 0
          ? sql`ARRAY[${sql.join(
              countries.map((c) => sql`${c}`),
              sql`, `
            )}]`
          : null;

      const platformFilter = platformArray
        ? sql` AND u.platform = ANY(${platformArray})`
        : sql.empty();

      const countryFilter = countryArray
        ? sql` AND u.country = ANY(${countryArray})`
        : sql.empty();

      const result = await db.execute(sql`
        WITH base_users AS (
          SELECT
            user_pseudo_id,
            install_timestamp
          FROM ${users} u
          WHERE 1=1 ${dateFilter} ${platformFilter} ${countryFilter}
        ),
        user_retention AS (
          SELECT
            (SELECT COUNT(*) FROM base_users) as total_users,
            COUNT(DISTINCT CASE
              WHEN ((s.session_timestamp::bigint / 1000) - bu.install_timestamp::bigint) / (3600 * 1000)
                BETWEEN 24 AND 48
              THEN bu.user_pseudo_id END) as d1_retention,
            COUNT(DISTINCT CASE
              WHEN ((s.session_timestamp::bigint / 1000) - bu.install_timestamp::bigint) / (3600 * 1000)
                BETWEEN 168 AND 192
              THEN bu.user_pseudo_id END) as d7_retention,
            COUNT(DISTINCT CASE
              WHEN ((s.session_timestamp::bigint / 1000) - bu.install_timestamp::bigint) / (3600 * 1000)
                BETWEEN 336 AND 360
              THEN bu.user_pseudo_id END) as d14_retention,
            COUNT(DISTINCT CASE
              WHEN ((s.session_timestamp::bigint / 1000) - bu.install_timestamp::bigint) / (3600 * 1000)
                BETWEEN 504 AND 528
              THEN bu.user_pseudo_id END) as d21_retention,
            COUNT(DISTINCT CASE
              WHEN ((s.session_timestamp::bigint / 1000) - bu.install_timestamp::bigint) / (3600 * 1000)
                BETWEEN 720 AND 744
              THEN bu.user_pseudo_id END) as d30_retention
          FROM base_users bu
          LEFT JOIN ${sessions} s ON bu.user_pseudo_id = s.user_pseudo_id
          AND (s.session_timestamp::bigint / 1000) > bu.install_timestamp::bigint
        )
        SELECT json_build_array(
          json_build_object(
            'id', 'total-users',
            'value', total_users,
            'label', 'Total Users'
          ),
          json_build_object(
            'id', 'd1-retention',
            'value', d1_retention,
            'label', 'Day 1 Retention',
            'ratio', ROUND(CAST((d1_retention::float / NULLIF(total_users, 0)) * 100 AS numeric), 2)
          ),
          json_build_object(
            'id', 'd7-retention',
            'value', d7_retention,
            'label', 'Day 7 Retention',
            'ratio', ROUND(CAST((d7_retention::float / NULLIF(total_users, 0)) * 100 AS numeric), 2)
          ),
          json_build_object(
            'id', 'd14-retention',
            'value', d14_retention,
            'label', 'Day 14 Retention',
            'ratio', ROUND(CAST((d14_retention::float / NULLIF(total_users, 0)) * 100 AS numeric), 2)
          ),
          json_build_object(
            'id', 'd21-retention',
            'value', d21_retention,
            'label', 'Day 21 Retention',
            'ratio', ROUND(CAST((d21_retention::float / NULLIF(total_users, 0)) * 100 AS numeric), 2)
          ),
          json_build_object(
            'id', 'd30-retention',
            'value', d30_retention,
            'label', 'Day 30 Retention',
            'ratio', ROUND(CAST((d30_retention::float / NULLIF(total_users, 0)) * 100 AS numeric), 2)
          )
        ) as data
        FROM user_retention
      `);

      return result.rows[0].data as {
        id: string;
        value: number;
        label: string;
        ratio?: number;
      }[];
    })
});

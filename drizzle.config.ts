import type { Config } from 'drizzle-kit';

export default {
  schema: './src/db/schemas.ts',
  out: './drizzle',
  dialect: 'postgresql'
} satisfies Config;

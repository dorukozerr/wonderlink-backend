import { pgTable, varchar, date, bigint } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  user_pseudo_id: varchar('user_pseudo_id').primaryKey(),
  install_date: date('install_date').notNull(),
  install_timestamp: bigint('install_timestamp', { mode: 'bigint' }).notNull(),
  platform: varchar('platform').notNull(),
  country: varchar('country').notNull()
});

export const sessions = pgTable('sessions', {
  session_id: varchar('session_id').primaryKey(),
  user_pseudo_id: varchar('user_pseudo_id')
    .notNull()
    .references(() => users.user_pseudo_id),
  session_date: date('session_date').notNull(),
  session_timestamp: bigint('session_timestamp', { mode: 'bigint' }).notNull()
});

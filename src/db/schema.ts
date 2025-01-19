import { pgTable, varchar, date, integer } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  userPseudoId: varchar('user_pseudo_id').primaryKey(),
  installDate: date('install_date').notNull(),
  installTimestamp: integer('install_timestamp').notNull(),
  platform: varchar('platform').notNull(),
  country: varchar('country').notNull()
});

export const sessions = pgTable('sessions', {
  sessionId: varchar('session_id').primaryKey(),
  userPseudoId: varchar('user_pseudo_id')
    .notNull()
    .references(() => users.userPseudoId),
  sessionDate: date('session_date').notNull(),
  sessionTimestamp: integer('session_timestamp').notNull()
});

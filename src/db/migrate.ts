import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  database: 'wonderlink',
  user: process.env.USER
});

const main = async () => {
  const db = drizzle(pool);

  console.log('Running migrations...');

  await migrate(db, { migrationsFolder: 'drizzle' });

  console.log('Migrations complete!');

  process.exit(0);
};

main().catch((err) => {
  console.error('Migration failed!');
  console.error(err);

  process.exit(1);
});

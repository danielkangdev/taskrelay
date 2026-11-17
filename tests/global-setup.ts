import { migrate } from '../src/db/migrate.js';
import { createPool } from '../src/db/pool.js';
import { seedOrders } from '../src/db/seed.js';
import { TEST_DATABASE_URL } from './env.js';

/** Rebuild the test database from the migrations once per `vitest run`. */
export default async function setup(): Promise<void> {
  const db = createPool(TEST_DATABASE_URL, 2);
  try {
    await db.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
    await migrate(db);
    await seedOrders(db, { tenants: 20, months: 12, ordersPerTenant: 120 });
  } finally {
    await db.end();
  }
}

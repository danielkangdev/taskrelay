import type { Queryable } from './pool.js';

export const SEED = {
  tenants: 1_000,
  months: 12, // 2025-01 .. 2025-12
  ordersPerTenant: 300, // 300k orders total
};

/** Deterministic order history for report jobs to aggregate. Idempotent. */
export async function seedOrders(db: Queryable, opts = SEED): Promise<number> {
  const { rows } = await db.query<{ n: string }>('SELECT count(*) AS n FROM orders');
  if (Number(rows[0]!.n) > 0) return 0;
  await db.query('SELECT setseed(0.42)');
  const res = await db.query(
    `INSERT INTO orders (tenant_id, amount, status, created_at)
     SELECT t,
            round((5 + random() * 495)::numeric, 2),
            (ARRAY['paid','paid','paid','paid','refunded','pending'])[1 + floor(random() * 6)::int],
            timestamptz '2025-01-01' + random() * ($3 * interval '1 month')
     FROM generate_series(1, $1) AS t, generate_series(1, $2) AS o`,
    [opts.tenants, opts.ordersPerTenant, opts.months],
  );
  await db.query('ANALYZE orders');
  return res.rowCount ?? 0;
}

/** All (tenant, period) pairs, for enqueueing report jobs. */
export function reportPeriods(opts = SEED): string[] {
  return Array.from({ length: opts.months }, (_, i) => `2025-${String(i + 1).padStart(2, '0')}`);
}

import { z } from 'zod';
import { type Handler, sleep } from './types.js';

export const ReportPayload = z.object({
  tenant_id: z.number().int().positive(),
  period: z.string().regex(/^\d{4}-(0[1-9]|1[0-2])$/, 'period must be YYYY-MM'),
  /** Business idempotency key; defaults to "<tenant>:<period>". */
  report_key: z.string().min(1).optional(),
  /** Extra simulated work (ms) before the write, used by the chaos experiment. */
  simulate_ms: z.number().int().nonnegative().max(60_000).optional(),
});
export type ReportPayload = z.infer<typeof ReportPayload>;

/**
 * Monthly report for one tenant: aggregate `orders`, write one `reports` row.
 *
 * The write is `INSERT ... ON CONFLICT (report_key) DO NOTHING` in its own
 * transaction, and the worker marks the job succeeded in a *separate*
 * transaction afterwards. A crash between the two is therefore possible (and
 * tested): the job is re-run after its lease expires, the aggregate is
 * recomputed, and the second INSERT is a no-op, so `reports` still has exactly
 * one row per report_key.
 */
export const reportHandler: Handler = async ({ job, db, workerId, signal, fault }) => {
  const p = ReportPayload.parse(job.payload);
  const key = p.report_key ?? `${p.tenant_id}:${p.period}`;

  const { rows } = await db.query<{ orders: string; revenue: string | null; refunded: string | null }>(
    `SELECT count(*) AS orders,
            sum(amount) FILTER (WHERE status = 'paid')     AS revenue,
            sum(amount) FILTER (WHERE status = 'refunded') AS refunded
     FROM orders
     WHERE tenant_id = $1
       AND created_at >= to_date($2, 'YYYY-MM')
       AND created_at <  to_date($2, 'YYYY-MM') + interval '1 month'`,
    [p.tenant_id, p.period],
  );
  if (p.simulate_ms) await sleep(p.simulate_ms, signal);

  const agg = rows[0]!;
  await db.query(
    `INSERT INTO reports (job_id, report_key, tenant_id, period, payload)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (report_key) DO NOTHING`,
    [
      job.id,
      key,
      p.tenant_id,
      p.period,
      JSON.stringify({
        orders: Number(agg.orders),
        revenue: Number(agg.revenue ?? 0),
        refunded: Number(agg.refunded ?? 0),
        generated_by: workerId,
        lease_token: job.lease_token,
        attempt: job.attempts,
      }),
    ],
  );
  fault('crash_after_write');
};

import type { Queryable } from '../db/pool.js';
import { leasesExpired } from '../metrics.js';
import { notifyChannel } from './types.js';

export interface ReapResult {
  requeued: number;
  dead: number;
}

/**
 * Recover jobs whose worker stopped heartbeating (crashed, hung, partitioned).
 * Jobs with attempts left go straight back to `queued` (a crash is not evidence
 * the job is bad, so no backoff); jobs out of attempts become `dead`.
 *
 * SKIP LOCKED lets every worker process run a reaper without contention. A
 * concurrent heartbeat either commits first (lease extended, so the row no longer
 * matches when re-checked) or waits and then finds the row requeued (0 rows,
 * lease lost). Expiration handling modelled on pg-boss's maintenance loop.
 */
export async function reapExpired(db: Queryable, batch = 1000): Promise<ReapResult> {
  const res = await db.query<{ status: string; queue: string }>(
    `WITH expired AS (
       SELECT id FROM jobs
       WHERE status = 'running' AND lease_expires_at < now()
       ORDER BY lease_expires_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE jobs j SET
       status      = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
       run_at      = now(),
       finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
       locked_by   = CASE WHEN j.attempts >= j.max_attempts THEN j.locked_by END,
       lease_expires_at = NULL,
       last_error  = 'lease expired (held by ' || coalesce(j.locked_by, '?') || ')'
     FROM expired WHERE j.id = expired.id
     RETURNING j.status, j.queue`,
    [batch],
  );
  const out: ReapResult = { requeued: 0, dead: 0 };
  const queues = new Set<string>();
  for (const r of res.rows) {
    if (r.status === 'dead') out.dead++;
    else {
      out.requeued++;
      queues.add(r.queue);
    }
  }
  if (out.requeued) leasesExpired.inc({ status: 'queued' }, out.requeued);
  if (out.dead) leasesExpired.inc({ status: 'dead' }, out.dead);
  for (const q of queues) await db.query('SELECT pg_notify($1, $2)', [notifyChannel(q), '']);
  return out;
}

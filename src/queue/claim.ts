import type { Queryable } from '../db/pool.js';
import type { ClaimStrategy } from '../config.js';
import type { ClaimedJob } from './types.js';

export interface ClaimOptions {
  queue: string;
  workerId: string;
  leaseMs: number;
  limit?: number;
  strategy?: ClaimStrategy;
}

// Columns set when a worker takes a job. A fresh lease_token per claim is the
// fencing token every later write from this worker must present.
const take = (worker: string, leaseMs: string) => `
  status = 'running',
  attempts = j.attempts + 1,
  locked_by = ${worker},
  lease_token = gen_random_uuid(),
  lease_expires_at = now() + make_interval(secs => ${leaseMs}::float8 / 1000),
  started_at = now()`;

const CANDIDATES = `
  SELECT id FROM jobs
  WHERE queue = $1 AND status = 'queued' AND run_at <= now()
  ORDER BY priority DESC, run_at, id
  LIMIT $2`;

/**
 * Claim up to `limit` runnable jobs. Three strategies exist so experiment E2 can
 * compare them on identical load; production code only uses `skip_locked`.
 *
 * - skip_locked: one statement; rows locked by a concurrent claimer are skipped,
 *   so N workers take N different jobs without waiting on each other.
 *   (Same shape as graphile-worker's get_job / pg-boss's fetchNextJob:
 *   SELECT ... FOR UPDATE SKIP LOCKED inside a CTE feeding an UPDATE.)
 * - for_update: identical but without SKIP LOCKED. Every claimer targets the
 *   same head-of-queue row and queues behind the row lock.
 * - naive: SELECT, then a separate UPDATE by id with no lock and no status
 *   re-check. Two workers can read the same id and both "claim" it.
 */
export async function claim(db: Queryable, opts: ClaimOptions): Promise<ClaimedJob[]> {
  const limit = opts.limit ?? 1;
  const params = [opts.queue, limit, opts.workerId, opts.leaseMs];
  const strategy = opts.strategy ?? 'skip_locked';
  let rows: ClaimedJob[];

  if (strategy === 'naive') {
    const ids = (await db.query<{ id: string }>(CANDIDATES, [opts.queue, limit])).rows.map((r) => r.id);
    if (ids.length === 0) return [];
    rows = (
      await db.query<ClaimedJob>(`UPDATE jobs j SET ${take('$2', '$3')} WHERE j.id = ANY($1::uuid[]) RETURNING j.*`, [
        ids,
        opts.workerId,
        opts.leaseMs,
      ])
    ).rows;
  } else {
    const lock = strategy === 'skip_locked' ? 'FOR UPDATE SKIP LOCKED' : 'FOR UPDATE';
    rows = (
      await db.query<ClaimedJob>(
        `WITH next AS (${CANDIDATES} ${lock})
         UPDATE jobs j SET ${take('$3', '$4')}
         FROM next WHERE j.id = next.id
         RETURNING j.*`,
        params,
      )
    ).rows;
  }
  // UPDATE ... RETURNING does not preserve the CTE's order.
  return rows.sort((a, b) => b.priority - a.priority || +a.run_at - +b.run_at || a.id.localeCompare(b.id));
}

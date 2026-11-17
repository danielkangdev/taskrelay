import type { Queryable } from '../db/pool.js';
import { jobsFinished, staleCompletions } from '../metrics.js';
import { type BackoffOptions, backoffMs } from './backoff.js';
import { type ClaimedJob, type Job, notifyChannel } from './types.js';

/*
 * Every state change a worker makes is fenced by `id = $1 AND lease_token = $2
 * AND status = 'running'`. If the lease expired and someone else claimed the job,
 * the token no longer matches: the UPDATE touches 0 rows and the write is counted
 * as a stale completion instead of overwriting the new owner's state.
 */
const FENCE = `id = $1 AND lease_token = $2 AND status = 'running'`;

function stale(job: ClaimedJob, op: string): void {
  staleCompletions.inc({ queue: job.queue, op });
  console.warn(`[taskrelay] stale ${op} rejected: job=${job.id} worker=${job.locked_by}`);
}

/** Mark a held job succeeded. Returns false if the lease was lost (stale completion). */
export async function completeJob(db: Queryable, job: ClaimedJob): Promise<boolean> {
  const res = await db.query(
    `UPDATE jobs SET status = 'succeeded', finished_at = now(), lease_expires_at = NULL, last_error = NULL
     WHERE ${FENCE}`,
    [job.id, job.lease_token],
  );
  if (res.rowCount === 0) {
    stale(job, 'complete');
    return false;
  }
  jobsFinished.inc({ queue: job.queue, type: job.type, outcome: 'succeeded' });
  return true;
}

export type FailOutcome = 'retried' | 'dead' | 'stale';

/**
 * Record a failed attempt. With attempts left the job goes back to `queued`
 * with run_at pushed out by exponential backoff; otherwise it becomes `dead`.
 */
export async function failJob(
  db: Queryable,
  job: ClaimedJob,
  error: unknown,
  backoff: BackoffOptions,
): Promise<FailOutcome> {
  const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  const res = await db.query<{ status: string }>(
    `UPDATE jobs SET
       status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
       run_at      = CASE WHEN attempts >= max_attempts THEN run_at
                          ELSE now() + make_interval(secs => $3::float8 / 1000) END,
       finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
       locked_by   = CASE WHEN attempts >= max_attempts THEN locked_by END,
       lease_expires_at = NULL,
       last_error  = $4
     WHERE ${FENCE}
     RETURNING status`,
    [job.id, job.lease_token, backoffMs(job.attempts, backoff), message.slice(0, 2000)],
  );
  const row = res.rows[0];
  if (!row) {
    stale(job, 'fail');
    return 'stale';
  }
  const outcome = row.status === 'dead' ? 'dead' : 'retried';
  jobsFinished.inc({ queue: job.queue, type: job.type, outcome });
  return outcome;
}

/** Extend the lease of held jobs. Returns the ids we still own; any others were lost. */
export async function heartbeat(db: Queryable, jobs: ClaimedJob[], leaseMs: number): Promise<Set<string>> {
  if (jobs.length === 0) return new Set();
  const res = await db.query<{ id: string }>(
    `UPDATE jobs j SET lease_expires_at = now() + make_interval(secs => $3::float8 / 1000)
     FROM unnest($1::uuid[], $2::uuid[]) AS h(id, token)
     WHERE j.id = h.id AND j.lease_token = h.token AND j.status = 'running'
     RETURNING j.id`,
    [jobs.map((j) => j.id), jobs.map((j) => j.lease_token), leaseMs],
  );
  return new Set(res.rows.map((r) => r.id));
}

/** Count a heartbeat that found the lease gone (exported for the worker). */
export function staleHeartbeat(job: ClaimedJob): void {
  stale(job, 'heartbeat');
}

/**
 * Give a held job back without counting it as a failure (graceful shutdown that
 * ran out of time, or batched jobs never started). `attempts` is not decremented:
 * it counts claims, which keeps a job that always hangs from cycling forever.
 */
export async function releaseJob(db: Queryable, job: ClaimedJob): Promise<boolean> {
  const res = await db.query(
    `WITH r AS (
       UPDATE jobs SET status = 'queued', run_at = now(), locked_by = NULL, lease_expires_at = NULL
       WHERE ${FENCE} RETURNING queue
     ) SELECT pg_notify($3, '') FROM r`,
    [job.id, job.lease_token, notifyChannel(job.queue)],
  );
  if (res.rowCount === 0) stale(job, 'release');
  return (res.rowCount ?? 0) > 0;
}

/** Dead-letter queue, most recently dead first. */
export async function listDead(db: Queryable, queue: string | undefined, limit: number): Promise<Job[]> {
  const res = await db.query<Job>(
    `SELECT * FROM jobs WHERE status = 'dead' AND ($1::text IS NULL OR queue = $1)
     ORDER BY finished_at DESC NULLS LAST, id LIMIT $2`,
    [queue ?? null, limit],
  );
  return res.rows;
}

/** Manually retry a dead job: back to `queued` with a fresh attempt budget. */
export async function retryDead(db: Queryable, id: string): Promise<Job | undefined> {
  const res = await db.query<Job>(
    `WITH r AS (
       UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL,
                       locked_by = NULL, lease_expires_at = NULL
       WHERE id = $1 AND status = 'dead'
       RETURNING *
     ), n AS (SELECT pg_notify('jobs_' || queue, '') FROM r)
     SELECT r.* FROM r LEFT JOIN n ON true`,
    [id],
  );
  return res.rows[0];
}

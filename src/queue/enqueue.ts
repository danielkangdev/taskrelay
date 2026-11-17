import type { Queryable } from '../db/pool.js';
import { type Job, notifyChannel } from './types.js';

export interface EnqueueInput {
  queue?: string;
  type: string;
  payload?: Record<string, unknown>;
  priority?: number;
  maxAttempts?: number;
  /** Delay before the job becomes claimable. */
  delayMs?: number;
  idempotencyKey?: string;
}

export interface EnqueueResult {
  job: Job;
  /** false when an existing job with the same (queue, idempotency key) was returned. */
  created: boolean;
}

/**
 * Insert a job and wake listening workers in the same statement.
 *
 * pg_notify is transactional: listeners only hear about the job once the
 * INSERT commits, so a worker can never be woken for a row it cannot see yet.
 * With an idempotency key, a conflicting insert does nothing and the existing
 * row is returned instead (ON CONFLICT waits for a concurrent inserter to
 * commit, so the follow-up SELECT always finds it).
 */
export async function enqueue(db: Queryable, input: EnqueueInput): Promise<EnqueueResult> {
  const queue = input.queue ?? 'default';
  const inserted = await db.query<Job>(
    `WITH ins AS (
       INSERT INTO jobs (queue, type, payload, priority, max_attempts, run_at, idempotency_key)
       VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6::float8 / 1000), $7)
       ON CONFLICT (queue, idempotency_key) DO NOTHING
       RETURNING *
     ), notified AS (
       SELECT pg_notify($8, '') FROM ins WHERE ins.run_at <= now()
     )
     SELECT ins.* FROM ins LEFT JOIN notified ON true`,
    [
      queue,
      input.type,
      JSON.stringify(input.payload ?? {}),
      input.priority ?? 0,
      input.maxAttempts ?? 5,
      input.delayMs ?? 0,
      input.idempotencyKey ?? null,
      notifyChannel(queue),
    ],
  );
  if (inserted.rows[0]) return { job: inserted.rows[0], created: true };

  const existing = await db.query<Job>('SELECT * FROM jobs WHERE queue = $1 AND idempotency_key = $2', [
    queue,
    input.idempotencyKey,
  ]);
  if (!existing.rows[0]) throw new Error('idempotent enqueue conflict but no existing row found');
  return { job: existing.rows[0], created: false };
}

/** Bulk insert for experiments and the demo: `count` jobs in one statement, one NOTIFY. */
export async function enqueueMany(
  db: Queryable,
  queue: string,
  type: string,
  count: number,
  /** Trusted SQL expression (may reference g.i), never user input. */
  payloadSql = `'{}'::jsonb`,
): Promise<number> {
  const res = await db.query(
    `INSERT INTO jobs (queue, type, payload)
     SELECT $1, $2, ${payloadSql} FROM generate_series(1, $3) AS g(i)`,
    [queue, type, count],
  );
  await db.query('SELECT pg_notify($1, $2)', [notifyChannel(queue), '']);
  return res.rowCount ?? 0;
}

import { afterAll, afterEach, beforeEach } from 'vitest';
import { type WorkerConfig, defaultWorkerConfig } from '../src/config.js';
import { type Db, createPool } from '../src/db/pool.js';
import { staleCompletions } from '../src/metrics.js';
import type { Job } from '../src/queue/types.js';
import { defaultHandlers } from '../src/worker/handlers/index.js';
import type { HandlerRegistry } from '../src/worker/handlers/types.js';
import { type PoolOptions, WorkerPool } from '../src/worker/pool.js';

import { TEST_DATABASE_URL } from './env.js';

/**
 * Per-file setup: a connection pool, clean job tables before every test, and
 * automatic shutdown of any worker pools a test started.
 */
export function useDb() {
  const db: Db = createPool(TEST_DATABASE_URL, 30);
  const pools: WorkerPool[] = [];

  beforeEach(async () => {
    await db.query('TRUNCATE jobs, job_runs, reports RESTART IDENTITY');
    delete process.env.TASKRELAY_FAULTS;
  });
  afterEach(async () => {
    delete process.env.TASKRELAY_FAULTS;
    await Promise.all(pools.splice(0).map((p) => p.stop()));
  });
  afterAll(async () => {
    await db.end();
  });

  return {
    db,
    /** Start a worker pool with fast test timings; stopped automatically after the test. */
    async startPool(
      cfg: Partial<WorkerConfig> = {},
      opts: PoolOptions = {},
      handlers: HandlerRegistry = defaultHandlers,
    ): Promise<WorkerPool> {
      const pool = new WorkerPool(
        db,
        defaultWorkerConfig({
          concurrency: 2,
          leaseMs: 5_000,
          heartbeatMs: 1_000,
          pollIntervalMs: 100,
          reaperIntervalMs: 100,
          shutdownTimeoutMs: 5_000,
          backoff: { baseMs: 50, capMs: 1_000 },
          ...cfg,
        }),
        handlers,
        { crashMode: 'stop', ...opts },
      );
      pools.push(pool);
      await pool.start();
      return pool;
    },
    async job(id: string): Promise<Job> {
      const { rows } = await db.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
      if (!rows[0]) throw new Error(`job ${id} not found`);
      return rows[0];
    },
    async count(sql: string, params: unknown[] = []): Promise<number> {
      const { rows } = await db.query<{ n: string }>(sql, params);
      return Number(rows[0]!.n);
    },
  };
}

export async function waitFor<T>(
  fn: () => Promise<T | undefined | false>,
  { timeoutMs = 10_000, intervalMs = 20, what = 'condition' } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function staleCount(op?: string): Promise<number> {
  const m = await staleCompletions.get();
  return m.values.filter((v) => !op || v.labels.op === op).reduce((s, v) => s + v.value, 0);
}

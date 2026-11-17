import { z } from 'zod';
import { type Handler, sleep } from './types.js';

const BenchPayload = z.object({
  mode: z.enum(['noop', 'write', 'sleep']).default('write'),
  ms: z.number().int().nonnegative().default(0),
});

/**
 * Synthetic load for experiments.
 *   noop  - do nothing (measures pure queue overhead)
 *   write - insert one row into `reports` keyed by job id (the default, so
 *           every experiment pays the same per-job database write)
 *   sleep - wait `ms` milliseconds (I/O-bound work, long jobs for heartbeat tests)
 */
export const benchHandler: Handler = async ({ job, db, signal, fault }) => {
  const p = BenchPayload.parse(job.payload);
  if (p.mode === 'sleep') return sleep(p.ms, signal);
  if (p.mode === 'write') {
    await db.query(
      `INSERT INTO reports (job_id, report_key, payload) VALUES ($1, $2, '{}')
       ON CONFLICT (report_key) DO NOTHING`,
      [job.id, `bench:${job.id}`],
    );
    fault('crash_after_write');
  }
};

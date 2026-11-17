import { describe, expect, it } from 'vitest';
import { enqueue } from '../src/queue/enqueue.js';
import { useDb, waitFor } from './helpers.js';

const t = useDb();

describe('test 4: crash between write and completion does not duplicate the write', () => {
  for (const point of ['crash_after_write', 'crash_before_complete'] as const) {
    it(`${point} on attempt 1 -> re-run after lease expiry -> one report row`, async () => {
      process.env.TASKRELAY_FAULTS = `${point}@1`;
      const { job } = await enqueue(t.db, { type: 'report', payload: { tenant_id: 7, period: '2025-03' } });
      const pool = await t.startPool({ concurrency: 2, leaseMs: 500, heartbeatMs: 150 });

      const done = await waitFor(async () => {
        const j = await t.job(job.id);
        return j.status === 'succeeded' && j;
      });

      expect(pool.workers.filter((w) => w.isCrashed)).toHaveLength(1);
      expect(done.attempts).toBe(2);
      expect(await t.count('SELECT count(*) AS n FROM job_runs WHERE job_id = $1', [job.id])).toBe(2);
      const reports = await t.db.query<{ payload: { attempt: number } }>(
        'SELECT payload FROM reports WHERE report_key = $1',
        ['7:2025-03'],
      );
      expect(reports.rows).toHaveLength(1);
      // The surviving row was written by the crashed first attempt: the second
      // attempt's INSERT really ran and was absorbed by ON CONFLICT DO NOTHING.
      expect(reports.rows[0]!.payload.attempt).toBe(1);
    });
  }
});

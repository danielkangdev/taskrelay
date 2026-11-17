import { describe, expect, it } from 'vitest';
import { claim } from '../src/queue/claim.js';
import { completeJob } from '../src/queue/complete.js';
import { enqueue, enqueueMany } from '../src/queue/enqueue.js';
import { useDb, waitFor } from './helpers.js';

const t = useDb();

describe('test 1: concurrent claiming never double-executes', () => {
  it('8 workers run 1,000 jobs exactly once each', async () => {
    await enqueueMany(t.db, 'default', 'bench', 1_000, `'{"mode":"write"}'::jsonb`);
    await t.startPool({ concurrency: 8 });

    await waitFor(async () => (await t.count(`SELECT count(*) AS n FROM jobs WHERE status = 'succeeded'`)) === 1_000, {
      timeoutMs: 60_000,
      what: '1,000 succeeded jobs',
    });

    const runs = await t.db.query<{ total: string; distinct_jobs: string; workers: string }>(
      'SELECT count(*) AS total, count(DISTINCT job_id) AS distinct_jobs, count(DISTINCT worker_id) AS workers FROM job_runs',
    );
    expect(runs.rows[0]).toMatchObject({ total: '1000', distinct_jobs: '1000' });
    expect(Number(runs.rows[0]!.workers)).toBeGreaterThan(1); // the load really was shared
    expect(await t.count('SELECT count(*) AS n FROM jobs WHERE attempts <> 1')).toBe(0);
    expect(await t.count('SELECT count(*) AS n FROM reports')).toBe(1_000);
  });

  it('64 simultaneous claim() calls on 200 jobs never return the same job twice', async () => {
    await enqueueMany(t.db, 'default', 'bench', 200);
    const seen = new Map<string, number>();
    for (let round = 0; round < 5; round++) {
      const batches = await Promise.all(
        Array.from({ length: 64 }, (_, i) =>
          claim(t.db, { queue: 'default', workerId: `w${i}`, leaseMs: 60_000, limit: 1 }),
        ),
      );
      for (const j of batches.flat()) seen.set(j.id, (seen.get(j.id) ?? 0) + 1);
    }
    expect(seen.size).toBe(200);
    expect([...seen.values()].every((n) => n === 1)).toBe(true);
  });

  it('claims by priority, then run_at; skips future jobs and other queues', async () => {
    const low = await enqueue(t.db, { type: 'bench', priority: 0 });
    const high = await enqueue(t.db, { type: 'bench', priority: 10 });
    await enqueue(t.db, { type: 'bench', priority: 100, delayMs: 60_000 });
    await enqueue(t.db, { type: 'bench', queue: 'other', priority: 100 });

    const got = await claim(t.db, { queue: 'default', workerId: 'w', leaseMs: 1_000, limit: 10 });
    expect(got.map((j) => j.id)).toEqual([high.job.id, low.job.id]);
    expect(got.every((j) => j.status === 'running' && j.attempts === 1 && j.lease_token)).toBe(true);
    expect(new Set(got.map((j) => j.lease_token)).size).toBe(2);
  });

  it.each(['skip_locked', 'for_update', 'naive'] as const)('strategy %s claims jobs sequentially', async (strategy) => {
    await enqueueMany(t.db, 'default', 'bench', 3);
    const got = await claim(t.db, { queue: 'default', workerId: 'w', leaseMs: 1_000, limit: 2, strategy });
    expect(got).toHaveLength(2);
    expect(got.every((j) => j.status === 'running' && j.locked_by === 'w' && j.attempts === 1)).toBe(true);
    expect(await claim(t.db, { queue: 'default', workerId: 'w', leaseMs: 1_000, limit: 2, strategy })).toHaveLength(1);
  });

  it('batch claim (limit 10) hands out disjoint batches', async () => {
    await enqueueMany(t.db, 'default', 'bench', 100);
    const batches = await Promise.all(
      Array.from({ length: 12 }, (_, i) => claim(t.db, { queue: 'default', workerId: `w${i}`, leaseMs: 60_000, limit: 10 })),
    );
    const ids = batches.flat().map((j) => j.id);
    expect(ids.length).toBe(100);
    expect(new Set(ids).size).toBe(100);
    for (const j of batches.flat()) await completeJob(t.db, j);
  });
});

import { describe, expect, it } from 'vitest';
import { claim } from '../src/queue/claim.js';
import { completeJob, failJob, heartbeat } from '../src/queue/complete.js';
import { enqueue } from '../src/queue/enqueue.js';
import { reapExpired } from '../src/queue/reaper.js';
import { reportHandler } from '../src/worker/handlers/report.js';
import { sleep, staleCount, useDb, waitFor } from './helpers.js';

const t = useDb();
const backoff = { baseMs: 50, capMs: 1_000 };

describe('test 2: expired leases are recovered', () => {
  it('a job claimed and abandoned is reclaimable after its lease expires', async () => {
    const { job } = await enqueue(t.db, { type: 'bench' });
    const [a] = await claim(t.db, { queue: 'default', workerId: 'A', leaseMs: 200 });
    expect(a!.id).toBe(job.id);

    // Before expiry: nothing to reap, nothing to claim.
    expect(await reapExpired(t.db)).toEqual({ requeued: 0, dead: 0 });
    expect(await claim(t.db, { queue: 'default', workerId: 'B', leaseMs: 5_000 })).toHaveLength(0);

    await sleep(300);
    expect(await reapExpired(t.db)).toEqual({ requeued: 1, dead: 0 });
    const [b] = await claim(t.db, { queue: 'default', workerId: 'B', leaseMs: 5_000 });
    expect(b).toMatchObject({ id: job.id, locked_by: 'B', attempts: 2 });
    expect((await t.job(job.id)).last_error).toMatch(/lease expired \(held by A\)/);
  });

  it('an expired job out of attempts goes to dead, not back to queued', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', maxAttempts: 1 });
    await claim(t.db, { queue: 'default', workerId: 'A', leaseMs: 50 });
    await sleep(120);
    expect(await reapExpired(t.db)).toEqual({ requeued: 0, dead: 1 });
    expect((await t.job(job.id)).status).toBe('dead');
  });

  it('end to end: a worker that hangs without heartbeating loses the job to another worker', async () => {
    process.env.TASKRELAY_FAULTS = 'hang_without_heartbeat@1';
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'write' } });
    await t.startPool({ concurrency: 2, leaseMs: 400, heartbeatMs: 100 });
    const done = await waitFor(async () => {
      const j = await t.job(job.id);
      return j.status === 'succeeded' && j;
    });
    expect(done.attempts).toBe(2);
    expect(await t.count('SELECT count(*) AS n FROM reports')).toBe(1);
  });
});

describe('test 3: a zombie worker cannot overwrite the new owner', () => {
  it("A's late completion affects 0 rows; final state and result are B's", async () => {
    const { job } = await enqueue(t.db, { type: 'report', payload: { tenant_id: 3, period: '2025-06' } });
    const [a] = await claim(t.db, { queue: 'default', workerId: 'A', leaseMs: 150 });
    await sleep(250);
    await reapExpired(t.db);
    const [b] = await claim(t.db, { queue: 'default', workerId: 'B', leaseMs: 5_000 });
    expect(b!.id).toBe(job.id);
    expect(b!.lease_token).not.toBe(a!.lease_token);

    const ctx = (j: typeof a, worker: string) => ({
      job: j!,
      db: t.db,
      workerId: worker,
      signal: new AbortController().signal,
      fault: () => undefined,
    });
    await reportHandler(ctx(b, 'B'));
    expect(await completeJob(t.db, b!)).toBe(true);

    // The zombie wakes up, redoes the work and tries to report success.
    const before = await staleCount('complete');
    await reportHandler(ctx(a, 'A'));
    expect(await completeJob(t.db, a!)).toBe(false);
    expect(await staleCount('complete')).toBe(before + 1);

    // Its other writes are fenced too.
    expect(await failJob(t.db, a!, new Error('late'), backoff)).toBe('stale');
    expect((await heartbeat(t.db, [a!], 5_000)).size).toBe(0);

    const final = await t.job(job.id);
    expect(final).toMatchObject({ status: 'succeeded', locked_by: 'B', lease_token: b!.lease_token, attempts: 2 });
    expect(final.last_error).toBeNull();
    const reports = await t.db.query('SELECT payload FROM reports WHERE report_key = $1', ['3:2025-06']);
    expect(reports.rows).toHaveLength(1);
    expect(reports.rows[0].payload).toMatchObject({ generated_by: 'B', lease_token: b!.lease_token });
  });

  it('a stale worker cannot complete a job that is running under a new lease', async () => {
    await enqueue(t.db, { type: 'bench' });
    const [a] = await claim(t.db, { queue: 'default', workerId: 'A', leaseMs: 50 });
    await sleep(100);
    await reapExpired(t.db);
    const [b] = await claim(t.db, { queue: 'default', workerId: 'B', leaseMs: 5_000 });
    expect(await completeJob(t.db, a!)).toBe(false);
    expect((await t.job(b!.id)).status).toBe('running'); // still B's, untouched
    expect(await completeJob(t.db, b!)).toBe(true);
    expect(await completeJob(t.db, b!)).toBe(false); // completing twice is also rejected
  });
});

describe('test 7: heartbeats keep long jobs alive', () => {
  it('a job running 4x longer than its lease is not reaped', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 2_000 } });
    await t.startPool({ concurrency: 2, leaseMs: 500, heartbeatMs: 150 });
    const done = await waitFor(async () => {
      const j = await t.job(job.id);
      return j.status === 'succeeded' && j;
    });
    expect(done.attempts).toBe(1);
    expect(await t.count('SELECT count(*) AS n FROM job_runs')).toBe(1);
  });

  it('control: without heartbeats the same job is reaped and run again', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 2_000 } });
    // heartbeatMs far beyond the run time = effectively no heartbeat
    await t.startPool({ concurrency: 2, leaseMs: 500, heartbeatMs: 60_000 });
    await waitFor(async () => (await t.count('SELECT count(*) AS n FROM job_runs')) >= 2, {
      what: 'a second execution',
    });
    expect((await t.job(job.id)).attempts).toBeGreaterThanOrEqual(2);
  });
});

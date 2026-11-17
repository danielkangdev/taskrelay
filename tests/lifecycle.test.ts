import { spawn } from 'node:child_process';
import { describe, expect, it } from 'vitest';
import { enqueue } from '../src/queue/enqueue.js';
import { TEST_DATABASE_URL } from './env.js';
import { sleep, useDb, waitFor } from './helpers.js';

const t = useDb();

describe('test 8: graceful shutdown', () => {
  it('stop() lets the in-flight job finish and claims nothing new', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 1_000 } });
    const pool = await t.startPool({ concurrency: 1 });
    await waitFor(async () => (await t.job(job.id)).status === 'running');

    const stopping = pool.stop();
    const later = await enqueue(t.db, { type: 'bench' });
    await stopping;

    expect(await t.job(job.id)).toMatchObject({ status: 'succeeded', attempts: 1 });
    expect(await t.count('SELECT count(*) AS n FROM job_runs WHERE job_id = $1', [job.id])).toBe(1);
    expect((await t.job(later.job.id)).status).toBe('queued');
  });

  it('on shutdown timeout the lease is released so another worker takes over at once', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 30_000 } });
    const pool = await t.startPool({ concurrency: 1, shutdownTimeoutMs: 300, leaseMs: 60_000 });
    await waitFor(async () => (await t.job(job.id)).status === 'running');
    await pool.stop();
    const released = await t.job(job.id);
    expect(released).toMatchObject({ status: 'queued', attempts: 1, locked_by: null });
  });

  it('batched jobs not yet started are released on stop', async () => {
    const a = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 500 } });
    const b = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 500 } });
    const pool = await t.startPool({ concurrency: 1, batchSize: 10 });
    await waitFor(async () => (await t.job(b.job.id)).status === 'running');
    await pool.stop();
    const statuses = [(await t.job(a.job.id)).status, (await t.job(b.job.id)).status].sort();
    expect(statuses).toEqual(['queued', 'succeeded']);
  });

  // Windows has no catchable SIGTERM (process.kill terminates immediately), so the
  // real-signal version runs on Linux/macOS (CI).
  it.skipIf(process.platform === 'win32')('a real SIGTERM to a worker process drains the job', async () => {
    const { job } = await enqueue(t.db, { type: 'bench', payload: { mode: 'sleep', ms: 1_500 } });
    const child = spawn(process.execPath, ['--import', 'tsx', 'src/main.ts', 'worker'], {
      env: { ...process.env, DATABASE_URL: TEST_DATABASE_URL, WORKER_CONCURRENCY: '1', TASKRELAY_FAULTS: '' },
      stdio: 'inherit',
    });
    const exited = new Promise<number | null>((r) => child.on('exit', (code) => r(code)));
    await waitFor(async () => (await t.job(job.id)).status === 'running', { timeoutMs: 20_000 });
    child.kill('SIGTERM');
    expect(await exited).toBe(0);
    expect(await t.job(job.id)).toMatchObject({ status: 'succeeded', attempts: 1 });
  });
});

describe('test 9: LISTEN/NOTIFY wakes idle workers', () => {
  it('a job starts well within the polling interval', async () => {
    await t.startPool({ concurrency: 2, pollIntervalMs: 5_000, useNotify: true });
    await sleep(200); // workers have done their initial empty claim and are idle
    const { job } = await enqueue(t.db, { type: 'bench' });
    const done = await waitFor(async () => {
      const j = await t.job(job.id);
      return j.started_at ? j : undefined;
    });
    const latencyMs = +done.started_at! - +done.created_at;
    expect(latencyMs).toBeLessThan(500);
  });

  it('control: with NOTIFY off the job waits for the next poll', async () => {
    await t.startPool({ concurrency: 2, pollIntervalMs: 2_000, useNotify: false });
    await sleep(200);
    const { job } = await enqueue(t.db, { type: 'bench' });
    const done = await waitFor(async () => {
      const j = await t.job(job.id);
      return j.started_at ? j : undefined;
    });
    // Next poll comes 1-3 s (2 s +/-50% jitter) after the initial empty claim; we enqueued ~0.2 s in.
    expect(+done.started_at! - +done.created_at).toBeGreaterThan(600);
  });
});

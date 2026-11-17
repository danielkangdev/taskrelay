import { describe, expect, it } from 'vitest';
import { buildServer } from '../src/api/server.js';
import { backoffRange } from '../src/queue/backoff.js';
import { claim } from '../src/queue/claim.js';
import { failJob } from '../src/queue/complete.js';
import { enqueue } from '../src/queue/enqueue.js';
import { useDb, waitFor } from './helpers.js';

const t = useDb();

describe('test 5: retries back off exponentially, then dead-letter', () => {
  it('run_at moves out by the backoff schedule each attempt; attempt max_attempts -> dead', async () => {
    const backoff = { baseMs: 1_000, capMs: 60_000 };
    const { job } = await enqueue(t.db, { type: 'bench', maxAttempts: 4 });
    const delays: number[] = [];

    for (let attempt = 1; attempt <= 4; attempt++) {
      // Time travel instead of sleeping through the backoff.
      await t.db.query('UPDATE jobs SET run_at = now() WHERE id = $1', [job.id]);
      const [j] = await claim(t.db, { queue: 'default', workerId: 'w', leaseMs: 5_000 });
      expect(j).toMatchObject({ id: job.id, attempts: attempt });
      const outcome = await failJob(t.db, j!, new Error(`boom ${attempt}`), backoff);
      const after = await t.db.query<{ status: string; delay_ms: number; last_error: string }>(
        `SELECT status, last_error, (extract(epoch FROM run_at - now()) * 1000)::float8 AS delay_ms FROM jobs WHERE id = $1`,
        [job.id],
      );
      const row = after.rows[0]!;
      expect(row.last_error).toBe(`Error: boom ${attempt}`);
      if (attempt < 4) {
        expect(outcome).toBe('retried');
        expect(row.status).toBe('queued');
        const [lo, hi] = backoffRange(attempt, backoff);
        expect(row.delay_ms).toBeGreaterThan(lo - 200);
        expect(row.delay_ms).toBeLessThanOrEqual(hi);
        // Not claimable until run_at.
        expect(await claim(t.db, { queue: 'default', workerId: 'w', leaseMs: 5_000 })).toHaveLength(0);
        delays.push(row.delay_ms);
      } else {
        expect(outcome).toBe('dead');
        expect(row.status).toBe('dead');
      }
    }
    // 1s, 2s, 4s bands do not overlap, so the delays strictly increase.
    expect(delays[0]!).toBeLessThan(delays[1]!);
    expect(delays[1]!).toBeLessThan(delays[2]!);
  });

  it('end to end through workers and the API: dead letter listing and manual retry', async () => {
    process.env.TASKRELAY_FAULTS = 'handler_throws';
    const { job } = await enqueue(t.db, { type: 'bench', maxAttempts: 3 });
    const pool = await t.startPool({ concurrency: 1, backoff: { baseMs: 20, capMs: 100 } });
    const dead = await waitFor(async () => {
      const j = await t.job(job.id);
      return j.status === 'dead' && j;
    });
    expect(dead.attempts).toBe(3);
    expect(dead.last_error).toMatch(/InjectedFault: injected fault at handler_throws/);
    await pool.stop();
    delete process.env.TASKRELAY_FAULTS;

    const app = buildServer(t.db);
    const list = await app.inject({ method: 'GET', url: '/dead-letter' });
    expect(list.statusCode).toBe(200);
    expect(list.json()).toMatchObject({ count: 1, jobs: [{ id: job.id, attempts: 3, status: 'dead' }] });

    const retried = await app.inject({ method: 'POST', url: `/jobs/${job.id}/retry` });
    expect(retried.statusCode).toBe(200);
    expect(retried.json().job).toMatchObject({ status: 'queued', attempts: 0 });

    // Retrying a job that is not dead is a conflict, unknown ids are 404.
    expect((await app.inject({ method: 'POST', url: `/jobs/${job.id}/retry` })).statusCode).toBe(409);
    const missing = await app.inject({ method: 'POST', url: `/jobs/00000000-0000-4000-8000-000000000000/retry` });
    expect(missing.statusCode).toBe(404);

    // With the fault gone the retried job now succeeds.
    await t.startPool({ concurrency: 1 });
    await waitFor(async () => (await t.job(job.id)).status === 'succeeded');
    await app.close();
  });
});

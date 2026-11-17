/**
 * E3: pickup latency at low load, polling-only vs LISTEN/NOTIFY.
 * Jobs are enqueued one at a time at RATE/s (exponential inter-arrival times)
 * for SECONDS. Latency = jobs.started_at - jobs.created_at, both stamped by the
 * database clock. Poll interval is 1 s in both modes; NOTIFY mode keeps polling
 * as a fallback.
 */
import { createPool } from '../src/db/pool.js';
import { enqueue } from '../src/queue/enqueue.js';
import { DATABASE_URL, environment, percentile, resetJobs, round, writeResult } from './lib.js';
import { defaultWorkerConfig } from '../src/config.js';
import { WorkerPool } from '../src/worker/pool.js';
import { defaultHandlers } from '../src/worker/handlers/index.js';

const RATE = Number(process.env.E3_RATE ?? 5);
const SECONDS = Number(process.env.E3_SECONDS ?? 60);
const WORKERS = (process.env.E3_WORKERS ?? '1,4').split(',').map(Number);
const POLL_MS = 1_000;

const db = createPool(DATABASE_URL, 20);
const runs = [];

for (const workers of WORKERS) {
  for (const mode of ['poll', 'notify'] as const) {
    await resetJobs(db);
    const pool = new WorkerPool(
      db,
      defaultWorkerConfig({ concurrency: workers, pollIntervalMs: POLL_MS, useNotify: mode === 'notify' }),
      defaultHandlers,
    );
    await pool.start();
    await new Promise((r) => setTimeout(r, 1_500)); // let workers go idle

    const end = Date.now() + SECONDS * 1_000;
    let sent = 0;
    while (Date.now() < end) {
      await enqueue(db, { type: 'bench', payload: { mode: 'write' } });
      sent++;
      await new Promise((r) => setTimeout(r, -Math.log(1 - Math.random()) * (1_000 / RATE)));
    }
    // Wait for the tail.
    for (;;) {
      const { rows } = await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM jobs WHERE status <> 'succeeded'`);
      if (rows[0]!.n === 0) break;
      await new Promise((r) => setTimeout(r, 200));
    }
    await pool.stop();

    const { rows } = await db.query<{ ms: number }>(
      `SELECT (extract(epoch FROM started_at - created_at) * 1000)::float8 AS ms FROM jobs`,
    );
    const ms = rows.map((r) => r.ms);
    const run = {
      mode,
      workers,
      poll_interval_ms: POLL_MS,
      jobs: sent,
      latency_ms: {
        p50: round(percentile(ms, 50), 1),
        p95: round(percentile(ms, 95), 1),
        p99: round(percentile(ms, 99), 1),
        max: round(Math.max(...ms), 1),
        mean: round(ms.reduce((a, b) => a + b, 0) / ms.length, 1),
      },
    };
    runs.push(run);
    console.log(`${mode} workers=${workers}: ${JSON.stringify(run.latency_ms)} over ${sent} jobs`);
  }
}

await writeResult('e3_wakeup', {
  experiment: 'E3 wake-up latency',
  method:
    `Poisson arrivals at ${RATE} jobs/s for ${SECONDS}s, bench(write) jobs, poll interval ${POLL_MS} ms. ` +
    'Latency = started_at - created_at (database clock).',
  environment: await environment(db),
  runs,
});
await db.end();

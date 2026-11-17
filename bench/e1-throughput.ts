/**
 * E1: throughput vs number of workers, batch size 1 and 10.
 *
 * Workers are concurrent loops inside ONE Node process (each with its own
 * PostgreSQL connection), not separate containers; this keeps the client side
 * identical across runs. Client CPU is recorded so a client-side bottleneck
 * would be visible.
 *
 * Load: `bench` handler in `write` mode (one INSERT into reports per job), with
 * the run-log INSERT on, i.e. per job: claim UPDATE, run-log INSERT, report
 * INSERT, complete UPDATE = 4 statements / 4 commits (claim is per batch).
 */
import { enqueueMany } from '../src/queue/enqueue.js';
import {
  ActivitySampler,
  benchPool,
  resultSuffix,
  duplicateExecutions,
  environment,
  percentile,
  resetJobs,
  resetStats,
  round,
  runPool,
  topStatements,
  walStats,
  writeResult,
} from './lib.js';

const JOBS = Number(process.env.E1_JOBS ?? 100_000);
const WORKERS = (process.env.E1_WORKERS ?? '1,2,4,8,16').split(',').map(Number);
const BATCHES = (process.env.E1_BATCH ?? '1,10').split(',').map(Number);
const REPEATS = Number(process.env.REPEATS ?? 1);

// One connection per worker loop, plus LISTEN, the done-check and headroom.
const db = benchPool(Math.max(...WORKERS) + 8);
const runs = [];

for (const batch of BATCHES) {
  for (const workers of WORKERS) {
    for (let rep = 1; rep <= REPEATS; rep++) {
      await resetJobs(db);
      await enqueueMany(db, 'default', 'bench', JOBS, `'{"mode":"write"}'::jsonb`);
      await db.query('VACUUM ANALYZE jobs');
      await db.query('CHECKPOINT');
      await resetStats(db);

      const sampler = new ActivitySampler();
      await sampler.start(100);
      const r = await runPool(
        db,
        { concurrency: workers, batchSize: batch, useNotify: true, pollIntervalMs: 1_000 },
        async (s) => (s.outcomes.succeeded ?? 0) >= JOBS,
        { pollMs: 25 },
      );
      const activity = await sampler.stop();

      const dup = await duplicateExecutions(db);
      const statusRows = await db.query(`SELECT status, count(*)::int AS n FROM jobs GROUP BY status`);
      const run = {
        workers,
        batch,
        rep,
        jobs: JOBS,
        seconds: round(r.wallMs / 1000, 2),
        throughput: round(JOBS / (r.wallMs / 1000), 0),
        exec_ms: { p50: round(percentile(r.execMs, 50)), p95: round(percentile(r.execMs, 95)), p99: round(percentile(r.execMs, 99)) },
        claim_ms: { p50: round(percentile(r.claimMs, 50)), p95: round(percentile(r.claimMs, 95)) },
        db_cpu_cores: round(r.dbCpuMs / r.wallMs, 2),
        client_cpu_cores: round(r.clientCpuMs / r.wallMs, 2),
        backends_by_state: activity,
        wal: await walStats(db),
        top_statements: await topStatements(db),
        duplicates: dup.duplicates,
        statuses: Object.fromEntries(statusRows.rows.map((x) => [x.status, x.n])),
      };
      runs.push(run);
      console.log(
        `batch=${batch} workers=${workers} rep=${rep}: ${run.throughput} jobs/s in ${run.seconds}s, ` +
          `exec p50/p95 ${run.exec_ms.p50}/${run.exec_ms.p95} ms, db cpu ${run.db_cpu_cores}, ` +
          `client cpu ${run.client_cpu_cores}, dup ${dup.duplicates}, activity ${JSON.stringify(activity)}`,
      );
    }
  }
}

await writeResult(`e1_throughput${resultSuffix}`, {
  experiment: 'E1 throughput vs workers',
  method:
    'Workers are concurrent loops in one Node.js process (one PG connection each). ' +
    `${JOBS} pre-inserted bench jobs (mode=write), skip_locked claiming, run log on. ` +
    'DB CPU is the PostgreSQL container cgroup usage_usec delta / wall time (1.0 = one core busy).',
  environment: await environment(db),
  runs,
});
await db.end();

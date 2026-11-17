/**
 * E2: claim strategy comparison on identical load.
 *   naive       SELECT ... LIMIT 1, then UPDATE ... WHERE id = $1 (no lock, no re-check)
 *   for_update  SELECT ... FOR UPDATE LIMIT 1 (no SKIP LOCKED)
 *   skip_locked SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1
 * 10,000 bench(write) jobs, 8 workers, batch size 1, REPEATS runs each.
 * Duplicate executions come from the job_runs log (one row per execution start).
 */
import { CLAIM_STRATEGIES } from '../src/config.js';
import { staleCompletions } from '../src/metrics.js';
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
  writeResult,
} from './lib.js';

const JOBS = Number(process.env.E2_JOBS ?? 10_000);
const WORKERS = Number(process.env.E2_WORKERS ?? 8);
const REPEATS = Number(process.env.REPEATS ?? 3);
const STRATEGIES = (process.env.E2_STRATEGIES?.split(',') ?? ['skip_locked', 'for_update', 'naive']) as (typeof CLAIM_STRATEGIES)[number][];

const db = benchPool(20);
const runs = [];

const staleTotal = async () => (await staleCompletions.get()).values.reduce((s, v) => s + v.value, 0);

for (let rep = 1; rep <= REPEATS; rep++) {
  for (const strategy of STRATEGIES) {
    await resetJobs(db);
    await enqueueMany(db, 'default', 'bench', JOBS, `'{"mode":"write"}'::jsonb`);
    await db.query('VACUUM ANALYZE jobs');
    await db.query('CHECKPOINT');
    await resetStats(db);
    const stale0 = await staleTotal();

    const sampler = new ActivitySampler();
    await sampler.start(50);
    // Done = nothing left queued or running (naive can re-run finished jobs, so
    // counting successes in-process is not enough).
    const r = await runPool(
      db,
      { concurrency: WORKERS, batchSize: 1, claimStrategy: strategy, useNotify: true },
      async () => {
        const { rows } = await db.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM jobs WHERE status IN ('queued', 'running')`,
        );
        return rows[0]!.n === 0;
      },
      { pollMs: 100 },
    );
    const activity = await sampler.stop();
    // Precise end time from the database instead of the polling loop.
    const { rows } = await db.query<{ secs: number }>(
      `SELECT extract(epoch FROM max(finished_at) - $1::timestamptz)::float8 AS secs FROM jobs`,
      [r.dbStart],
    );
    const secs = rows[0]!.secs;
    const dup = await duplicateExecutions(db);
    const dupJobs = await db.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM (SELECT job_id FROM job_runs GROUP BY job_id HAVING count(*) > 1) d`,
    );
    const lockWaitShare =
      Object.entries(activity)
        .filter(([k]) => k.startsWith('Lock:'))
        .reduce((s, [, v]) => s + v, 0) / WORKERS;

    const run = {
      strategy,
      rep,
      jobs: JOBS,
      workers: WORKERS,
      seconds: round(secs, 2),
      throughput: round(JOBS / secs, 0),
      executions: dup.runs,
      duplicate_executions: dup.duplicates,
      jobs_executed_more_than_once: dupJobs.rows[0]!.n,
      stale_completions: (await staleTotal()) - stale0,
      claim_ms: {
        p50: round(percentile(r.claimMs, 50)),
        p95: round(percentile(r.claimMs, 95)),
        p99: round(percentile(r.claimMs, 99)),
      },
      empty_claims: r.emptyClaims,
      avg_backends_waiting_on_lock: round(lockWaitShare * WORKERS, 2),
      lock_wait_share_of_workers: round(lockWaitShare, 3),
      backends_by_state: activity,
      db_cpu_cores: round(r.dbCpuMs / r.wallMs, 2),
      top_statements: await topStatements(db, 4),
    };
    runs.push(run);
    console.log(
      `${strategy} rep=${rep}: ${run.throughput} jobs/s, dup exec ${run.duplicate_executions} ` +
        `(${run.jobs_executed_more_than_once} jobs), stale ${run.stale_completions}, claim p95 ${run.claim_ms.p95} ms, ` +
        `lock-waiting backends ${run.avg_backends_waiting_on_lock}`,
    );
  }
}

await writeResult(`e2_claim_strategies${resultSuffix}`, {
  experiment: 'E2 claim strategies',
  method:
    `${JOBS} bench(write) jobs, ${WORKERS} worker loops in one process, batch 1, ${REPEATS} repetitions ` +
    'interleaved by strategy. Lock waits: pg_stat_activity sampled every 50 ms.',
  environment: await environment(db),
  runs,
});
await db.end();

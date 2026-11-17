/**
 * E4: chaos. 10,000 report jobs, 8 worker containers (1 loop each). Every 2-4 s
 * a random worker container gets SIGKILL (`docker kill`), and is restarted 1 s
 * later. Afterwards we check:
 *   - every job ended `succeeded` or `dead`, none stuck in `running`
 *   - duplicate executions (at-least-once actually happened)
 *   - reports rows vs succeeded jobs, duplicate report_keys (idempotency held)
 *
 * Runs on the host (it drives the docker CLI); the workers and PostgreSQL run
 * in docker compose. Usage: npm run bench:e4
 */
import { execFileSync } from 'node:child_process';
import { createPool } from '../src/db/pool.js';
import { seedOrders } from '../src/db/seed.js';
import { environment, resetJobs, round, writeResult } from './lib.js';

const JOBS_TENANTS = 1_000;
const JOBS_PERIODS = Number(process.env.E4_PERIODS ?? 10); // 1,000 x 10 = 10,000 jobs
const WORKERS = Number(process.env.E4_WORKERS ?? 8);
const SIM_MS = Number(process.env.E4_SIM_MS ?? 100);
const LEASE_MS = Number(process.env.E4_LEASE_MS ?? 5_000);
const KILL_MIN_MS = 2_000;
const KILL_MAX_MS = 4_000;
const TIMEOUT_MS = 30 * 60_000;

// The host talks to the compose postgres through the published port; only a
// handful of queries per second, so the Docker Desktop port proxy is fine here.
const db = createPool(process.env.DATABASE_URL ?? 'postgres://taskrelay:taskrelay@localhost:5432/taskrelay', 4);

const composeEnv = {
  ...process.env,
  WORKER_CONCURRENCY: '1',
  LEASE_MS: String(LEASE_MS),
  REAPER_INTERVAL_MS: '1000',
  TASKRELAY_FAULTS: '',
};
const sh = (args: string[]) => execFileSync('docker', args, { env: composeEnv, encoding: 'utf8' }).trim();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function remaining(): Promise<number> {
  const { rows } = await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM jobs WHERE status IN ('queued','running')`);
  return rows[0]!.n;
}

sh(['compose', 'up', '-d', '--wait', 'postgres']);
sh(['compose', 'rm', '-sf', 'worker']);
await resetJobs(db);
await seedOrders(db);
await db.query(
  `INSERT INTO jobs (type, payload)
   SELECT 'report', jsonb_build_object('tenant_id', t, 'period', '2025-' || lpad(m::text, 2, '0'), 'simulate_ms', $3::int)
   FROM generate_series(1, $1::int) t, generate_series(1, $2::int) m`,
  [JOBS_TENANTS, JOBS_PERIODS, SIM_MS],
);
const total = JOBS_TENANTS * JOBS_PERIODS;
console.log(`enqueued ${total} report jobs; starting ${WORKERS} workers`);

const t0 = Date.now();
sh(['compose', 'up', '-d', '--scale', `worker=${WORKERS}`, 'worker']);

const kills: { at_s: number; container: string; jobs_in_flight: number }[] = [];
const restarts: Promise<void>[] = [];
while ((await remaining()) > 0) {
  if (Date.now() - t0 > TIMEOUT_MS) throw new Error('chaos run timed out');
  await sleep(KILL_MIN_MS + Math.random() * (KILL_MAX_MS - KILL_MIN_MS));
  const running = sh(['compose', 'ps', '-q', '--status', 'running', 'worker']).split(/\s+/).filter(Boolean);
  if (running.length === 0) continue;
  const victim = running[Math.floor(Math.random() * running.length)]!;
  const short = victim.slice(0, 12); // container hostname = short id = worker id prefix
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM jobs WHERE status = 'running' AND locked_by LIKE $1`,
    [`${short}-%`],
  );
  sh(['kill', '--signal', 'KILL', victim]);
  kills.push({ at_s: round((Date.now() - t0) / 1000, 1), container: short, jobs_in_flight: rows[0]!.n });
  console.log(`[${kills.at(-1)!.at_s}s] SIGKILL ${short} (holding ${rows[0]!.n} job), remaining ${await remaining()}`);
  restarts.push(sleep(1_000).then(() => void sh(['start', victim])));
}
const wallS = (Date.now() - t0) / 1000;
await Promise.all(restarts);
sh(['compose', 'stop', 'worker']);

// ---- verification ---------------------------------------------------------
const one = async <T>(sql: string) => (await db.query(sql)).rows[0] as T;
const statuses = Object.fromEntries(
  (await db.query<{ status: string; n: number }>(`SELECT status, count(*)::int AS n FROM jobs GROUP BY status`)).rows.map(
    (r) => [r.status, r.n],
  ),
);
const runs = await one<{ runs: number; distinct_jobs: number; multi: number }>(
  `SELECT count(*)::int AS runs, count(DISTINCT job_id)::int AS distinct_jobs,
          (SELECT count(*)::int FROM (SELECT 1 FROM job_runs GROUP BY job_id HAVING count(*) > 1) x) AS multi
   FROM job_runs`,
);
const reports = await one<{ rows: number; distinct_keys: number; dup_keys: number }>(
  `SELECT count(*)::int AS rows, count(DISTINCT report_key)::int AS distinct_keys,
          (SELECT count(*)::int FROM (SELECT 1 FROM reports GROUP BY report_key HAVING count(*) > 1) x) AS dup_keys
   FROM reports`,
);
const consistency = await one<{
  succeeded_without_report: number;
  reports_of_dead_jobs: number;
  absorbed_writes: number;
}>(
  `SELECT
     (SELECT count(*)::int FROM jobs j WHERE j.status = 'succeeded'
        AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.job_id = j.id)) AS succeeded_without_report,
     (SELECT count(*)::int FROM reports r JOIN jobs j ON j.id = r.job_id WHERE j.status = 'dead') AS reports_of_dead_jobs,
     -- The final, successful attempt ran the INSERT (the handler returned), but the
     -- stored row carries an earlier attempt's lease token: that INSERT was absorbed.
     (SELECT count(*)::int FROM jobs j JOIN reports r ON r.job_id = j.id
        WHERE j.status = 'succeeded' AND r.payload->>'lease_token' <> j.lease_token::text) AS absorbed_writes`,
);
const attempts = (
  await db.query<{ attempts: number; n: number }>(
    `SELECT attempts, count(*)::int AS n FROM jobs GROUP BY attempts ORDER BY attempts`,
  )
).rows;

const result = {
  experiment: 'E4 chaos (SIGKILL workers)',
  method:
    `${total} report jobs (simulate_ms=${SIM_MS}), ${WORKERS} worker containers x 1 loop, lease ${LEASE_MS} ms, ` +
    `reaper every 1 s. Every ${KILL_MIN_MS / 1000}-${KILL_MAX_MS / 1000} s a random running worker container is ` +
    'killed with `docker kill --signal KILL` and restarted 1 s later.',
  environment: { ...(await environment(db)), client: { note: 'orchestrator on Windows host; workers in containers' } },
  wall_seconds: round(wallS, 1),
  kills: kills.length,
  kills_holding_a_job: kills.filter((k) => k.jobs_in_flight > 0).length,
  jobs: { total, ...statuses, stuck_running: statuses.running ?? 0 },
  executions: { total: runs.runs, distinct_jobs: runs.distinct_jobs, duplicates: runs.runs - runs.distinct_jobs, jobs_run_more_than_once: runs.multi },
  reports: { ...reports, succeeded_jobs: statuses.succeeded ?? 0 },
  consistency,
  attempts_histogram: Object.fromEntries(attempts.map((a) => [a.attempts, a.n])),
  kill_log: kills,
};
console.log(JSON.stringify({ ...result, kill_log: undefined, environment: undefined }, null, 2));
await writeResult('e4_chaos', result);
await db.end();

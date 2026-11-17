import { mkdir, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import pg from 'pg';
import { defaultWorkerConfig, type WorkerConfig } from '../src/config.js';
import { migrate } from '../src/db/migrate.js';
import { type Db, createPool } from '../src/db/pool.js';
import { defaultHandlers } from '../src/worker/handlers/index.js';
import { WorkerPool } from '../src/worker/pool.js';
import type { JobOutcome } from '../src/worker/worker.js';

export const DATABASE_URL = process.env.DATABASE_URL ?? 'postgres://taskrelay:taskrelay@localhost:5432/taskrelay';

/**
 * SYNC_COMMIT=off runs the same experiment with synchronous_commit=off on the
 * bench connections: commits stop waiting for the WAL fsync. Used only to test
 * the hypothesis that WAL flushing is the bottleneck, never as a recommendation.
 */
export const SYNC_COMMIT = process.env.SYNC_COMMIT === 'off' ? 'off' : 'on';
export const resultSuffix = (SYNC_COMMIT === 'off' ? '_async_commit' : '') + (process.env.RESULT_TAG ?? '');

export function benchPool(max: number): Db {
  const db = createPool(DATABASE_URL, max);
  if (SYNC_COMMIT === 'off') db.on('connect', (c) => void c.query('SET synchronous_commit = off'));
  return db;
}

export function percentile(values: number[], p: number): number {
  if (values.length === 0) return NaN;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))]!;
}

export const round = (x: number, d = 2) => Math.round(x * 10 ** d) / 10 ** d;

/** Where and on what the numbers were produced. Stored with every result. */
export async function environment(db: Db) {
  const q = async (sql: string) => (await db.query(sql)).rows[0];
  const settings = await db.query<{ name: string; setting: string; unit: string | null }>(
    `SELECT name, setting, unit FROM pg_settings WHERE name IN
     ('shared_buffers','max_connections','synchronous_commit','fsync','wal_level','max_wal_size','work_mem')`,
  );
  const cpuMax = await db
    .query<{ v: string }>(`SELECT pg_read_file('/sys/fs/cgroup/cpu.max') AS v`)
    .then((r) => r.rows[0]!.v.trim())
    .catch(() => 'unknown');
  return {
    timestamp: new Date().toISOString(),
    client: {
      in_docker: process.env.TASKRELAY_IN_DOCKER === '1',
      node: process.version,
      platform: `${os.platform()} ${os.release()}`,
      cpu_model: os.cpus()[0]?.model,
      cpu_cores: os.cpus().length,
      mem_gb: round(os.totalmem() / 2 ** 30, 1),
    },
    postgres: {
      version: (await q('SHOW server_version')).server_version,
      in_docker: true,
      container_cpu_limit: cpuMax, // "max 100000" = no limit
      settings: Object.fromEntries(settings.rows.map((s) => [s.name, s.unit ? `${s.setting} ${s.unit}` : s.setting])),
    },
    bench_synchronous_commit: SYNC_COMMIT,
    host_note: 'Windows 11 host, Docker Desktop (WSL2 VM); client and PostgreSQL share the VM.',
  };
}

/** CPU time consumed by the PostgreSQL container (cgroup v2), in microseconds. */
export async function dbCpuUsec(db: Db): Promise<number> {
  const { rows } = await db.query<{ v: string }>(`SELECT pg_read_file('/sys/fs/cgroup/cpu.stat') AS v`);
  const m = /usage_usec (\d+)/.exec(rows[0]!.v);
  return m ? Number(m[1]) : NaN;
}

export async function resetStats(db: Db): Promise<void> {
  await db.query('SELECT pg_stat_statements_reset()');
  await db.query(`SELECT pg_stat_reset_shared('wal')`);
}

export async function walStats(db: Db) {
  const { rows } = await db.query(
    `SELECT wal_records::bigint AS records, wal_bytes::bigint AS bytes, wal_write AS writes, wal_sync AS syncs,
            round(wal_write_time::numeric, 1) AS write_ms, round(wal_sync_time::numeric, 1) AS sync_ms
     FROM pg_stat_wal`,
  );
  const r = rows[0];
  return Object.fromEntries(Object.entries(r).map(([k, v]) => [k, Number(v)]));
}

/** Most expensive statements since the last reset (pg_stat_statements). */
export async function topStatements(db: Db, n = 6) {
  const { rows } = await db.query(
    `SELECT regexp_replace(left(query, 90), '\\s+', ' ', 'g') AS query, calls::bigint AS calls,
            round(total_exec_time::numeric, 0) AS total_ms, round(mean_exec_time::numeric, 3) AS mean_ms,
            round((blk_read_time + blk_write_time)::numeric, 0) AS io_ms
     FROM pg_stat_statements
     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
       AND query NOT ILIKE '%pg_stat%' AND query NOT ILIKE '%pg_read_file%'
     ORDER BY total_exec_time DESC LIMIT $1`,
    [n],
  );
  return rows.map((r) => ({ ...r, calls: Number(r.calls), total_ms: Number(r.total_ms), mean_ms: Number(r.mean_ms), io_ms: Number(r.io_ms) }));
}

/**
 * Samples pg_stat_activity of our client backends every `intervalMs` and
 * tallies what they are doing: on CPU (active, no wait event), waiting on a
 * lock, on WAL/IO, or idle (= waiting for the client).
 */
export class ActivitySampler {
  private client = new pg.Client({ connectionString: DATABASE_URL });
  private timer?: NodeJS.Timeout;
  private samples = 0;
  readonly tally = new Map<string, number>();

  async start(intervalMs = 100): Promise<void> {
    await this.client.connect();
    this.timer = setInterval(() => void this.sample(), intervalMs);
  }

  private async sample(): Promise<void> {
    const { rows } = await this.client.query<{ k: string; n: string }>(
      `SELECT CASE WHEN state = 'active' THEN coalesce(wait_event_type || ':' || wait_event, 'CPU')
                   ELSE coalesce(state, 'unknown') END AS k, count(*) AS n
       FROM pg_stat_activity
       WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid()
         AND query NOT ILIKE 'LISTEN%'
       GROUP BY 1`,
    );
    this.samples++;
    for (const r of rows) this.tally.set(r.k, (this.tally.get(r.k) ?? 0) + Number(r.n));
  }

  /** Average number of backends in each state per sample. */
  async stop(): Promise<Record<string, number>> {
    clearInterval(this.timer);
    await this.client.end();
    const out: Record<string, number> = {};
    for (const [k, v] of [...this.tally.entries()].sort((a, b) => b[1] - a[1])) out[k] = round(v / Math.max(1, this.samples), 2);
    return out;
  }
}

export async function resetJobs(db: Db): Promise<void> {
  await migrate(db);
  await db.query('TRUNCATE jobs, job_runs, reports RESTART IDENTITY');
}

export interface RunStats {
  claimMs: number[];
  emptyClaims: number;
  execMs: number[];
  outcomes: Record<string, number>;
}

/** Run a worker pool until `done()` is true; collect per-claim and per-job timings. */
export async function runPool(
  db: Db,
  cfg: Partial<WorkerConfig>,
  done: (s: RunStats) => Promise<boolean>,
  { pollMs = 200, timeoutMs = 30 * 60_000 } = {},
): Promise<RunStats & { wallMs: number; clientCpuMs: number; dbCpuMs: number; dbStart: Date }> {
  const stats: RunStats = { claimMs: [], emptyClaims: 0, execMs: [], outcomes: {} };
  const pool = new WorkerPool(db, defaultWorkerConfig({ reaperIntervalMs: 5_000, ...cfg }), defaultHandlers, {
    crashMode: 'stop',
    hooks: {
      onClaim(ms, n) {
        if (n > 0) stats.claimMs.push(ms);
        else stats.emptyClaims++;
      },
      onJobDone(_job, outcome: JobOutcome, ms) {
        stats.execMs.push(ms);
        stats.outcomes[outcome] = (stats.outcomes[outcome] ?? 0) + 1;
      },
    },
  });
  const cpu0 = process.cpuUsage();
  const db0 = await dbCpuUsec(db);
  const dbStart = (await db.query<{ now: Date }>('SELECT clock_timestamp() AS now')).rows[0]!.now;
  const t0 = performance.now();
  await pool.start();
  const deadline = Date.now() + timeoutMs;
  while (!(await done(stats))) {
    if (Date.now() > deadline) throw new Error('run timed out');
    await new Promise((r) => setTimeout(r, pollMs));
  }
  const wallMs = performance.now() - t0;
  const cpu = process.cpuUsage(cpu0);
  const db1 = await dbCpuUsec(db);
  await pool.stop();
  return { ...stats, wallMs, clientCpuMs: (cpu.user + cpu.system) / 1000, dbCpuMs: (db1 - db0) / 1000, dbStart };
}

export async function duplicateExecutions(db: Db): Promise<{ runs: number; distinct: number; duplicates: number }> {
  const { rows } = await db.query<{ runs: string; d: string }>(
    'SELECT count(*) AS runs, count(DISTINCT job_id) AS d FROM job_runs',
  );
  const runs = Number(rows[0]!.runs);
  const distinct = Number(rows[0]!.d);
  return { runs, distinct, duplicates: runs - distinct };
}

export async function writeResult(name: string, data: unknown): Promise<string> {
  const dir = path.resolve('bench/results');
  await mkdir(dir, { recursive: true });
  const file = path.join(dir, `${name}_${new Date().toISOString().slice(0, 10)}.json`);
  await writeFile(file, JSON.stringify(data, null, 2) + '\n');
  console.log(`wrote ${file}`);
  return file;
}

import client from 'prom-client';
import type { Db } from './db/pool.js';

export const registry = new client.Registry();

export const jobsFinished = new client.Counter({
  name: 'taskrelay_jobs_finished_total',
  help: 'Job attempts that ended, by outcome (succeeded | retried | dead).',
  labelNames: ['queue', 'type', 'outcome'] as const,
  registers: [registry],
});

export const staleCompletions = new client.Counter({
  name: 'taskrelay_stale_completion_total',
  help: 'Writes rejected because the worker no longer held the lease (lease_token mismatch).',
  labelNames: ['queue', 'op'] as const,
  registers: [registry],
});

export const leasesExpired = new client.Counter({
  name: 'taskrelay_lease_expired_total',
  help: 'Running jobs whose lease expired and were recovered by the reaper, by resulting status.',
  labelNames: ['status'] as const,
  registers: [registry],
});

const SECONDS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300];

export const jobWait = new client.Histogram({
  name: 'taskrelay_job_wait_seconds',
  help: 'Time from run_at (when a job became runnable) to being claimed.',
  labelNames: ['queue', 'type'] as const,
  buckets: SECONDS,
  registers: [registry],
});

export const jobDuration = new client.Histogram({
  name: 'taskrelay_job_duration_seconds',
  help: 'Handler execution time.',
  labelNames: ['queue', 'type', 'outcome'] as const,
  buckets: SECONDS,
  registers: [registry],
});

let depthDb: Db | undefined;

/** Queue depth by status, read from the database at scrape time. */
new client.Gauge({
  name: 'taskrelay_queue_depth',
  help: 'Number of jobs per queue and status (status="dead" is the dead-letter queue size).',
  labelNames: ['queue', 'status'] as const,
  registers: [registry],
  async collect() {
    if (!depthDb) return;
    this.reset();
    const { rows } = await depthDb.query<{ queue: string; status: string; n: string }>(
      'SELECT queue, status, count(*) AS n FROM jobs GROUP BY queue, status',
    );
    for (const r of rows) this.set({ queue: r.queue, status: r.status }, Number(r.n));
  },
});

export function enableQueueDepthMetrics(db: Db): void {
  depthDb = db;
}

export function enableDefaultMetrics(): void {
  client.collectDefaultMetrics({ register: registry, prefix: 'taskrelay_process_' });
}

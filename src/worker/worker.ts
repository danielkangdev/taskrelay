import type { WorkerConfig } from '../config.js';
import type { Db } from '../db/pool.js';
import { SimulatedCrash, fault, shouldFault } from '../faults.js';
import { jobDuration, jobWait } from '../metrics.js';
import { claim } from '../queue/claim.js';
import { type FailOutcome, completeJob, failJob, heartbeat, releaseJob, staleHeartbeat } from '../queue/complete.js';
import type { ClaimedJob } from '../queue/types.js';
import type { HandlerRegistry } from './handlers/types.js';

export type JobOutcome = 'succeeded' | FailOutcome | 'crashed' | 'hung';

/** Optional instrumentation, used by experiments. */
export interface WorkerHooks {
  onClaim?(latencyMs: number, claimed: number): void;
  onJobDone?(job: ClaimedJob, outcome: JobOutcome, durationMs: number): void;
}

/**
 * Wakes idle workers. `gen` increments on every wake so a worker that checked the
 * queue, found it empty, and only then started waiting does not sleep through a
 * notification that arrived in between.
 */
export class Wakeup {
  gen = 0;
  private waiters = new Set<() => void>();

  wake(): void {
    this.gen++;
    for (const w of this.waiters) w();
    this.waiters.clear();
  }

  wait(ms: number, sinceGen: number): Promise<void> {
    if (this.gen !== sinceGen) return Promise.resolve();
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(t);
        this.waiters.delete(done);
        resolve();
      };
      const t = setTimeout(done, ms);
      this.waiters.add(done);
    });
  }
}

interface Held {
  job: ClaimedJob;
  abort: AbortController;
  /** Fault `hang_without_heartbeat`: excluded from heartbeats. */
  hung: boolean;
  /** Handler returned/threw; the completing UPDATE is in flight. */
  finishing: boolean;
}

/**
 * One worker loop: claim -> (heartbeat while held) -> execute -> complete/fail.
 * A worker holds at most `batchSize` jobs and runs them one after another.
 */
export class Worker {
  private held = new Map<string, Held>();
  private stopping = false;
  private crashed = false;
  private beating = false;
  private loop?: Promise<void>;

  constructor(
    private readonly db: Db,
    readonly id: string,
    private readonly cfg: WorkerConfig,
    private readonly handlers: HandlerRegistry,
    private readonly wakeup: Wakeup,
    private readonly hooks: WorkerHooks = {},
    /** 'exit' kills the process at a crash point (production); 'stop' only stops this worker (tests). */
    private readonly crashMode: 'exit' | 'stop' = 'exit',
  ) {}

  start(): void {
    this.loop ??= this.run();
  }

  /** Stop claiming. The in-flight job finishes; unstarted batched jobs are released. */
  stop(): void {
    this.stopping = true;
  }

  done(): Promise<void> {
    return this.loop ?? Promise.resolve();
  }

  get isCrashed(): boolean {
    return this.crashed;
  }

  /** Shutdown timed out: hand every held job back and abort its handler. */
  async releaseHeld(): Promise<void> {
    for (const h of [...this.held.values()]) {
      h.abort.abort(new Error('worker shutting down'));
      await releaseJob(this.db, h.job).catch((e) => console.error(`[${this.id}] release failed:`, e));
      this.held.delete(h.job.id);
    }
  }

  private async run(): Promise<void> {
    const hb = setInterval(() => void this.beat(), this.cfg.heartbeatMs);
    try {
      while (!this.stopping) {
        const gen = this.wakeup.gen;
        let jobs: ClaimedJob[];
        const t0 = performance.now();
        try {
          jobs = await claim(this.db, {
            queue: this.cfg.queue,
            workerId: this.id,
            leaseMs: this.cfg.leaseMs,
            limit: this.cfg.batchSize,
            strategy: this.cfg.claimStrategy,
          });
        } catch (err) {
          console.error(`[${this.id}] claim failed:`, (err as Error).message);
          // Back off on database errors; a wake-up (e.g. shutdown) still ends the wait early.
          await this.wakeup.wait(this.cfg.pollIntervalMs, this.wakeup.gen);
          continue;
        }
        this.hooks.onClaim?.(performance.now() - t0, jobs.length);
        if (jobs.length === 0) {
          await this.wakeup.wait(this.cfg.pollIntervalMs, gen);
          continue;
        }
        for (const job of jobs) this.held.set(job.id, { job, abort: new AbortController(), hung: false, finishing: false });
        for (const job of jobs) {
          if (this.stopping) {
            await releaseJob(this.db, job).catch(() => undefined);
            this.held.delete(job.id);
            continue;
          }
          await this.execute(job);
          if (this.crashed) return;
        }
      }
    } finally {
      clearInterval(hb);
    }
  }

  private async execute(job: ClaimedJob): Promise<void> {
    const h = this.held.get(job.id)!;
    const labels = { queue: job.queue, type: job.type };
    if (job.started_at) jobWait.observe(labels, Math.max(0, +job.started_at - +job.run_at) / 1000);
    const t0 = performance.now();
    let outcome: JobOutcome = 'succeeded';
    try {
      if (shouldFault('hang_without_heartbeat', job.attempts)) {
        h.hung = true;
        outcome = 'hung';
        await new Promise<void>((resolve) => h.abort.signal.addEventListener('abort', () => resolve()));
        return;
      }
      if (this.cfg.runLog) {
        await this.db.query(
          'INSERT INTO job_runs (job_id, worker_id, attempt, lease_token) VALUES ($1, $2, $3, $4)',
          [job.id, this.id, job.attempts, job.lease_token],
        );
      }
      fault('handler_throws', job.attempts);
      const handler = this.handlers[job.type];
      if (!handler) throw new Error(`no handler registered for job type "${job.type}"`);
      await handler({
        job,
        db: this.db,
        workerId: this.id,
        signal: h.abort.signal,
        fault: (point) => fault(point, job.attempts),
      });
      fault('crash_before_complete', job.attempts);
      h.finishing = true;
      outcome = (await completeJob(this.db, job)) ? 'succeeded' : 'stale';
    } catch (err) {
      if (err instanceof SimulatedCrash) {
        outcome = 'crashed';
        this.crash(err);
        return;
      }
      h.finishing = true;
      try {
        outcome = await failJob(this.db, job, err, this.cfg.backoff);
      } catch (dbErr) {
        // Could not record the failure; the lease will expire and the reaper requeues it.
        console.error(`[${this.id}] failed to record failure of ${job.id}:`, (dbErr as Error).message);
        outcome = 'stale';
      }
    } finally {
      const ms = performance.now() - t0;
      jobDuration.observe({ ...labels, outcome }, ms / 1000);
      this.hooks.onJobDone?.(job, outcome, ms);
      if (!this.crashed) this.held.delete(job.id);
    }
  }

  private crash(err: SimulatedCrash): void {
    console.error(`[${this.id}] ${err.message}`);
    if (this.crashMode === 'exit') process.kill(process.pid, 'SIGKILL');
    // In-process simulation: stop everything, write nothing more. Held leases
    // are left to expire, exactly as if the process had died.
    this.crashed = true;
    this.stopping = true;
  }

  private async beat(): Promise<void> {
    if (this.beating || this.crashed) return;
    this.beating = true;
    try {
      const live = [...this.held.values()].filter((h) => !h.hung && !h.finishing);
      const owned = await heartbeat(
        this.db,
        live.map((h) => h.job),
        this.cfg.leaseMs,
      );
      for (const h of live) {
        // A job that started finishing while the heartbeat was in flight is no
        // longer `running`; that is not a lost lease.
        if (owned.has(h.job.id) || h.finishing || !this.held.has(h.job.id)) continue;
        staleHeartbeat(h.job);
        h.abort.abort(new Error('lease lost'));
      }
    } catch (err) {
      console.error(`[${this.id}] heartbeat failed:`, (err as Error).message);
    } finally {
      this.beating = false;
    }
  }
}

import { randomBytes } from 'node:crypto';
import os from 'node:os';
import type pg from 'pg';
import type { WorkerConfig } from '../config.js';
import type { Db } from '../db/pool.js';
import { reapExpired } from '../queue/reaper.js';
import { notifyChannel } from '../queue/types.js';
import type { HandlerRegistry } from './handlers/types.js';
import { Wakeup, Worker, type WorkerHooks } from './worker.js';

export interface PoolOptions {
  idPrefix?: string;
  hooks?: WorkerHooks;
  crashMode?: 'exit' | 'stop';
  /** Run the lease reaper in this pool (default true). */
  reaper?: boolean;
}

/**
 * `concurrency` worker loops in one process sharing one LISTEN connection, one
 * reaper timer and one shutdown sequence (pattern from graphile-worker's runner:
 * a dedicated LISTEN client nudges idle workers; polling remains as a fallback
 * for missed notifications and for jobs whose run_at arrives later).
 */
export class WorkerPool {
  readonly workers: Worker[] = [];
  readonly wakeup = new Wakeup();
  private listener?: pg.PoolClient;
  private reaperTimer?: NodeJS.Timeout;
  private stopped = false;
  private stopping?: Promise<void>;

  constructor(
    private readonly db: Db,
    readonly cfg: WorkerConfig,
    handlers: HandlerRegistry,
    private readonly opts: PoolOptions = {},
  ) {
    // Boot id: a restarted container keeps its hostname and PID 1, so add randomness.
    const prefix = opts.idPrefix ?? `${os.hostname()}-${process.pid}-${randomBytes(2).toString('hex')}`;
    for (let i = 0; i < cfg.concurrency; i++) {
      this.workers.push(new Worker(db, `${prefix}-${i}`, cfg, handlers, this.wakeup, opts.hooks, opts.crashMode));
    }
  }

  async start(): Promise<void> {
    if (this.cfg.useNotify) await this.listen();
    for (const w of this.workers) w.start();
    if (this.opts.reaper !== false) {
      this.reaperTimer = setInterval(() => {
        reapExpired(this.db).catch((e) => console.error('[reaper]', (e as Error).message));
      }, this.cfg.reaperIntervalMs);
    }
  }

  /**
   * Graceful shutdown: stop claiming, let in-flight jobs finish, and if they do
   * not finish within shutdownTimeoutMs, release their leases so another worker
   * can pick them up immediately instead of waiting for expiry.
   */
  stop(): Promise<void> {
    this.stopping ??= this.doStop();
    return this.stopping;
  }

  private async doStop(): Promise<void> {
    this.stopped = true;
    clearInterval(this.reaperTimer);
    for (const w of this.workers) w.stop();
    this.wakeup.wake();
    const all = Promise.all(this.workers.map((w) => w.done()));
    let timer: NodeJS.Timeout | undefined;
    const timedOut = await Promise.race([
      all.then(() => false),
      new Promise<boolean>((r) => (timer = setTimeout(() => r(true), this.cfg.shutdownTimeoutMs))),
    ]);
    clearTimeout(timer);
    if (timedOut) {
      console.warn('[pool] shutdown timeout: releasing in-flight jobs');
      await Promise.all(this.workers.map((w) => w.releaseHeld()));
      // Handlers were aborted; give the loops a moment to observe it.
      await Promise.race([all, new Promise((r) => setTimeout(r, 2_000))]);
    }
    if (this.listener) {
      this.listener.release(true);
      this.listener = undefined;
    }
  }

  /** SIGTERM / SIGINT -> graceful stop -> exit. A second signal exits immediately. */
  installSignalHandlers(onStopped: () => Promise<void> = async () => undefined): void {
    let received = false;
    const handler = (sig: NodeJS.Signals) => {
      if (received) process.exit(130);
      received = true;
      console.log(`[pool] ${sig}: stopping (in-flight jobs will finish)`);
      void this.stop()
        .then(onStopped)
        .then(() => process.exit(0));
    };
    process.on('SIGTERM', handler);
    process.on('SIGINT', handler);
  }

  private async listen(): Promise<void> {
    try {
      const client = await this.db.connect();
      client.on('notification', () => this.wakeup.wake());
      client.on('error', (err) => {
        console.error('[pool] LISTEN connection lost:', err.message);
        client.release(true);
        this.listener = undefined;
        this.scheduleRelisten();
      });
      await client.query(`LISTEN ${client.escapeIdentifier(notifyChannel(this.cfg.queue))}`);
      this.listener = client;
    } catch (err) {
      console.error('[pool] LISTEN failed, polling only until reconnect:', (err as Error).message);
      this.scheduleRelisten();
    }
  }

  private scheduleRelisten(): void {
    if (this.stopped) return;
    setTimeout(() => {
      if (!this.stopped) void this.listen().then(() => this.wakeup.wake());
    }, 1_000);
  }
}

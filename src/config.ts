import { z } from 'zod';

export const CLAIM_STRATEGIES = ['skip_locked', 'for_update', 'naive'] as const;
export type ClaimStrategy = (typeof CLAIM_STRATEGIES)[number];

const int = (def: number) => z.coerce.number().int().nonnegative().default(def);
const bool = (def: boolean) =>
  z
    .enum(['0', '1', 'true', 'false'])
    .default(def ? '1' : '0')
    .transform((v) => v === '1' || v === 'true');

const EnvSchema = z.object({
  DATABASE_URL: z.string().default('postgres://taskrelay:taskrelay@localhost:5432/taskrelay'),
  PG_POOL_MAX: int(20),
  API_PORT: int(3000),
  API_HOST: z.string().default('0.0.0.0'),
  METRICS_PORT: int(9100), // worker processes serve /metrics here
  QUEUE: z.string().default('default'),
  WORKER_CONCURRENCY: int(4),
  WORKER_BATCH_SIZE: int(1),
  LEASE_MS: int(30_000),
  HEARTBEAT_MS: int(0), // 0 = LEASE_MS / 3
  POLL_INTERVAL_MS: int(1_000),
  REAPER_INTERVAL_MS: int(5_000),
  SHUTDOWN_TIMEOUT_MS: int(30_000),
  USE_NOTIFY: bool(true),
  RUN_LOG: bool(true),
  CLAIM_STRATEGY: z.enum(CLAIM_STRATEGIES).default('skip_locked'),
  BACKOFF_BASE_MS: int(1_000),
  BACKOFF_CAP_MS: int(300_000),
});

export interface WorkerConfig {
  queue: string;
  concurrency: number;
  batchSize: number;
  leaseMs: number;
  heartbeatMs: number;
  pollIntervalMs: number;
  reaperIntervalMs: number;
  shutdownTimeoutMs: number;
  useNotify: boolean;
  runLog: boolean;
  claimStrategy: ClaimStrategy;
  backoff: { baseMs: number; capMs: number };
}

export function loadEnv(env: NodeJS.ProcessEnv = process.env) {
  return EnvSchema.parse(env);
}

export function workerConfigFromEnv(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const e = loadEnv(env);
  return {
    queue: e.QUEUE,
    concurrency: Math.max(1, e.WORKER_CONCURRENCY),
    batchSize: Math.max(1, e.WORKER_BATCH_SIZE),
    leaseMs: e.LEASE_MS,
    heartbeatMs: e.HEARTBEAT_MS || Math.floor(e.LEASE_MS / 3),
    pollIntervalMs: e.POLL_INTERVAL_MS,
    reaperIntervalMs: e.REAPER_INTERVAL_MS,
    shutdownTimeoutMs: e.SHUTDOWN_TIMEOUT_MS,
    useNotify: e.USE_NOTIFY,
    runLog: e.RUN_LOG,
    claimStrategy: e.CLAIM_STRATEGY,
    backoff: { baseMs: e.BACKOFF_BASE_MS, capMs: e.BACKOFF_CAP_MS },
  };
}

export function defaultWorkerConfig(overrides: Partial<WorkerConfig> = {}): WorkerConfig {
  return { ...workerConfigFromEnv({}), ...overrides };
}

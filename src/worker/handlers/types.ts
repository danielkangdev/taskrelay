import type { Db } from '../../db/pool.js';
import type { ClaimedJob } from '../../queue/types.js';

export interface JobContext {
  job: ClaimedJob;
  db: Db;
  workerId: string;
  /** Aborted when this worker loses the job's lease. Long handlers should honour it. */
  signal: AbortSignal;
  /** Fault-injection hook (no-op unless TASKRELAY_FAULTS is set). */
  fault: (point: 'crash_after_write') => void;
}

export type Handler = (ctx: JobContext) => Promise<void>;
export type HandlerRegistry = Record<string, Handler>;

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const t = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(signal?.reason);
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export interface BackoffOptions {
  baseMs: number;
  capMs: number;
}

/**
 * Delay before the next attempt, after `attempts` attempts have failed (attempts >= 1).
 *
 * Exponential with a cap, plus "equal jitter": half the delay is fixed, the other
 * half is uniformly random. The fixed half guarantees the delay grows with every
 * attempt; the random half spreads out retries of jobs that failed together.
 * Same shape as pg-boss's retryBackoff, computed in TypeScript so it is unit-testable.
 */
export function backoffMs(attempts: number, opts: BackoffOptions, rand: () => number = Math.random): number {
  const exp = opts.baseMs * 2 ** Math.max(0, attempts - 1);
  const capped = Math.min(exp, opts.capMs);
  return Math.round(capped / 2 + (capped / 2) * rand());
}

/** Bounds of backoffMs for a given attempt, for tests and docs. */
export function backoffRange(attempts: number, opts: BackoffOptions): [number, number] {
  const capped = Math.min(opts.baseMs * 2 ** Math.max(0, attempts - 1), opts.capMs);
  return [Math.round(capped / 2), Math.round(capped)];
}

import { describe, expect, it } from 'vitest';
import { parseEnqueue } from '../src/api/server.js';
import { defaultWorkerConfig, workerConfigFromEnv } from '../src/config.js';
import { InjectedFault, SimulatedCrash, fault, parseFaults, shouldFault } from '../src/faults.js';
import { backoffMs, backoffRange } from '../src/queue/backoff.js';
import type { Db } from '../src/db/pool.js';
import { Wakeup, Worker, jitteredPollMs } from '../src/worker/worker.js';

describe('backoff', () => {
  const opts = { baseMs: 1_000, capMs: 60_000 };

  it('doubles per attempt with equal jitter in [d/2, d]', () => {
    expect(backoffMs(1, opts, () => 0)).toBe(500);
    expect(backoffMs(1, opts, () => 1)).toBe(1_000);
    expect(backoffMs(2, opts, () => 0)).toBe(1_000);
    expect(backoffMs(3, opts, () => 1)).toBe(4_000);
    expect(backoffRange(4, opts)).toEqual([4_000, 8_000]);
  });

  it('never decreases between attempts, whatever the jitter', () => {
    for (let a = 1; a < 12; a++) {
      const [, hiPrev] = backoffRange(a, opts);
      const [loNext] = backoffRange(a + 1, opts);
      expect(loNext).toBeGreaterThanOrEqual(Math.min(hiPrev, opts.capMs / 2));
    }
  });

  it('is capped', () => {
    expect(backoffMs(30, opts, () => 1)).toBe(60_000);
    expect(backoffMs(30, opts, () => 0)).toBe(30_000);
  });

  it('stays within range for random jitter', () => {
    for (let i = 0; i < 1_000; i++) {
      const a = 1 + (i % 8);
      const [lo, hi] = backoffRange(a, opts);
      const d = backoffMs(a, opts);
      expect(d).toBeGreaterThanOrEqual(lo);
      expect(d).toBeLessThanOrEqual(hi);
    }
  });
});

describe('faults', () => {
  it('parses point, probability and attempt', () => {
    expect(parseFaults('crash_after_write@1, handler_throws:0.25')).toEqual([
      { point: 'crash_after_write', probability: 1, attempt: 1 },
      { point: 'handler_throws', probability: 0.25, attempt: undefined },
    ]);
    expect(() => parseFaults('not_a_point')).toThrow(/invalid/);
  });

  it('is inert unless TASKRELAY_FAULTS is set', () => {
    delete process.env.TASKRELAY_FAULTS;
    expect(shouldFault('handler_throws', 1)).toBe(false);
    expect(() => fault('crash_after_write', 1)).not.toThrow();
  });

  it('fires only on the configured attempt and throws the right error type', () => {
    process.env.TASKRELAY_FAULTS = 'crash_after_write@2,handler_throws';
    try {
      expect(() => fault('crash_after_write', 1)).not.toThrow();
      expect(() => fault('crash_after_write', 2)).toThrow(SimulatedCrash);
      expect(() => fault('handler_throws', 7)).toThrow(InjectedFault);
    } finally {
      delete process.env.TASKRELAY_FAULTS;
    }
  });
});

describe('config', () => {
  it('derives heartbeat from lease and validates strategy', () => {
    const c = workerConfigFromEnv({ LEASE_MS: '9000', CLAIM_STRATEGY: 'for_update', USE_NOTIFY: '0' });
    expect(c.heartbeatMs).toBe(3_000);
    expect(c.claimStrategy).toBe('for_update');
    expect(c.useNotify).toBe(false);
    expect(() => workerConfigFromEnv({ CLAIM_STRATEGY: 'yolo' })).toThrow();
    expect(defaultWorkerConfig({ concurrency: 3 }).concurrency).toBe(3);
  });
});

describe('API validation', () => {
  it('applies defaults', () => {
    const b = parseEnqueue({ type: 'bench' });
    expect(b).toMatchObject({ queue: 'default', priority: 0, max_attempts: 5, delay_ms: 0, payload: {} });
  });

  it('rejects bad input', () => {
    expect(() => parseEnqueue({})).toThrow();
    expect(() => parseEnqueue({ type: 'bench', queue: 'Bad Queue!' })).toThrow();
    expect(() => parseEnqueue({ type: 'bench', max_attempts: 0 })).toThrow();
    expect(() => parseEnqueue({ type: 'bench', payload: [1, 2] })).toThrow();
  });

  it('validates payloads of known job types', () => {
    expect(() => parseEnqueue({ type: 'report', payload: { tenant_id: 1, period: '2025-13' } })).toThrow(/YYYY-MM/);
    expect(() => parseEnqueue({ type: 'report', payload: { period: '2025-01' } })).toThrow();
    expect(parseEnqueue({ type: 'report', payload: { tenant_id: 1, period: '2025-01' } }).type).toBe('report');
  });
});

describe('Worker', () => {
  it('backs off instead of spinning when the database is down', async () => {
    let calls = 0;
    const deadDb = { query: async () => (calls++, Promise.reject(new Error('connection refused'))) } as unknown as Db;
    const cfg = defaultWorkerConfig({ pollIntervalMs: 100, heartbeatMs: 60_000 });
    const w = new Worker(deadDb, 'w', cfg, {}, new Wakeup(), {}, 'stop');
    const err = console.error;
    console.error = () => undefined;
    try {
      w.start();
      await new Promise((r) => setTimeout(r, 350));
      w.stop();
    } finally {
      console.error = err;
    }
    expect(calls).toBeGreaterThanOrEqual(2);
    expect(calls).toBeLessThanOrEqual(9); // 350 ms / (100 ms -50% jitter) + 1; a spin would be thousands
  });
});

describe('poll jitter', () => {
  it('spreads the interval over [0.5x, 1.5x] with the same mean', () => {
    expect(jitteredPollMs(1_000, () => 0)).toBe(500);
    expect(jitteredPollMs(1_000, () => 0.999999)).toBe(1_500);
    const xs = Array.from({ length: 20_000 }, () => jitteredPollMs(1_000));
    const mean = xs.reduce((a, b) => a + b, 0) / xs.length;
    expect(Math.min(...xs)).toBeGreaterThanOrEqual(500);
    expect(Math.max(...xs)).toBeLessThanOrEqual(1_500);
    expect(mean).toBeGreaterThan(980);
    expect(mean).toBeLessThan(1_020);
  });
});

describe('Wakeup', () => {
  it('does not sleep through a wake that happened before wait()', async () => {
    const w = new Wakeup();
    const gen = w.gen;
    w.wake(); // notification arrives between "queue empty" and "start waiting"
    const t0 = Date.now();
    await w.wait(5_000, gen);
    expect(Date.now() - t0).toBeLessThan(50);
  });

  it('wakes all waiters, or times out', async () => {
    const w = new Wakeup();
    const waits = [w.wait(5_000, w.gen), w.wait(5_000, w.gen)];
    w.wake();
    await Promise.all(waits);
    const t0 = Date.now();
    await w.wait(60, w.gen);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(50);
  });
});

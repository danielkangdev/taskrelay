import { afterAll, describe, expect, it } from 'vitest';
import { buildServer } from '../src/api/server.js';
import { useDb } from './helpers.js';

const t = useDb();
const app = buildServer(t.db);
afterAll(() => app.close());

const post = (body: unknown, key?: string) =>
  app.inject({ method: 'POST', url: '/jobs', payload: body as object, headers: key ? { 'idempotency-key': key } : {} });

describe('test 6: idempotent enqueue', () => {
  it('same Idempotency-Key twice -> same job id, one row', async () => {
    const body = { type: 'report', payload: { tenant_id: 1, period: '2025-01' } };
    const first = await post(body, 'order-42');
    const second = await post(body, 'order-42');
    expect(first.statusCode).toBe(201);
    expect(second.statusCode).toBe(200);
    expect(first.json().created).toBe(true);
    expect(second.json().created).toBe(false);
    expect(second.json().job.id).toBe(first.json().job.id);
    expect(await t.count('SELECT count(*) AS n FROM jobs')).toBe(1);
  });

  it('20 concurrent requests with one key create exactly one job', async () => {
    const res = await Promise.all(Array.from({ length: 20 }, () => post({ type: 'bench' }, 'burst')));
    expect(new Set(res.map((r) => r.json().job.id)).size).toBe(1);
    expect(res.filter((r) => r.statusCode === 201)).toHaveLength(1);
    expect(await t.count('SELECT count(*) AS n FROM jobs')).toBe(1);
  });

  it('keys are scoped per queue; no key means no dedup', async () => {
    await post({ type: 'bench', queue: 'a' }, 'k');
    await post({ type: 'bench', queue: 'b' }, 'k');
    await post({ type: 'bench' });
    await post({ type: 'bench' });
    expect(await t.count('SELECT count(*) AS n FROM jobs')).toBe(4);
  });
});

describe('API', () => {
  it('validates input with 400s', async () => {
    expect((await post({})).statusCode).toBe(400);
    expect((await post({ type: 'report', payload: { tenant_id: 1, period: 'jan' } })).statusCode).toBe(400);
    expect((await post({ type: 'bench', priority: 1.5 })).statusCode).toBe(400);
    expect((await app.inject({ method: 'GET', url: '/jobs/not-a-uuid' })).statusCode).toBe(400);
    expect((await app.inject({ method: 'GET', url: '/dead-letter?limit=0' })).statusCode).toBe(400);
  });

  it('GET /jobs/:id, queue stats, health, metrics', async () => {
    const created = (await post({ type: 'bench', delay_ms: 60_000 })).json().job;
    await post({ type: 'bench' });

    const got = await app.inject({ method: 'GET', url: `/jobs/${created.id}` });
    expect(got.json().job).toMatchObject({ id: created.id, status: 'queued', attempts: 0 });
    const missing = await app.inject({ method: 'GET', url: '/jobs/00000000-0000-4000-8000-000000000000' });
    expect(missing.statusCode).toBe(404);

    const stats = (await app.inject({ method: 'GET', url: '/queues/default/stats' })).json();
    expect(stats.counts).toEqual({ queued: 2, running: 0, succeeded: 0, dead: 0 });
    expect(stats.oldest_runnable_wait_s).toBeGreaterThanOrEqual(0);

    expect((await app.inject({ method: 'GET', url: '/health' })).json()).toEqual({ ok: true });

    const metrics = await app.inject({ method: 'GET', url: '/metrics' });
    expect(metrics.body).toMatch(/taskrelay_queue_depth\{queue="default",status="queued"\} 2/);
    expect(metrics.body).toMatch(/taskrelay_stale_completion_total/);
  });
});

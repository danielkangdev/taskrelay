import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import type { Db } from '../db/pool.js';
import { enableQueueDepthMetrics, registry } from '../metrics.js';
import { listDead, retryDead } from '../queue/complete.js';
import { enqueue } from '../queue/enqueue.js';
import type { Job } from '../queue/types.js';
import { ReportPayload } from '../worker/handlers/report.js';

const QueueName = z.string().regex(/^[a-z0-9_]{1,48}$/, 'queue must match [a-z0-9_]{1,48}');

export const EnqueueBody = z.object({
  queue: QueueName.default('default'),
  type: z.string().min(1).max(64),
  payload: z.record(z.string(), z.unknown()).default({}),
  priority: z.number().int().min(-1000).max(1000).default(0),
  max_attempts: z.number().int().min(1).max(100).default(5),
  delay_ms: z
    .number()
    .int()
    .min(0)
    .max(30 * 24 * 3600 * 1000)
    .default(0),
});

/** Payload schemas of known job types, checked at the door rather than in the worker. */
const PAYLOAD_SCHEMAS: Record<string, z.ZodType> = { report: ReportPayload };

export function parseEnqueue(raw: unknown) {
  const body = EnqueueBody.parse(raw ?? {});
  PAYLOAD_SCHEMAS[body.type]?.parse(body.payload);
  return body;
}

const IdParam = z.object({ id: z.uuid() });
const IdempotencyKey = z.string().min(1).max(200);
const DeadQuery = z.object({
  queue: QueueName.optional(),
  limit: z.coerce.number().int().min(1).max(500).default(50),
});

function present(job: Job) {
  return {
    id: job.id,
    queue: job.queue,
    type: job.type,
    status: job.status,
    payload: job.payload,
    priority: job.priority,
    attempts: job.attempts,
    max_attempts: job.max_attempts,
    run_at: job.run_at,
    locked_by: job.locked_by,
    lease_expires_at: job.lease_expires_at,
    last_error: job.last_error,
    idempotency_key: job.idempotency_key,
    created_at: job.created_at,
    started_at: job.started_at,
    finished_at: job.finished_at,
  };
}

export function buildServer(db: Db): FastifyInstance {
  const app = Fastify({ logger: false });
  enableQueueDepthMetrics(db);

  app.setErrorHandler((err, _req, reply) => {
    if (err instanceof z.ZodError) {
      return reply.code(400).send({ error: 'validation_failed', issues: err.issues });
    }
    const status = (err as { statusCode?: number }).statusCode ?? 500;
    if (status >= 500) console.error('[api]', err);
    return reply.code(status).send({ error: (err as Error).message });
  });

  app.get('/health', async () => {
    await db.query('SELECT 1');
    return { ok: true };
  });

  app.post('/jobs', async (req, reply) => {
    const body = parseEnqueue(req.body);
    const header = req.headers['idempotency-key'];
    const key = header === undefined ? undefined : IdempotencyKey.parse(Array.isArray(header) ? header[0] : header);
    const { job, created } = await enqueue(db, {
      queue: body.queue,
      type: body.type,
      payload: body.payload,
      priority: body.priority,
      maxAttempts: body.max_attempts,
      delayMs: body.delay_ms,
      idempotencyKey: key,
    });
    return reply.code(created ? 201 : 200).send({ created, job: present(job) });
  });

  app.get('/jobs/:id', async (req, reply) => {
    const { id } = IdParam.parse(req.params);
    const { rows } = await db.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
    if (!rows[0]) return reply.code(404).send({ error: 'not_found' });
    return { job: present(rows[0]) };
  });

  app.post('/jobs/:id/retry', async (req, reply) => {
    const { id } = IdParam.parse(req.params);
    const job = await retryDead(db, id);
    if (job) return { job: present(job) };
    const exists = await db.query<{ status: string }>('SELECT status FROM jobs WHERE id = $1', [id]);
    if (!exists.rows[0]) return reply.code(404).send({ error: 'not_found' });
    return reply.code(409).send({ error: 'not_dead', status: exists.rows[0].status });
  });

  app.get('/queues/:queue/stats', async (req) => {
    const { queue } = z.object({ queue: QueueName }).parse(req.params);
    const { rows } = await db.query<{ status: string; n: string; oldest_queued_s: number | null }>(
      `SELECT status, count(*) AS n,
              extract(epoch FROM now() - min(run_at) FILTER (WHERE status = 'queued' AND run_at <= now()))::float8
                AS oldest_queued_s
       FROM jobs WHERE queue = $1 GROUP BY status`,
      [queue],
    );
    const counts: Record<string, number> = { queued: 0, running: 0, succeeded: 0, dead: 0 };
    let oldest: number | null = null;
    for (const r of rows) {
      counts[r.status] = Number(r.n);
      if (r.oldest_queued_s !== null) oldest = r.oldest_queued_s;
    }
    return { queue, counts, oldest_runnable_wait_s: oldest };
  });

  app.get('/dead-letter', async (req) => {
    const q = DeadQuery.parse(req.query);
    const jobs = await listDead(db, q.queue, q.limit);
    return { count: jobs.length, jobs: jobs.map(present) };
  });

  app.get('/metrics', async (_req, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });

  return app;
}

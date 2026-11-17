-- 001_init: core job table, run log, business tables.

CREATE TABLE jobs (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  queue            text        NOT NULL DEFAULT 'default',
  type             text        NOT NULL,
  payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
  status           text        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
  priority         int         NOT NULL DEFAULT 0,
  attempts         int         NOT NULL DEFAULT 0,
  max_attempts     int         NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  run_at           timestamptz NOT NULL DEFAULT now(),
  locked_by        text,
  lease_expires_at timestamptz,
  -- Fencing token: regenerated on every claim. Every write a worker makes about
  -- a job it holds is conditioned on this token, so a worker whose lease was
  -- taken over can no longer change the job.
  lease_token      uuid,
  idempotency_key  text,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  CONSTRAINT running_has_lease CHECK (
    status <> 'running' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
  )
);

-- Claim path: WHERE queue = $1 AND status = 'queued' AND run_at <= now()
--             ORDER BY priority DESC, run_at, id
CREATE INDEX jobs_claim_idx ON jobs (queue, status, priority DESC, run_at, id);
-- Reaper path: WHERE status = 'running' AND lease_expires_at < now()
CREATE INDEX jobs_lease_idx ON jobs (status, lease_expires_at);
-- Enqueue idempotency. NULL keys never conflict.
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (queue, idempotency_key);

-- Append-only execution log: one row each time a worker starts running a job.
-- count(*) - count(DISTINCT job_id) = number of duplicate executions, which is
-- how tests and experiments measure at-least-once behaviour.
CREATE TABLE job_runs (
  id          bigserial PRIMARY KEY,
  job_id      uuid        NOT NULL,
  worker_id   text        NOT NULL,
  attempt     int         NOT NULL,
  lease_token uuid        NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX job_runs_job_idx ON job_runs (job_id);

-- Seed data the report handler aggregates over.
CREATE TABLE orders (
  id          bigserial PRIMARY KEY,
  tenant_id   int           NOT NULL,
  amount      numeric(12,2) NOT NULL,
  status      text          NOT NULL,
  created_at  timestamptz   NOT NULL
);
CREATE INDEX orders_tenant_time_idx ON orders (tenant_id, created_at);

-- Handler output. report_key is the business idempotency key: no matter how
-- many times a report job runs, the report exists at most once.
CREATE TABLE reports (
  id          bigserial PRIMARY KEY,
  job_id      uuid        NOT NULL,
  report_key  text        NOT NULL UNIQUE,
  tenant_id   int,
  period      text,
  payload     jsonb       NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

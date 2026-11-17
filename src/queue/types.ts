export type JobStatus = 'queued' | 'running' | 'succeeded' | 'dead';

export interface Job {
  id: string;
  queue: string;
  type: string;
  payload: Record<string, unknown>;
  status: JobStatus;
  priority: number;
  attempts: number;
  max_attempts: number;
  run_at: Date;
  locked_by: string | null;
  lease_expires_at: Date | null;
  lease_token: string | null;
  idempotency_key: string | null;
  last_error: string | null;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
}

/** A job as held by a worker: it is running and we know the lease token we hold. */
export type ClaimedJob = Job & { status: 'running'; lease_token: string };

export const notifyChannel = (queue: string) => `jobs_${queue}`;

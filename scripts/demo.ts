/**
 * npm run demo: start postgres + api + 4 workers in docker compose, enqueue
 * 1,000 report jobs through the HTTP API, wait for them, print queue stats and
 * a /metrics summary.
 */
import { execFileSync } from 'node:child_process';

const API = process.env.API_URL ?? 'http://localhost:3000';
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

console.log('> docker compose up -d --build --scale worker=4 api worker');
execFileSync('docker', ['compose', 'up', '-d', '--build', '--wait', '--scale', 'worker=4', 'api', 'worker'], {
  stdio: 'inherit',
});

for (let i = 0; ; i++) {
  try {
    if ((await fetch(`${API}/health`)).ok) break;
  } catch {
    /* not up yet */
  }
  if (i > 60) throw new Error('api did not become healthy');
  await sleep(500);
}

const t0 = Date.now();
const ids: string[] = [];
for (let i = 0; i < 1_000; i++) {
  const tenant = (i % 100) + 1;
  const period = `2025-${String((Math.floor(i / 100) % 12) + 1).padStart(2, '0')}`;
  const res = await fetch(`${API}/jobs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': `demo-${tenant}-${period}` },
    body: JSON.stringify({ type: 'report', payload: { tenant_id: tenant, period } }),
  });
  ids.push(((await res.json()) as { job: { id: string } }).job.id);
}
console.log(`enqueued ${ids.length} report jobs in ${Date.now() - t0} ms (re-running the demo dedups via Idempotency-Key)`);

for (;;) {
  const stats = (await (await fetch(`${API}/queues/default/stats`)).json()) as { counts: Record<string, number> };
  process.stdout.write(`\r${JSON.stringify(stats.counts)}   `);
  if (stats.counts.queued === 0 && stats.counts.running === 0) break;
  await sleep(250);
}
console.log(`\nall done in ${Date.now() - t0} ms\n`);

console.log('queue stats:', await (await fetch(`${API}/queues/default/stats`)).json());

const show = (title: string, text: string, keep: RegExp) => {
  console.log(`\n${title}`);
  for (const line of text.split('\n')) if (keep.test(line)) console.log('  ' + line);
};
show('api /metrics (queue depth):', await (await fetch(`${API}/metrics`)).text(), /^taskrelay_queue_depth/);

// Job-level metrics live in each worker process (port 9100 inside its container).
const workerMetrics = execFileSync(
  'docker',
  ['compose', 'exec', '-T', '--index', '1', 'worker', 'node', '-e',
    "fetch('http://localhost:9100/metrics').then(r => r.text()).then(t => process.stdout.write(t))"],
  { encoding: 'utf8' },
);
show(
  'worker #1 /metrics (job counters, histogram summaries):',
  workerMetrics,
  /^taskrelay_(jobs_finished_total|stale_completion_total|lease_expired_total|job_(wait|duration)_seconds_(sum|count))/,
);

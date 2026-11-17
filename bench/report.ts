/**
 * Builds the README results section from the newest bench/results/*.json of
 * each experiment, replacing everything between the results markers.
 * Usage: npm run bench:report
 */
import { readdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

const DIR = path.resolve('bench/results');
const START = '<!-- results:start -->';
const END = '<!-- results:end -->';

/* eslint-disable @typescript-eslint/no-explicit-any */
async function latest(prefix: string): Promise<any | undefined> {
  const files = (await readdir(DIR)).filter((f) => new RegExp(`^${prefix}_\\d{4}-\\d{2}-\\d{2}\\.json$`).test(f)).sort();
  const f = files.at(-1);
  return f ? { file: f, ...JSON.parse(await readFile(path.join(DIR, f), 'utf8')) } : undefined;
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m]! : (s[m - 1]! + s[m]!) / 2;
};
const fmt = (n: number) => n.toLocaleString('en-US');
const table = (head: string[], rows: (string | number)[][]) =>
  [`| ${head.join(' | ')} |`, `| ${head.map(() => '---').join(' | ')} |`, ...rows.map((r) => `| ${r.join(' | ')} |`)].join('\n');

function topWaits(activity: Record<string, number>, n = 2): string {
  return Object.entries(activity)
    .filter(([k]) => !k.startsWith('idle'))
    .slice(0, n)
    .map(([k, v]) => `${k} ${v}`)
    .join(', ');
}

function envLine(r: any): string {
  const e = r.environment;
  const pg = e.postgres;
  const c = e.client ?? {};
  const cpu = c.cpu_cores ? `${c.cpu_cores} vCPU / ${c.mem_gb} GB visible to containers` : '';
  return `_Source: \`bench/results/${r.file}\`. PostgreSQL ${pg.version} in Docker (no CPU limit, synchronous_commit=${pg.settings.synchronous_commit}), ${cpu}${c.cpu_model ? `, ${c.cpu_model}` : ''}; ${e.host_note}_`;
}

const out: string[] = [];

const e1 = await latest('e1_throughput');
const e1a = await latest('e1_throughput_async_commit');
const e1w = await latest('e1_throughput_wide'); // follow-up run with 32/64 workers
if (e1 && e1w) {
  e1.runs.push(...e1w.runs);
  e1.runs.sort((a: any, b: any) => a.batch - b.batch || a.workers - b.workers);
  e1.file = `${e1.file}\` + \`bench/results/${e1w.file}`;
}
if (e1) {
  out.push('### E1 — throughput vs workers (100,000 jobs)\n');
  for (const batch of [...new Set<number>(e1.runs.map((r: any) => r.batch))]) {
    const rows = e1.runs
      .filter((r: any) => r.batch === batch)
      .map((r: any) => {
        const asyncRun = e1a?.runs.find((a: any) => a.batch === batch && a.workers === r.workers);
        return [
          r.workers,
          `**${fmt(r.throughput)}**`,
          r.seconds,
          `${r.exec_ms.p50} / ${r.exec_ms.p95}`,
          r.db_cpu_cores,
          r.client_cpu_cores,
          topWaits(r.backends_by_state),
          asyncRun ? fmt(asyncRun.throughput) : '—',
        ];
      });
    out.push(`**Batch size ${batch}** (jobs claimed per claim query)\n`);
    out.push(
      table(
        ['workers', 'jobs/s', 'total s', 'exec p50 / p95 ms', 'DB CPU (cores)', 'client CPU (cores)', 'avg backends in top wait states', 'jobs/s, sync_commit=off'],
        rows,
      ) + '\n',
    );
  }
  out.push(envLine(e1) + '\n');
}

const e2 = await latest('e2_claim_strategies');
const e2a = await latest('e2_claim_strategies_async_commit');
if (e2) {
  const reps = Math.max(...e2.runs.map((r: any) => r.rep));
  out.push(`### E2 — claim strategies (10,000 jobs, 8 workers; median of ${reps} runs)\n`);
  const rows = ['naive', 'for_update', 'skip_locked']
    .filter((s) => e2.runs.some((r: any) => r.strategy === s))
    .map((s) => {
      const rs = e2.runs.filter((r: any) => r.strategy === s);
      const as = e2a?.runs.filter((r: any) => r.strategy === s) ?? [];
      return [
        `\`${s}\``,
        `**${fmt(median(rs.map((r: any) => r.throughput)))}**`,
        `${fmt(median(rs.map((r: any) => r.duplicate_executions)))} (${rs.map((r: any) => fmt(r.duplicate_executions)).join(' / ')})`,
        fmt(median(rs.map((r: any) => r.jobs_executed_more_than_once))),
        median(rs.map((r: any) => r.claim_ms.p95)),
        median(rs.map((r: any) => r.avg_backends_waiting_on_lock)),
        as.length ? fmt(median(as.map((r: any) => r.throughput))) : '—',
      ];
    });
  out.push(
    table(
      ['strategy', 'jobs/s', 'duplicate executions (all runs)', 'jobs run >1×', 'claim p95 ms', 'avg backends waiting on locks', 'jobs/s, sync_commit=off'],
      rows,
    ) + '\n',
  );
  out.push(envLine(e2) + '\n');
}

const e3 = await latest('e3_wakeup');
const e3old = await latest('e3_wakeup_nojitter'); // same experiment before poll jitter was added
if (e3) {
  out.push('### E3 — wake-up latency at low load (enqueue → start, poll interval 1 s)\n');
  const row = (r: any, mode: string) => [r.workers, mode, r.jobs, r.latency_ms.p50, r.latency_ms.p95, r.latency_ms.p99, r.latency_ms.max];
  const rows = e3.runs.map((r: any) => row(r, r.mode === 'notify' ? 'LISTEN/NOTIFY + jittered poll' : 'poll only, ±50% jitter'));
  for (const r of e3old?.runs.filter((x: any) => x.mode === 'poll') ?? []) rows.push(row(r, '_poll only, fixed interval (before jitter)_'));
  rows.sort((a: any, b: any) => a[0] - b[0]);
  out.push(table(['workers', 'mode', 'jobs', 'p50 ms', 'p95 ms', 'p99 ms', 'max ms'], rows) + '\n');
  out.push(envLine(e3) + (e3old ? ` _Fixed-interval rows: \`bench/results/${e3old.file}\`._` : '') + '\n');
}

const e4 = await latest('e4_chaos');
if (e4) {
  out.push('### E4 — chaos: SIGKILL random workers (10,000 report jobs, 8 worker containers)\n');
  out.push(
    table(
      ['metric', 'value'],
      [
        ['run time', `${e4.wall_seconds} s`],
        ['workers killed (`docker kill -s KILL`)', `${e4.kills} (${e4.kills_holding_a_job} while holding a job)`],
        ['jobs succeeded / dead / stuck running', `${fmt(e4.jobs.succeeded ?? 0)} / ${e4.jobs.dead ?? 0} / **${e4.jobs.stuck_running}**`],
        ['executions / distinct jobs', `${fmt(e4.executions.total)} / ${fmt(e4.executions.distinct_jobs)}`],
        ['duplicate executions (at-least-once)', `**${e4.executions.duplicates}** (${e4.executions.jobs_run_more_than_once} jobs)`],
        ['`reports` rows / succeeded jobs', `${fmt(e4.reports.rows)} / ${fmt(e4.reports.succeeded_jobs)}`],
        ['duplicate report rows', `**${e4.reports.dup_keys}**`],
        ['re-executions whose INSERT was absorbed by ON CONFLICT', e4.consistency.absorbed_writes],
        ['succeeded jobs missing a report', e4.consistency.succeeded_without_report],
      ],
    ) + '\n',
  );
  out.push(`_Source: \`bench/results/${e4.file}\`. ${e4.method}_\n`);
}

const readme = await readFile('README.md', 'utf8');
const s = readme.indexOf(START);
const e = readme.indexOf(END);
if (s < 0 || e < 0) throw new Error('README is missing the results markers');
await writeFile('README.md', readme.slice(0, s + START.length) + '\n\n' + out.join('\n') + '\n' + readme.slice(e));
console.log(`README results updated from ${[e1, e1a, e2, e2a, e3, e4].filter(Boolean).map((r) => r.file).join(', ')}`);

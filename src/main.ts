import { createServer } from 'node:http';
import { buildServer } from './api/server.js';
import { loadEnv, workerConfigFromEnv } from './config.js';
import { migrate } from './db/migrate.js';
import { createPool } from './db/pool.js';
import { seedOrders } from './db/seed.js';
import { enableDefaultMetrics, enableQueueDepthMetrics, registry } from './metrics.js';
import { defaultHandlers } from './worker/handlers/index.js';
import { WorkerPool } from './worker/pool.js';

async function main(role: string | undefined): Promise<void> {
  const env = loadEnv();
  const cfg = workerConfigFromEnv();
  // Worker loops + LISTEN connection + reaper + headroom.
  const db = createPool(env.DATABASE_URL, Math.max(env.PG_POOL_MAX, cfg.concurrency + 3));

  switch (role) {
    case 'migrate': {
      const applied = await migrate(db);
      console.log(applied.length ? `applied: ${applied.join(', ')}` : 'up to date');
      await db.end();
      return;
    }
    case 'seed': {
      await migrate(db);
      console.log(`seeded ${await seedOrders(db)} orders`);
      await db.end();
      return;
    }
    case 'api': {
      await migrate(db);
      await seedOrders(db);
      enableDefaultMetrics();
      const app = buildServer(db);
      await app.listen({ port: env.API_PORT, host: env.API_HOST });
      console.log(`[api] listening on :${env.API_PORT}`);
      const close = async () => {
        await app.close();
        await db.end();
        process.exit(0);
      };
      process.once('SIGTERM', close);
      process.once('SIGINT', close);
      return;
    }
    case 'worker': {
      await migrate(db);
      enableDefaultMetrics();
      enableQueueDepthMetrics(db);
      const pool = new WorkerPool(db, cfg, defaultHandlers);
      // Per-process job metrics (wait/duration histograms, retries, stale completions).
      const metricsServer = createServer((req, res) => {
        if (req.url !== '/metrics') return void res.writeHead(404).end();
        registry.metrics().then(
          (body) => res.writeHead(200, { 'content-type': registry.contentType }).end(body),
          (err: Error) => res.writeHead(500).end(err.message),
        );
      })
        .on('error', (err) => console.error(`[worker] metrics server disabled: ${err.message}`))
        .listen(env.METRICS_PORT);
      pool.installSignalHandlers(async () => {
        metricsServer.close();
        await db.end();
      });
      await pool.start();
      console.log(
        `[worker] ${cfg.concurrency} loops on queue "${cfg.queue}" (lease ${cfg.leaseMs} ms, ` +
          `notify ${cfg.useNotify ? 'on' : 'off'}, faults "${process.env.TASKRELAY_FAULTS ?? ''}")`,
      );
      return;
    }
    default:
      console.error('usage: main.ts <api|worker|migrate|seed>');
      process.exit(2);
  }
}

main(process.argv[2]).catch((err) => {
  console.error(err);
  process.exit(1);
});

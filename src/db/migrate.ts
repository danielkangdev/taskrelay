import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import type { Db } from './pool.js';
import { withTx } from './pool.js';

// Arbitrary constant: serialises concurrent migrators (api + N workers starting together).
const MIGRATION_LOCK = 72_616_001;

export async function migrate(db: Db, dir = path.resolve('migrations')): Promise<string[]> {
  const files = (await readdir(dir)).filter((f) => /^\d+_.+\.sql$/.test(f)).sort();
  return withTx(db, async (c) => {
    await c.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK]);
    await c.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
    const done = new Set(
      (await c.query<{ name: string }>('SELECT name FROM schema_migrations')).rows.map((r) => r.name),
    );
    const applied: string[] = [];
    for (const file of files) {
      if (done.has(file)) continue;
      await c.query(await readFile(path.join(dir, file), 'utf8'));
      await c.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
      applied.push(file);
    }
    return applied;
  });
}

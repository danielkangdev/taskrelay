import pg from 'pg';

export type Db = pg.Pool;
export type Queryable = pg.Pool | pg.PoolClient | pg.Client;

export function createPool(connectionString: string, max = 20): Db {
  const pool = new pg.Pool({ connectionString, max });
  // An idle client erroring (e.g. the server restarted) must not crash the process.
  pool.on('error', (err) => console.error('[pg] idle client error:', err.message));
  return pool;
}

export async function withTx<T>(db: Db, fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

const { createPool, ensureSchema, withRetry } = require('@saga/common');

const pool = createPool();

async function init() {
  await withRetry(() => ensureSchema(pool, 'sagas', [
    `CREATE TABLE IF NOT EXISTS sagas.events (
      id BIGSERIAL PRIMARY KEY,
      saga_id TEXT,
      type TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
  ]));
}

module.exports = { pool, init };

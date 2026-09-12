const { createPool, ensureSchema, withRetry } = require('@saga/common');

const pool = createPool();

async function init() {
  await withRetry(() => ensureSchema(pool, 'gateway', [
    `CREATE TABLE IF NOT EXISTS gateway.idempotency (
      key TEXT PRIMARY KEY,
      response JSONB NOT NULL,
      status_code INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
  ]));
}

module.exports = { pool, init };

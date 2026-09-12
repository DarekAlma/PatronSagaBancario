const { createPool, ensureSchema, withRetry } = require('@saga/common');

const pool = createPool();

async function init() {
  await withRetry(() => ensureSchema(pool, 'clearing', [
    `CREATE TABLE IF NOT EXISTS clearing.settlements (
      saga_id TEXT PRIMARY KEY,
      destination_account_id TEXT NOT NULL,
      amount_cents BIGINT NOT NULL,
      status TEXT NOT NULL,
      external_ref TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
    `CREATE TABLE IF NOT EXISTS clearing.idempotency (
      key TEXT PRIMARY KEY,
      response JSONB NOT NULL,
      status_code INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
  ]));
}

module.exports = { pool, init };

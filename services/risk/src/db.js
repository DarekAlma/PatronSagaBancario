const { createPool, ensureSchema, withRetry } = require('@saga/common');

const pool = createPool();

const SEED_LIMITS = [
  { account_id: 'ACC-001', daily_limit_cents: 300000 },
  { account_id: 'ACC-002', daily_limit_cents: 100000 },
  { account_id: 'ACC-003', daily_limit_cents: 1000000 },
];

async function init() {
  await withRetry(() => ensureSchema(pool, 'risk', [
    `CREATE TABLE IF NOT EXISTS risk.limits (
      account_id TEXT PRIMARY KEY,
      daily_limit_cents BIGINT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS risk.evaluations (
      saga_id TEXT PRIMARY KEY,
      origin_account_id TEXT NOT NULL,
      amount_cents BIGINT NOT NULL,
      approved BOOLEAN NOT NULL,
      reason TEXT,
      status TEXT NOT NULL DEFAULT 'ACTIVE',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
    `CREATE TABLE IF NOT EXISTS risk.idempotency (
      key TEXT PRIMARY KEY,
      response JSONB NOT NULL,
      status_code INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
  ]));

  for (const limit of SEED_LIMITS) {
    await pool.query(
      `INSERT INTO risk.limits (account_id, daily_limit_cents) VALUES ($1, $2)
       ON CONFLICT (account_id) DO NOTHING`,
      [limit.account_id, limit.daily_limit_cents],
    );
  }
}

module.exports = { pool, init };

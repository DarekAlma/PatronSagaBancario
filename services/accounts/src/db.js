const { createPool, ensureSchema, withRetry } = require('@saga/common');

const pool = createPool();

const SEED_ACCOUNTS = [
  { id: 'ACC-001', owner: 'Empresa Exportadora S.A.', balance_cents: 500000 },
  { id: 'ACC-002', owner: 'Juan Perez', balance_cents: 150000 },
  { id: 'ACC-003', owner: 'NovaBank Tesoreria', balance_cents: 2000000 },
];

async function init() {
  await withRetry(() => ensureSchema(pool, 'accounts', [
    `CREATE TABLE IF NOT EXISTS accounts.accounts (
      id TEXT PRIMARY KEY,
      owner TEXT NOT NULL,
      balance_cents BIGINT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS accounts.ledger (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL REFERENCES accounts.accounts(id),
      saga_id TEXT NOT NULL,
      type TEXT NOT NULL,
      amount_cents BIGINT NOT NULL,
      balance_after_cents BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
    `CREATE TABLE IF NOT EXISTS accounts.idempotency (
      key TEXT PRIMARY KEY,
      response JSONB NOT NULL,
      status_code INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`,
  ]));

  for (const acc of SEED_ACCOUNTS) {
    await pool.query(
      `INSERT INTO accounts.accounts (id, owner, balance_cents)
       VALUES ($1, $2, $3)
       ON CONFLICT (id) DO NOTHING`,
      [acc.id, acc.owner, acc.balance_cents],
    );
  }
}

module.exports = { pool, init };

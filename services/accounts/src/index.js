const express = require('express');
const cors = require('cors');
const { stepDelay, runIdempotent, reportSagaStatus, EVENT_TYPE, STEP_NAME } = require('@saga/common');
const { pool, init } = require('./db');

const PORT = Number(process.env.PORT || 4001);
const BRIDGE_URL = process.env.BRIDGE_URL || 'http://localhost:4040';
const EVENT_BUS_URL = process.env.EVENT_BUS_URL || 'http://localhost:4010';

const app = express();
app.use(cors());
app.use(express.json());

async function publishEvent(type, payload) {
  await fetch(`${EVENT_BUS_URL}/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, payload }),
  }).catch((err) => console.error('[event-bus] publish fallo', err.message));
}

// --- Logica de negocio real (usada tanto por Prefect como por las rutas internas) ---

async function doDebit(accountId, amountCents, sagaId) {
  return runIdempotent(pool, 'accounts.idempotency', `${sagaId}:debit`, async () => {
    await stepDelay();
    const { rows } = await pool.query('SELECT balance_cents FROM accounts.accounts WHERE id = $1 FOR UPDATE', [accountId]);
    if (rows.length === 0) {
      return { statusCode: 404, body: { error: 'CUENTA_NO_EXISTE' } };
    }
    const balance = Number(rows[0].balance_cents);
    if (balance < amountCents) {
      return { statusCode: 409, body: { error: 'FONDOS_INSUFICIENTES', balance_cents: balance } };
    }
    const newBalance = balance - amountCents;
    await pool.query('UPDATE accounts.accounts SET balance_cents = $2 WHERE id = $1', [accountId, newBalance]);
    await pool.query(
      `INSERT INTO accounts.ledger (id, account_id, saga_id, type, amount_cents, balance_after_cents)
       VALUES ($1, $2, $3, 'DEBITO', $4, $5)`,
      [`${sagaId}:debit`, accountId, sagaId, amountCents, newBalance],
    );
    return { statusCode: 200, body: { success: true, balance_cents: newBalance } };
  });
}

async function doCredit(accountId, amountCents, sagaId) {
  return runIdempotent(pool, 'accounts.idempotency', `${sagaId}:credit`, async () => {
    await stepDelay();
    const { rows } = await pool.query('SELECT balance_cents FROM accounts.accounts WHERE id = $1 FOR UPDATE', [accountId]);
    if (rows.length === 0) {
      return { statusCode: 404, body: { error: 'CUENTA_NO_EXISTE' } };
    }
    const newBalance = Number(rows[0].balance_cents) + amountCents;
    await pool.query('UPDATE accounts.accounts SET balance_cents = $2 WHERE id = $1', [accountId, newBalance]);
    await pool.query(
      `INSERT INTO accounts.ledger (id, account_id, saga_id, type, amount_cents, balance_after_cents)
       VALUES ($1, $2, $3, 'REVERSA', $4, $5)`,
      [`${sagaId}:credit`, accountId, sagaId, amountCents, newBalance],
    );
    return { statusCode: 200, body: { success: true, balance_cents: newBalance } };
  });
}

// --- Rutas internas (llamadas por el flujo de Prefect, orquestado o coreografiado) ---

app.post('/internal/accounts/:id/debit', async (req, res) => {
  const { sagaId, amountCents } = req.body;
  const result = await doDebit(req.params.id, Number(amountCents), sagaId);
  res.status(result.statusCode).json(result.body);
});

app.post('/internal/accounts/:id/credit', async (req, res) => {
  const { sagaId, amountCents } = req.body;
  const result = await doCredit(req.params.id, Number(amountCents), sagaId);
  res.status(result.statusCode).json(result.body);
});

// --- Consultas de lectura ---

app.get('/accounts', async (_req, res) => {
  const { rows } = await pool.query('SELECT id, owner, balance_cents FROM accounts.accounts ORDER BY id');
  res.json(rows);
});

app.get('/accounts/:id/ledger', async (req, res) => {
  const { rows } = await pool.query(
    'SELECT id, saga_id, type, amount_cents, balance_after_cents, created_at FROM accounts.ledger WHERE account_id = $1 ORDER BY created_at DESC LIMIT 50',
    [req.params.id],
  );
  res.json(rows);
});

// --- Reaccion a eventos de dominio (Saga Coreografiada) ---
// Este servicio no conoce a ningun orquestador: solo reacciona a eventos del bus.

app.post('/events/handle', async (req, res) => {
  const { type, payload } = req.body;
  try {
    if (type === EVENT_TYPE.TRANSFERENCIA_SOLICITADA) {
      const { sagaId, originAccountId, destinationAccountId, amountCents, chaos } = payload;
      const proxied = await fetch(`${BRIDGE_URL}/steps/debito-origen`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sagaId, accountId: originAccountId, amountCents, mode: 'choreography' }),
      }).then((r) => r.json());

      if (proxied.statusCode === 200) {
        await publishEvent(EVENT_TYPE.SALDO_DEBITADO, {
          sagaId, originAccountId, destinationAccountId, amountCents, chaos,
        });
      } else {
        await reportSagaStatus(BRIDGE_URL, sagaId, 'RECHAZADO_FONDOS', proxied.body);
        await publishEvent(EVENT_TYPE.DEBITO_RECHAZADO, {
          sagaId, originAccountId, destinationAccountId, amountCents, chaos, reason: 'FONDOS', detail: proxied.body,
        });
      }
    } else if (type === EVENT_TYPE.RIESGO_RECHAZADO || (type === EVENT_TYPE.TRANSFERENCIA_FALLIDA && payload.reason === 'RED')) {
      const { sagaId, originAccountId, amountCents } = payload;
      await fetch(`${BRIDGE_URL}/steps/compensar-debito`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sagaId, accountId: originAccountId, amountCents, mode: 'choreography' }),
      });
      await publishEvent(EVENT_TYPE.SALDO_RESTITUIDO, { sagaId, originAccountId, amountCents });
    }
    res.json({ received: true });
  } catch (err) {
    console.error('[accounts] error manejando evento', err);
    res.status(500).json({ error: 'ERROR_MANEJANDO_EVENTO', message: err.message });
  }
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'accounts' }));

init()
  .then(() => {
    app.listen(PORT, () => console.log(`[accounts] escuchando en :${PORT}`));
  })
  .catch((err) => {
    console.error('[accounts] fallo iniciando esquema', err);
    process.exit(1);
  });

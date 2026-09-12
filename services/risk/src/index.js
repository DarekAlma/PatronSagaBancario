const express = require('express');
const cors = require('cors');
const { stepDelay, runIdempotent, reportSagaStatus, EVENT_TYPE } = require('@saga/common');
const { pool, init } = require('./db');

const PORT = Number(process.env.PORT || 4002);
const BRIDGE_URL = process.env.BRIDGE_URL || 'http://localhost:4040';
const EVENT_BUS_URL = process.env.EVENT_BUS_URL || 'http://localhost:4010';
const DEFAULT_LIMIT_CENTS = 200000;

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

async function doEvaluate(sagaId, originAccountId, amountCents, forceFraud) {
  return runIdempotent(pool, 'risk.idempotency', `${sagaId}:evaluate`, async () => {
    await stepDelay();
    const { rows } = await pool.query('SELECT daily_limit_cents FROM risk.limits WHERE account_id = $1', [originAccountId]);
    const limit = rows.length ? Number(rows[0].daily_limit_cents) : DEFAULT_LIMIT_CENTS;
    const overLimit = amountCents > limit;
    const approved = !forceFraud && !overLimit;
    const reason = forceFraud ? 'FRAUDE_FORZADO' : overLimit ? 'LIMITE_DIARIO_EXCEDIDO' : null;

    await pool.query(
      `INSERT INTO risk.evaluations (saga_id, origin_account_id, amount_cents, approved, reason, status)
       VALUES ($1, $2, $3, $4, $5, 'ACTIVE')
       ON CONFLICT (saga_id) DO UPDATE SET approved = $4, reason = $5, updated_at = now()`,
      [sagaId, originAccountId, amountCents, approved, reason],
    );

    if (!approved) {
      return { statusCode: 422, body: { approved: false, reason } };
    }
    return { statusCode: 200, body: { approved: true } };
  });
}

async function doCancel(sagaId) {
  return runIdempotent(pool, 'risk.idempotency', `${sagaId}:cancel`, async () => {
    await stepDelay();
    await pool.query(
      `UPDATE risk.evaluations SET status = 'CANCELLED', updated_at = now() WHERE saga_id = $1`,
      [sagaId],
    );
    return { statusCode: 200, body: { cancelled: true } };
  });
}

app.post('/internal/risk/:sagaId/evaluate', async (req, res) => {
  const { originAccountId, amountCents, forceFraud } = req.body;
  const result = await doEvaluate(req.params.sagaId, originAccountId, Number(amountCents), Boolean(forceFraud));
  res.status(result.statusCode).json(result.body);
});

app.post('/internal/risk/:sagaId/cancel', async (req, res) => {
  const result = await doCancel(req.params.sagaId);
  res.status(result.statusCode).json(result.body);
});

app.get('/risk/:sagaId', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM risk.evaluations WHERE saga_id = $1', [req.params.sagaId]);
  res.json(rows[0] || null);
});

// --- Reaccion a eventos de dominio (Saga Coreografiada) ---

app.post('/events/handle', async (req, res) => {
  const { type, payload } = req.body;
  try {
    if (type === EVENT_TYPE.SALDO_DEBITADO) {
      const { sagaId, originAccountId, destinationAccountId, amountCents, chaos } = payload;
      const proxied = await fetch(`${BRIDGE_URL}/steps/validar-riesgo`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sagaId, originAccountId, amountCents, forceFraud: !!(chaos && chaos.forceFraud), mode: 'choreography',
        }),
      }).then((r) => r.json());

      if (proxied.statusCode === 200) {
        await publishEvent(EVENT_TYPE.RIESGO_APROBADO, { sagaId, originAccountId, destinationAccountId, amountCents, chaos });
      } else {
        await reportSagaStatus(BRIDGE_URL, sagaId, 'RECHAZADO_RIESGO', proxied.body);
        await publishEvent(EVENT_TYPE.RIESGO_RECHAZADO, {
          sagaId, originAccountId, destinationAccountId, amountCents, chaos, reason: proxied.body.reason,
        });
      }
    } else if (type === EVENT_TYPE.TRANSFERENCIA_FALLIDA && payload.reason === 'RED') {
      await fetch(`${BRIDGE_URL}/steps/compensar-riesgo`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sagaId: payload.sagaId, mode: 'choreography' }),
      });
      await publishEvent(EVENT_TYPE.RIESGO_ANULADO, { sagaId: payload.sagaId });
    }
    res.json({ received: true });
  } catch (err) {
    console.error('[risk] error manejando evento', err);
    res.status(500).json({ error: 'ERROR_MANEJANDO_EVENTO', message: err.message });
  }
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'risk' }));

init()
  .then(() => {
    app.listen(PORT, () => console.log(`[risk] escuchando en :${PORT}`));
  })
  .catch((err) => {
    console.error('[risk] fallo iniciando esquema', err);
    process.exit(1);
  });

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const { stepDelay, runIdempotent, reportSagaStatus, EVENT_TYPE } = require('@saga/common');
const { pool, init } = require('./db');

const PORT = Number(process.env.PORT || 4003);
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

async function doSettle(sagaId, destinationAccountId, amountCents, forceTimeout) {
  return runIdempotent(pool, 'clearing.idempotency', `${sagaId}:settle`, async () => {
    await stepDelay();
    if (forceTimeout) {
      await pool.query(
        `INSERT INTO clearing.settlements (saga_id, destination_account_id, amount_cents, status)
         VALUES ($1, $2, $3, 'FALLIDO')
         ON CONFLICT (saga_id) DO UPDATE SET status = 'FALLIDO'`,
        [sagaId, destinationAccountId, amountCents],
      );
      return { statusCode: 504, body: { error: 'RED_INTERBANCARIA_CAIDA' } };
    }
    const externalRef = `SWIFT-${crypto.randomUUID().slice(0, 8).toUpperCase()}`;
    await pool.query(
      `INSERT INTO clearing.settlements (saga_id, destination_account_id, amount_cents, status, external_ref)
       VALUES ($1, $2, $3, 'LIQUIDADO', $4)
       ON CONFLICT (saga_id) DO UPDATE SET status = 'LIQUIDADO', external_ref = $4`,
      [sagaId, destinationAccountId, amountCents, externalRef],
    );
    return { statusCode: 200, body: { success: true, external_ref: externalRef } };
  });
}

app.post('/internal/clearing/:sagaId/settle', async (req, res) => {
  const { destinationAccountId, amountCents, forceTimeout } = req.body;
  const result = await doSettle(req.params.sagaId, destinationAccountId, Number(amountCents), Boolean(forceTimeout));
  res.status(result.statusCode).json(result.body);
});

app.get('/clearing/:sagaId', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM clearing.settlements WHERE saga_id = $1', [req.params.sagaId]);
  res.json(rows[0] || null);
});

// --- Reaccion a eventos de dominio (Saga Coreografiada) ---

app.post('/events/handle', async (req, res) => {
  const { type, payload } = req.body;
  try {
    if (type === EVENT_TYPE.RIESGO_APROBADO) {
      const { sagaId, originAccountId, destinationAccountId, amountCents, chaos } = payload;
      const proxied = await fetch(`${BRIDGE_URL}/steps/liquidacion-interbancaria`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sagaId, destinationAccountId, amountCents, forceTimeout: !!(chaos && chaos.forceNetworkTimeout), mode: 'choreography',
        }),
      }).then((r) => r.json());

      if (proxied.statusCode === 200) {
        await reportSagaStatus(BRIDGE_URL, sagaId, 'CONFIRMADO', proxied.body);
        await publishEvent(EVENT_TYPE.TRANSFERENCIA_CONFIRMADA, { sagaId, externalRef: proxied.body.body?.external_ref });
      } else {
        await reportSagaStatus(BRIDGE_URL, sagaId, 'RECHAZADO_RED', proxied.body);
        await publishEvent(EVENT_TYPE.TRANSFERENCIA_FALLIDA, {
          sagaId, originAccountId, destinationAccountId, amountCents, chaos, reason: 'RED',
        });
      }
    }
    res.json({ received: true });
  } catch (err) {
    console.error('[clearing] error manejando evento', err);
    res.status(500).json({ error: 'ERROR_MANEJANDO_EVENTO', message: err.message });
  }
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'clearing' }));

init()
  .then(() => {
    app.listen(PORT, () => console.log(`[clearing] escuchando en :${PORT}`));
  })
  .catch((err) => {
    console.error('[clearing] fallo iniciando esquema', err);
    process.exit(1);
  });

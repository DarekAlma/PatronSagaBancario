const express = require('express');
const cors = require('cors');
const { newId, runIdempotent, SAGA_MODE, EVENT_TYPE } = require('@saga/common');
const { pool, init } = require('./db');

const PORT = Number(process.env.PORT || 4000);
const BRIDGE_URL = process.env.BRIDGE_URL || 'http://localhost:4040';
const EVENT_BUS_URL = process.env.EVENT_BUS_URL || 'http://localhost:4010';
const ACCOUNTS_URL = process.env.ACCOUNTS_URL || 'http://localhost:4001';

const app = express();
app.use(cors());
app.use(express.json());

// --- Punto de entrada centralizado para el frontend ---
// Emite el sagaId (idempotencia), y despacha hacia orquestacion (Prefect) o
// coreografia (Event Bus) segun el modo elegido por el usuario.
app.post('/api/transfers', async (req, res) => {
  const {
    originAccountId, destinationAccountId, amount, mode, chaos, idempotencyKey,
  } = req.body;

  if (!originAccountId || !destinationAccountId || !amount || amount <= 0) {
    return res.status(400).json({ error: 'DATOS_INVALIDOS' });
  }
  if (![SAGA_MODE.ORCHESTRATION, SAGA_MODE.CHOREOGRAPHY].includes(mode)) {
    return res.status(400).json({ error: 'MODO_INVALIDO' });
  }

  const amountCents = Math.round(Number(amount) * 100);
  const key = idempotencyKey || newId();

  const result = await runIdempotent(pool, 'gateway.idempotency', key, async () => {
    const sagaId = newId();

    await fetch(`${BRIDGE_URL}/sagas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sagaId, mode, originAccountId, destinationAccountId, amountCents, chaos: chaos || {},
      }),
    });

    if (mode === SAGA_MODE.ORCHESTRATION) {
      // Dispara el flow de Prefect (se ejecuta en segundo plano en el bridge).
      fetch(`${BRIDGE_URL}/orchestrate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sagaId, originAccountId, destinationAccountId, amountCents, chaos: chaos || {},
        }),
      }).catch((err) => console.error('[gateway] fallo disparando orquestacion', err.message));
    } else {
      // Publica el primer evento de dominio; nadie mas coordina desde aqui.
      fetch(`${EVENT_BUS_URL}/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: EVENT_TYPE.TRANSFERENCIA_SOLICITADA,
          payload: {
            sagaId, originAccountId, destinationAccountId, amountCents, chaos: chaos || {},
          },
        }),
      }).catch((err) => console.error('[gateway] fallo publicando evento inicial', err.message));
    }

    return { statusCode: 202, body: { sagaId, mode, status: 'ACEPTADA' } };
  });

  res.status(result.statusCode).json({ ...result.body, idempotencyKey: key, duplicado: result.cached });
});

app.get('/api/transfers/:sagaId', async (req, res) => {
  const upstream = await fetch(`${BRIDGE_URL}/sagas/${req.params.sagaId}`);
  const body = await upstream.json();
  res.status(upstream.status).json(body);
});

app.get('/api/transfers', async (_req, res) => {
  const upstream = await fetch(`${BRIDGE_URL}/sagas`);
  const body = await upstream.json();
  res.status(upstream.status).json(body);
});

app.get('/api/accounts', async (_req, res) => {
  const upstream = await fetch(`${ACCOUNTS_URL}/accounts`);
  const body = await upstream.json();
  res.status(upstream.status).json(body);
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'gateway' }));

init()
  .then(() => {
    app.listen(PORT, () => console.log(`[gateway] escuchando en :${PORT}`));
  })
  .catch((err) => {
    console.error('[gateway] fallo iniciando esquema', err);
    process.exit(1);
  });

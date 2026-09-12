const express = require('express');
const cors = require('cors');
const { EVENT_TYPE } = require('@saga/common');
const { pool, init } = require('./db');

const PORT = Number(process.env.PORT || 4010);
const ACCOUNTS_URL = process.env.ACCOUNTS_URL || 'http://localhost:4001';
const RISK_URL = process.env.RISK_URL || 'http://localhost:4002';
const CLEARING_URL = process.env.CLEARING_URL || 'http://localhost:4003';

// Mapa ESTATICO evento -> suscriptores. El bus no sabe nada de reglas de negocio,
// solo entrega mensajes: no existe aqui ningun coordinador central de la Saga.
const SUBSCRIPTIONS = {
  [EVENT_TYPE.TRANSFERENCIA_SOLICITADA]: [`${ACCOUNTS_URL}/events/handle`],
  [EVENT_TYPE.SALDO_DEBITADO]: [`${RISK_URL}/events/handle`],
  [EVENT_TYPE.RIESGO_APROBADO]: [`${CLEARING_URL}/events/handle`],
  [EVENT_TYPE.RIESGO_RECHAZADO]: [`${ACCOUNTS_URL}/events/handle`],
  [EVENT_TYPE.TRANSFERENCIA_FALLIDA]: [`${ACCOUNTS_URL}/events/handle`, `${RISK_URL}/events/handle`],
  [EVENT_TYPE.DEBITO_RECHAZADO]: [],
  [EVENT_TYPE.TRANSFERENCIA_CONFIRMADA]: [],
  [EVENT_TYPE.SALDO_RESTITUIDO]: [],
  [EVENT_TYPE.RIESGO_ANULADO]: [],
};

const app = express();
app.use(cors());
app.use(express.json());

async function dispatch(type, payload) {
  const targets = SUBSCRIPTIONS[type] || [];
  await Promise.allSettled(
    targets.map((url) => fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, payload }),
    }).then(async (r) => {
      if (!r.ok) {
        console.error(`[event-bus] ${url} respondio ${r.status} para ${type}`);
      }
    })),
  );
}

app.post('/events', async (req, res) => {
  const { type, payload } = req.body;
  if (!type || !payload || !payload.sagaId) {
    return res.status(400).json({ error: 'EVENTO_INVALIDO' });
  }

  await pool.query(
    'INSERT INTO sagas.events (saga_id, type, payload) VALUES ($1, $2, $3)',
    [payload.sagaId, type, payload],
  );

  res.json({ accepted: true, subscribers: (SUBSCRIPTIONS[type] || []).length });

  // Entrega asincrona: el publicador no espera a que toda la coreografia termine.
  dispatch(type, payload).catch((err) => console.error('[event-bus] dispatch fallo', err));
});

app.get('/events/:sagaId', async (req, res) => {
  const { rows } = await pool.query(
    'SELECT type, payload, created_at FROM sagas.events WHERE saga_id = $1 ORDER BY created_at ASC',
    [req.params.sagaId],
  );
  res.json(rows);
});

app.get('/health', (_req, res) => res.json({ ok: true, service: 'event-bus' }));

init()
  .then(() => {
    app.listen(PORT, () => console.log(`[event-bus] escuchando en :${PORT}`));
  })
  .catch((err) => {
    console.error('[event-bus] fallo iniciando esquema', err);
    process.exit(1);
  });

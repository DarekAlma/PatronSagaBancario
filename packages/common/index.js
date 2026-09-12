const crypto = require('crypto');

function newId() {
  return crypto.randomUUID();
}

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Simula el micro-delay observable de cada paso de la Saga (2 a 4 segundos por defecto).
async function stepDelay() {
  const min = Number(process.env.STEP_DELAY_MIN_MS || 2000);
  const max = Number(process.env.STEP_DELAY_MAX_MS || 4000);
  const ms = randomBetween(min, max);
  await new Promise((resolve) => setTimeout(resolve, ms));
  return ms;
}

const SAGA_MODE = {
  ORCHESTRATION: 'orchestration',
  CHOREOGRAPHY: 'choreography',
};

const SAGA_STATUS = {
  PENDING: 'PENDING',
  RUNNING: 'RUNNING',
  CONFIRMADO: 'CONFIRMADO',
  RECHAZADO_FONDOS: 'RECHAZADO_FONDOS',
  RECHAZADO_RIESGO: 'RECHAZADO_RIESGO',
  RECHAZADO_RED: 'RECHAZADO_RED',
};

const STEP_STATE = {
  RUNNING: 'RUNNING',
  SUCCESS: 'SUCCESS',
  FAILED: 'FAILED',
  COMPENSATING: 'COMPENSATING',
  COMPENSATED: 'COMPENSATED',
};

const STEP_NAME = {
  DEBITO_ORIGEN: 'DEBITO_ORIGEN',
  VALIDACION_RIESGO: 'VALIDACION_RIESGO',
  LIQUIDACION_INTERBANCARIA: 'LIQUIDACION_INTERBANCARIA',
  COMPENSACION_RIESGO: 'COMPENSACION_RIESGO',
  COMPENSACION_DEBITO: 'COMPENSACION_DEBITO',
};

const EVENT_TYPE = {
  TRANSFERENCIA_SOLICITADA: 'TransferenciaSolicitada',
  SALDO_DEBITADO: 'SaldoDebitado',
  DEBITO_RECHAZADO: 'DebitoRechazado',
  RIESGO_APROBADO: 'RiesgoAprobado',
  RIESGO_RECHAZADO: 'RiesgoRechazado',
  TRANSFERENCIA_CONFIRMADA: 'TransferenciaConfirmada',
  TRANSFERENCIA_FALLIDA: 'TransferenciaFallida',
  SALDO_RESTITUIDO: 'SaldoRestituido',
  RIESGO_ANULADO: 'RiesgoAnulado',
};

// El "bridge" (services/prefect-bridge, Python+FastAPI) es quien conserva el
// estado liviano de cada saga y quien ejecuta los pasos como tasks/flows de Prefect.
async function reportStep(bridgeUrl, { sagaId, step, state, detail }) {
  try {
    await fetch(`${bridgeUrl}/sagas/${sagaId}/steps`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ step, state, detail: detail || null }),
    });
  } catch (err) {
    console.error('[bridge] no se pudo reportar paso', err.message);
  }
}

async function reportSagaStatus(bridgeUrl, sagaId, status, meta) {
  try {
    await fetch(`${bridgeUrl}/sagas/${sagaId}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status, meta: meta || null }),
    });
  } catch (err) {
    console.error('[bridge] no se pudo reportar estado de saga', err.message);
  }
}

module.exports = {
  newId,
  randomBetween,
  stepDelay,
  SAGA_MODE,
  SAGA_STATUS,
  STEP_STATE,
  STEP_NAME,
  EVENT_TYPE,
  reportStep,
  reportSagaStatus,
  ...require('./db'),
};

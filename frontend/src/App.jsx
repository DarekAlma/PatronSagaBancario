import React, { useEffect, useRef, useState } from 'react';
import { getAccounts, postTransfer, getTransfer, listTransfers, PREFECT_URL } from './api.js';

const STEP_LABELS = {
  DEBITO_ORIGEN: 'Débito en cuenta origen',
  VALIDACION_RIESGO: 'Validación de riesgo / antifraude',
  LIQUIDACION_INTERBANCARIA: 'Liquidación interbancaria',
  COMPENSACION_RIESGO: 'Compensación: anular aprobación de riesgo',
  COMPENSACION_DEBITO: 'Compensación: reintegrar débito',
  ESTADO_SAGA: 'Estado general de la saga',
};

const STATE_CLASS = {
  RUNNING: 'badge running',
  SUCCESS: 'badge success',
  FAILED: 'badge failed',
  COMPENSATING: 'badge compensating',
  COMPENSATED: 'badge compensated',
};

const STATUS_LABELS = {
  PENDING: 'Pendiente',
  RUNNING: 'En ejecución',
  CONFIRMADO: 'Confirmado',
  RECHAZADO_FONDOS: 'Rechazado (fondos insuficientes)',
  RECHAZADO_RIESGO: 'Rechazado (riesgo / antifraude)',
  RECHAZADO_RED: 'Rechazado (red interbancaria caída)',
};

const ACTIVE_STATUSES = new Set(['PENDING', 'RUNNING']);

function centsToStr(cents) {
  return (Number(cents) / 100).toLocaleString('es-CO', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export default function App() {
  const [accounts, setAccounts] = useState([]);
  const [history, setHistory] = useState([]);
  const [form, setForm] = useState({
    originAccountId: '',
    destinationAccountId: 'IBAN-EXTERNO-998877',
    amount: '1000',
    mode: 'orchestration',
    forceFraud: false,
    forceNetworkTimeout: false,
  });
  const [currentSaga, setCurrentSaga] = useState(null);
  const [lastSubmission, setLastSubmission] = useState(null);
  const [error, setError] = useState('');
  const pollRef = useRef(null);

  async function refreshAccounts() {
    try {
      setAccounts(await getAccounts());
    } catch (err) {
      console.error(err);
    }
  }

  async function refreshHistory() {
    try {
      setHistory(await listTransfers());
    } catch (err) {
      console.error(err);
    }
  }

  useEffect(() => {
    refreshAccounts();
    refreshHistory();
  }, []);

  useEffect(() => {
    if (accounts.length && !form.originAccountId) {
      setForm((f) => ({ ...f, originAccountId: accounts[0].id }));
    }
  }, [accounts]);

  function stopPolling() {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }

  function startPolling(sagaId) {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const data = await getTransfer(sagaId);
        setCurrentSaga(data);
        if (!ACTIVE_STATUSES.has(data.status)) {
          stopPolling();
          refreshAccounts();
          refreshHistory();
        }
      } catch (err) {
        console.error(err);
      }
    }, 1000);
  }

  useEffect(() => () => stopPolling(), []);

  async function submitTransfer(e, reuseSubmission) {
    if (e) e.preventDefault();
    setError('');

    const payload = reuseSubmission ? reuseSubmission.payload : {
      originAccountId: form.originAccountId,
      destinationAccountId: form.destinationAccountId,
      amount: Number(form.amount),
      mode: form.mode,
      chaos: {
        forceFraud: form.forceFraud,
        forceNetworkTimeout: form.forceNetworkTimeout,
      },
    };
    const idempotencyKey = reuseSubmission ? reuseSubmission.idempotencyKey : undefined;

    try {
      const result = await postTransfer(idempotencyKey ? { ...payload, idempotencyKey } : payload);
      setLastSubmission({ payload, idempotencyKey: result.idempotencyKey });
      setCurrentSaga({ sagaId: result.sagaId, status: 'PENDING', steps: [], mode: result.mode, duplicado: result.duplicado });
      startPolling(result.sagaId);
    } catch (err) {
      setError(err.message);
    }
  }

  function resendLast() {
    if (!lastSubmission) return;
    submitTransfer(null, lastSubmission);
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>NovaBank International</h1>
        <p>Simulador del Patrón Saga · Orquestación vs. Coreografía · Observabilidad con Prefect</p>
      </header>

      <div className="grid">
        <section className="card">
          <h2>Nueva transferencia interbancaria</h2>
          <form onSubmit={submitTransfer} className="form">
            <label>
              Cuenta origen
              <select
                value={form.originAccountId}
                onChange={(e) => setForm({ ...form, originAccountId: e.target.value })}
              >
                {accounts.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.id} · {a.owner} · ${centsToStr(a.balance_cents)}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Cuenta destino (banco externo)
              <input
                type="text"
                value={form.destinationAccountId}
                onChange={(e) => setForm({ ...form, destinationAccountId: e.target.value })}
              />
            </label>

            <label>
              Importe (USD)
              <input
                type="number"
                min="1"
                step="0.01"
                value={form.amount}
                onChange={(e) => setForm({ ...form, amount: e.target.value })}
              />
            </label>

            <fieldset>
              <legend>Modalidad de la Saga</legend>
              <label className="radio">
                <input
                  type="radio"
                  name="mode"
                  checked={form.mode === 'orchestration'}
                  onChange={() => setForm({ ...form, mode: 'orchestration' })}
                />
                Orquestada (flow de Prefect coordina)
              </label>
              <label className="radio">
                <input
                  type="radio"
                  name="mode"
                  checked={form.mode === 'choreography'}
                  onChange={() => setForm({ ...form, mode: 'choreography' })}
                />
                Coreografiada (eventos, sin coordinador)
              </label>
            </fieldset>

            <fieldset>
              <legend>Simulador de caos</legend>
              <label className="switch">
                <input
                  type="checkbox"
                  checked={form.forceFraud}
                  onChange={(e) => setForm({ ...form, forceFraud: e.target.checked })}
                />
                Forzar rechazo de riesgo / antifraude (CP-03)
              </label>
              <label className="switch">
                <input
                  type="checkbox"
                  checked={form.forceNetworkTimeout}
                  onChange={(e) => setForm({ ...form, forceNetworkTimeout: e.target.checked })}
                />
                Forzar caída de red interbancaria (CP-04)
              </label>
              <p className="hint">
                CP-02 (fondos insuficientes): ingresa un importe mayor al saldo disponible.
              </p>
            </fieldset>

            <div className="actions">
              <button type="submit">Iniciar transferencia</button>
              <button type="button" disabled={!lastSubmission} onClick={resendLast}>
                Reenviar última operación (CP-05: idempotencia)
              </button>
            </div>
          </form>
          {error && <p className="error">{error}</p>}
        </section>

        <section className="card">
          <h2>Traza de la Saga en curso</h2>
          {!currentSaga && <p className="hint">Inicia una transferencia para ver el avance paso a paso.</p>}
          {currentSaga && (
            <div className="saga-detail">
              <div className="saga-summary">
                <span className="saga-id">Saga: {currentSaga.sagaId}</span>
                <span className={`status-pill status-${currentSaga.status}`}>
                  {STATUS_LABELS[currentSaga.status] || currentSaga.status}
                </span>
                {currentSaga.duplicado && <span className="badge compensated">Duplicado detectado (idempotencia)</span>}
              </div>
              <ol className="timeline">
                {(currentSaga.steps || [])
                  .filter((s) => s.step !== 'ESTADO_SAGA')
                  .map((s, idx) => (
                    <li key={idx}>
                      <span className={STATE_CLASS[s.state] || 'badge'}>{s.state}</span>
                      <span className="step-name">{STEP_LABELS[s.step] || s.step}</span>
                      <span className="step-time">{new Date(s.at).toLocaleTimeString()}</span>
                    </li>
                  ))}
                {(!currentSaga.steps || currentSaga.steps.length === 0) && (
                  <li className="hint">Esperando el primer paso...</li>
                )}
              </ol>
              <a className="prefect-link" href={PREFECT_URL} target="_blank" rel="noreferrer">
                Ver traza detallada en Prefect ↗ (filtra por el tag {currentSaga.sagaId})
              </a>
            </div>
          )}
        </section>
      </div>

      <div className="grid">
        <section className="card">
          <h2>Cuentas y saldos (Account &amp; Ledger)</h2>
          <button type="button" onClick={refreshAccounts}>Actualizar</button>
          <table>
            <thead>
              <tr><th>Cuenta</th><th>Titular</th><th>Saldo</th></tr>
            </thead>
            <tbody>
              {accounts.map((a) => (
                <tr key={a.id}>
                  <td>{a.id}</td>
                  <td>{a.owner}</td>
                  <td>${centsToStr(a.balance_cents)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="card">
          <h2>Historial de sagas</h2>
          <button type="button" onClick={refreshHistory}>Actualizar</button>
          <table>
            <thead>
              <tr><th>Saga</th><th>Modo</th><th>Monto</th><th>Estado</th></tr>
            </thead>
            <tbody>
              {history.map((h) => (
                <tr key={h.sagaId} className="clickable" onClick={() => { setCurrentSaga({ ...h, steps: [] }); getTransfer(h.sagaId).then(setCurrentSaga); }}>
                  <td>{h.sagaId.slice(0, 8)}…</td>
                  <td>{h.mode === 'orchestration' ? 'Orquestada' : 'Coreografiada'}</td>
                  <td>${centsToStr(h.amountCents)}</td>
                  <td>{STATUS_LABELS[h.status] || h.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:4000';
export const PREFECT_URL = import.meta.env.VITE_PREFECT_URL || 'http://localhost:4200';

async function json(res) {
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(body.error || `HTTP ${res.status}`);
    err.body = body;
    throw err;
  }
  return body;
}

export function getAccounts() {
  return fetch(`${API_URL}/api/accounts`).then(json);
}

export function postTransfer(payload) {
  return fetch(`${API_URL}/api/transfers`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then(json);
}

export function getTransfer(sagaId) {
  return fetch(`${API_URL}/api/transfers/${sagaId}`).then(json);
}

export function listTransfers() {
  return fetch(`${API_URL}/api/transfers`).then(json);
}

import os
import httpx
from prefect import task, get_run_logger

ACCOUNTS_URL = os.getenv("ACCOUNTS_URL", "http://localhost:4001")
RISK_URL = os.getenv("RISK_URL", "http://localhost:4002")
CLEARING_URL = os.getenv("CLEARING_URL", "http://localhost:4003")

TIMEOUT = httpx.Timeout(30.0)


class StepRejected(Exception):
    """Una llamada a un microservicio de dominio respondio con un rechazo de negocio
    (fondos insuficientes, riesgo, red caida). Se relanza para que Prefect marque
    la task como Failed y el flow decida la compensacion correspondiente."""

    def __init__(self, status_code: int, body: dict):
        super().__init__(f"Paso rechazado ({status_code}): {body}")
        self.status_code = status_code
        self.body = body


async def _post(url: str, json_body: dict, label: str) -> dict:
    logger = get_run_logger()
    logger.info(f"-> {label}: POST {url} {json_body}")
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(url, json=json_body)
    body = resp.json()
    logger.info(f"<- {label}: HTTP {resp.status_code} {body}")
    if resp.status_code >= 400:
        raise StepRejected(resp.status_code, body)
    return body


@task(name="Debitar cuenta origen", retries=0)
async def t_debitar_origen(origin_account_id: str, amount_cents: int, saga_id: str) -> dict:
    return await _post(
        f"{ACCOUNTS_URL}/internal/accounts/{origin_account_id}/debit",
        {"sagaId": saga_id, "amountCents": amount_cents},
        "Debito origen",
    )


@task(name="Validar riesgo y antifraude", retries=0)
async def t_validar_riesgo(origin_account_id: str, amount_cents: int, saga_id: str, force_fraud: bool) -> dict:
    return await _post(
        f"{RISK_URL}/internal/risk/{saga_id}/evaluate",
        {"originAccountId": origin_account_id, "amountCents": amount_cents, "forceFraud": force_fraud},
        "Validacion de riesgo",
    )


@task(name="Liquidar en pasarela interbancaria", retries=0)
async def t_liquidar_interbancaria(destination_account_id: str, amount_cents: int, saga_id: str, force_timeout: bool) -> dict:
    return await _post(
        f"{CLEARING_URL}/internal/clearing/{saga_id}/settle",
        {"destinationAccountId": destination_account_id, "amountCents": amount_cents, "forceTimeout": force_timeout},
        "Liquidacion interbancaria",
    )


@task(name="Compensar: anular aprobacion de riesgo", retries=0)
async def t_compensar_riesgo(saga_id: str) -> dict:
    return await _post(
        f"{RISK_URL}/internal/risk/{saga_id}/cancel",
        {},
        "Compensacion de riesgo",
    )


@task(name="Compensar: reintegrar debito", retries=0)
async def t_compensar_debito(origin_account_id: str, amount_cents: int, saga_id: str) -> dict:
    return await _post(
        f"{ACCOUNTS_URL}/internal/accounts/{origin_account_id}/credit",
        {"sagaId": saga_id, "amountCents": amount_cents},
        "Compensacion de debito",
    )

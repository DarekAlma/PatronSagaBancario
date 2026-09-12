from prefect import flow, get_run_logger

from app import store
from app.tasks import (
    StepRejected,
    t_debitar_origen,
    t_validar_riesgo,
    t_liquidar_interbancaria,
    t_compensar_riesgo,
    t_compensar_debito,
)

DEBITO_ORIGEN = "DEBITO_ORIGEN"
VALIDACION_RIESGO = "VALIDACION_RIESGO"
LIQUIDACION_INTERBANCARIA = "LIQUIDACION_INTERBANCARIA"
COMPENSACION_RIESGO = "COMPENSACION_RIESGO"
COMPENSACION_DEBITO = "COMPENSACION_DEBITO"


# ============================================================================
# SAGA ORQUESTADA: un unico flow de Prefect es el coordinador central.
# El orquestador llama explicitamente a cada servicio y, ante un fallo,
# dispara las compensaciones en orden estrictamente inverso.
# ============================================================================
@flow(name="Saga Orquestada - Transferencia Bancaria", log_prints=True)
async def saga_orquestada_flow(saga_id: str, origin_account_id: str, destination_account_id: str,
                                amount_cents: int, chaos: dict):
    logger = get_run_logger()
    chaos = chaos or {}
    await store.set_status(saga_id, "RUNNING")

    # Paso 1: Debito en cuenta origen
    await store.add_step(saga_id, DEBITO_ORIGEN, "RUNNING")
    try:
        body = await t_debitar_origen(origin_account_id, amount_cents, saga_id)
        await store.add_step(saga_id, DEBITO_ORIGEN, "SUCCESS", body)
    except StepRejected as err:
        logger.warning(f"Fondos insuficientes, no hay compensacion que ejecutar: {err.body}")
        await store.add_step(saga_id, DEBITO_ORIGEN, "FAILED", err.body)
        await store.set_status(saga_id, "RECHAZADO_FONDOS", err.body)
        return

    # Paso 2: Validacion de riesgo / antifraude
    await store.add_step(saga_id, VALIDACION_RIESGO, "RUNNING")
    try:
        body = await t_validar_riesgo(origin_account_id, amount_cents, saga_id, bool(chaos.get("forceFraud")))
        await store.add_step(saga_id, VALIDACION_RIESGO, "SUCCESS", body)
    except StepRejected as err:
        await store.add_step(saga_id, VALIDACION_RIESGO, "FAILED", err.body)
        logger.warning("Riesgo rechazado -> compensando debito de origen")
        await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATING")
        comp = await t_compensar_debito(origin_account_id, amount_cents, saga_id)
        await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATED", comp)
        await store.set_status(saga_id, "RECHAZADO_RIESGO", err.body)
        return

    # Paso 3: Liquidacion en la pasarela interbancaria
    await store.add_step(saga_id, LIQUIDACION_INTERBANCARIA, "RUNNING")
    try:
        body = await t_liquidar_interbancaria(
            destination_account_id, amount_cents, saga_id, bool(chaos.get("forceNetworkTimeout")),
        )
        await store.add_step(saga_id, LIQUIDACION_INTERBANCARIA, "SUCCESS", body)
        await store.set_status(saga_id, "CONFIRMADO", body)
    except StepRejected as err:
        await store.add_step(saga_id, LIQUIDACION_INTERBANCARIA, "FAILED", err.body)
        logger.warning("Fallo de red interbancaria -> compensando en orden inverso (riesgo, luego debito)")

        await store.add_step(saga_id, COMPENSACION_RIESGO, "COMPENSATING")
        comp1 = await t_compensar_riesgo(saga_id)
        await store.add_step(saga_id, COMPENSACION_RIESGO, "COMPENSATED", comp1)

        await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATING")
        comp2 = await t_compensar_debito(origin_account_id, amount_cents, saga_id)
        await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATED", comp2)

        await store.set_status(saga_id, "RECHAZADO_RED", err.body)


# ============================================================================
# SAGA COREOGRAFIADA: no hay flow unico ni coordinador. Cada servicio de
# dominio, al reaccionar a un evento del bus, invoca uno de estos flows
# individuales solo para ejecutar SU paso via Prefect (trazabilidad),
# y es el propio servicio quien decide que evento de dominio publica despues.
# ============================================================================

async def _run_step(saga_id, step_name, coro):
    await store.add_step(saga_id, step_name, "RUNNING")
    try:
        body = await coro
        await store.add_step(saga_id, step_name, "SUCCESS", body)
        return {"statusCode": 200, "body": body}
    except StepRejected as err:
        await store.add_step(saga_id, step_name, "FAILED", err.body)
        return {"statusCode": err.status_code, "body": err.body}


@flow(name="Coreografia - Debito en cuenta origen")
async def choreo_debito_origen(saga_id: str, account_id: str, amount_cents: int):
    return await _run_step(saga_id, DEBITO_ORIGEN, t_debitar_origen(account_id, amount_cents, saga_id))


@flow(name="Coreografia - Validar riesgo")
async def choreo_validar_riesgo(saga_id: str, origin_account_id: str, amount_cents: int, force_fraud: bool):
    return await _run_step(
        saga_id, VALIDACION_RIESGO,
        t_validar_riesgo(origin_account_id, amount_cents, saga_id, force_fraud),
    )


@flow(name="Coreografia - Liquidacion interbancaria")
async def choreo_liquidar_interbancaria(saga_id: str, destination_account_id: str, amount_cents: int, force_timeout: bool):
    return await _run_step(
        saga_id, LIQUIDACION_INTERBANCARIA,
        t_liquidar_interbancaria(destination_account_id, amount_cents, saga_id, force_timeout),
    )


@flow(name="Coreografia - Compensar riesgo")
async def choreo_compensar_riesgo(saga_id: str):
    await store.add_step(saga_id, COMPENSACION_RIESGO, "COMPENSATING")
    body = await t_compensar_riesgo(saga_id)
    await store.add_step(saga_id, COMPENSACION_RIESGO, "COMPENSATED", body)
    return {"statusCode": 200, "body": body}


@flow(name="Coreografia - Compensar debito")
async def choreo_compensar_debito(saga_id: str, account_id: str, amount_cents: int):
    await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATING")
    body = await t_compensar_debito(account_id, amount_cents, saga_id)
    await store.add_step(saga_id, COMPENSACION_DEBITO, "COMPENSATED", body)
    return {"statusCode": 200, "body": body}

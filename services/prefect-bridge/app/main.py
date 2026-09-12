from fastapi import FastAPI, BackgroundTasks, HTTPException
from prefect import tags

from app import store, flows
from app.db import ensure_schema

app = FastAPI(title="Prefect Bridge - Saga Bancaria")


@app.on_event("startup")
async def startup():
    await ensure_schema()


@app.get("/health")
async def health():
    return {"ok": True, "service": "prefect-bridge"}


@app.post("/sagas")
async def create_saga(payload: dict):
    await store.create_saga(
        payload["sagaId"], payload["mode"], payload["originAccountId"],
        payload["destinationAccountId"], payload["amountCents"], payload.get("chaos") or {},
    )
    return {"ok": True}


@app.get("/sagas")
async def list_sagas():
    return await store.list_sagas()


@app.get("/sagas/{saga_id}")
async def get_saga(saga_id: str):
    data = await store.get_saga(saga_id)
    if not data:
        raise HTTPException(status_code=404, detail="SAGA_NO_ENCONTRADA")
    return data


@app.post("/sagas/{saga_id}/status")
async def update_status(saga_id: str, payload: dict):
    await store.set_status(saga_id, payload["status"], payload.get("meta"))
    return {"ok": True}


@app.post("/sagas/{saga_id}/steps")
async def append_step(saga_id: str, payload: dict):
    await store.add_step(saga_id, payload["step"], payload["state"], payload.get("detail"))
    return {"ok": True}


# --- Disparo de la Saga Orquestada (flow de Prefect en segundo plano) ---
@app.post("/orchestrate")
async def orchestrate(payload: dict, background_tasks: BackgroundTasks):
    saga_id = payload["sagaId"]

    async def run():
        with tags(saga_id, "orchestration"):
            await flows.saga_orquestada_flow(
                saga_id, payload["originAccountId"], payload["destinationAccountId"],
                payload["amountCents"], payload.get("chaos") or {},
            )

    background_tasks.add_task(run)
    return {"accepted": True, "sagaId": saga_id}


# --- Pasos individuales usados por la Saga Coreografiada ---
# Cada uno corre como su propio flow-run de Prefect (sin coordinador central).

@app.post("/steps/debito-origen")
async def step_debito_origen(payload: dict):
    saga_id = payload["sagaId"]
    with tags(saga_id, "choreography", "DEBITO_ORIGEN"):
        return await flows.choreo_debito_origen(saga_id, payload["accountId"], payload["amountCents"])


@app.post("/steps/validar-riesgo")
async def step_validar_riesgo(payload: dict):
    saga_id = payload["sagaId"]
    with tags(saga_id, "choreography", "VALIDACION_RIESGO"):
        return await flows.choreo_validar_riesgo(
            saga_id, payload["originAccountId"], payload["amountCents"], bool(payload.get("forceFraud")),
        )


@app.post("/steps/liquidacion-interbancaria")
async def step_liquidacion_interbancaria(payload: dict):
    saga_id = payload["sagaId"]
    with tags(saga_id, "choreography", "LIQUIDACION_INTERBANCARIA"):
        return await flows.choreo_liquidar_interbancaria(
            saga_id, payload["destinationAccountId"], payload["amountCents"], bool(payload.get("forceTimeout")),
        )


@app.post("/steps/compensar-riesgo")
async def step_compensar_riesgo(payload: dict):
    saga_id = payload["sagaId"]
    with tags(saga_id, "choreography", "COMPENSACION_RIESGO"):
        return await flows.choreo_compensar_riesgo(saga_id)


@app.post("/steps/compensar-debito")
async def step_compensar_debito(payload: dict):
    saga_id = payload["sagaId"]
    with tags(saga_id, "choreography", "COMPENSACION_DEBITO"):
        return await flows.choreo_compensar_debito(saga_id, payload["accountId"], payload["amountCents"])

from app.db import get_pool


async def create_saga(saga_id, mode, origin_account_id, destination_account_id, amount_cents, chaos):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO sagas.sagas (id, mode, origin_account_id, destination_account_id, amount_cents, chaos, status)
        VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
        ON CONFLICT (id) DO NOTHING
        """,
        saga_id, mode, origin_account_id, destination_account_id, amount_cents, chaos,
    )


async def set_status(saga_id, status, meta=None):
    pool = await get_pool()
    await pool.execute(
        "UPDATE sagas.sagas SET status = $2, updated_at = now() WHERE id = $1",
        saga_id, status,
    )
    await add_step(saga_id, "ESTADO_SAGA", status, meta)


async def add_step(saga_id, step, state, detail=None):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO sagas.steps (saga_id, step, state, detail) VALUES ($1, $2, $3, $4)",
        saga_id, step, state, detail,
    )


async def get_saga(saga_id):
    pool = await get_pool()
    saga_row = await pool.fetchrow("SELECT * FROM sagas.sagas WHERE id = $1", saga_id)
    if not saga_row:
        return None
    steps = await pool.fetch(
        "SELECT step, state, detail, at FROM sagas.steps WHERE saga_id = $1 ORDER BY at ASC",
        saga_id,
    )
    return {
        "sagaId": saga_row["id"],
        "mode": saga_row["mode"],
        "originAccountId": saga_row["origin_account_id"],
        "destinationAccountId": saga_row["destination_account_id"],
        "amountCents": saga_row["amount_cents"],
        "chaos": saga_row["chaos"],
        "status": saga_row["status"],
        "createdAt": saga_row["created_at"].isoformat(),
        "updatedAt": saga_row["updated_at"].isoformat(),
        "steps": [
            {
                "step": s["step"],
                "state": s["state"],
                "detail": s["detail"],
                "at": s["at"].isoformat(),
            }
            for s in steps
        ],
    }


async def list_sagas(limit=50):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT id, mode, origin_account_id, destination_account_id, amount_cents, status, created_at "
        "FROM sagas.sagas ORDER BY created_at DESC LIMIT $1",
        limit,
    )
    return [
        {
            "sagaId": r["id"],
            "mode": r["mode"],
            "originAccountId": r["origin_account_id"],
            "destinationAccountId": r["destination_account_id"],
            "amountCents": r["amount_cents"],
            "status": r["status"],
            "createdAt": r["created_at"].isoformat(),
        }
        for r in rows
    ]

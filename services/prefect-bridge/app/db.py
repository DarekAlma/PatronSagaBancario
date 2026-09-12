import os
import ssl
import json
import asyncpg

_pool: asyncpg.Pool | None = None


def _build_ssl():
    if os.getenv("PGSSL", "true").lower() == "false":
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = os.environ["DATABASE_URL"]
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            ssl=_build_ssl(),
            min_size=1,
            max_size=int(os.getenv("PG_POOL_MAX", "5")),
            init=_register_json_codec,
        )
    return _pool


async def _register_json_codec(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


async def ensure_schema():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS sagas")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sagas.sagas (
              id TEXT PRIMARY KEY,
              mode TEXT NOT NULL,
              origin_account_id TEXT NOT NULL,
              destination_account_id TEXT NOT NULL,
              amount_cents BIGINT NOT NULL,
              chaos JSONB NOT NULL DEFAULT '{}'::jsonb,
              status TEXT NOT NULL DEFAULT 'PENDING',
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sagas.steps (
              id BIGSERIAL PRIMARY KEY,
              saga_id TEXT NOT NULL,
              step TEXT NOT NULL,
              state TEXT NOT NULL,
              detail JSONB,
              at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

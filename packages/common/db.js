const { Pool } = require('pg');

// Todos los servicios comparten un unico proyecto Postgres (Supabase) pero
// cada uno vive en su propio esquema logico -> aislamiento de datos por servicio
// tal como permite el enunciado ("Database-per-Service o esquemas logicamente aislados").
function createPool() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('Falta la variable de entorno DATABASE_URL (cadena de conexion de Supabase/Postgres)');
  }
  const sslDisabled = process.env.PGSSL === 'false';
  return new Pool({
    connectionString,
    ssl: sslDisabled ? false : { rejectUnauthorized: false },
    max: Number(process.env.PG_POOL_MAX || 5),
  });
}

async function ensureSchema(pool, schema, statements) {
  await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  for (const statement of statements) {
    await pool.query(statement);
  }
}

async function withRetry(fn, { attempts = 5, delayMs = 1500 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      console.warn(`[db] intento ${i + 1}/${attempts} fallo: ${err.message}`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastErr;
}

// Patron "claim-then-execute": inserta primero una fila reservando la clave de
// idempotencia; si ya existia, devuelve la respuesta cacheada sin repetir el
// efecto (debitos/creditos duplicados ante reintentos - Caso de prueba CP-05).
async function runIdempotent(pool, table, key, worker) {
  const claim = await pool.query(
    `INSERT INTO ${table} (key, response, status_code) VALUES ($1, '{}'::jsonb, 0) ON CONFLICT (key) DO NOTHING`,
    [key],
  );
  if (claim.rowCount === 0) {
    const existing = await pool.query(`SELECT response, status_code FROM ${table} WHERE key = $1`, [key]);
    const row = existing.rows[0];
    return { cached: true, statusCode: row.status_code, body: row.response };
  }
  let result;
  try {
    result = await worker();
  } catch (err) {
    result = { statusCode: 500, body: { error: 'ERROR_INTERNO', message: err.message } };
  }
  await pool.query(`UPDATE ${table} SET response = $2, status_code = $3 WHERE key = $1`, [key, result.body, result.statusCode]);
  return { cached: false, statusCode: result.statusCode, body: result.body };
}

module.exports = { createPool, ensureSchema, withRetry, runIdempotent };

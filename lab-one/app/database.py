import asyncio

import asyncpg

from app.config import Settings


async def create_pool(settings: Settings) -> asyncpg.Pool:
    async def attempt():
        while True:
            try:
                return await asyncpg.create_pool(
                    host=settings.db_host,
                    port=settings.db_port,
                    user=settings.db_user,
                    password=settings.db_password,
                    database=settings.db_name,
                    min_size=settings.pool_min,
                    max_size=settings.pool_max,
                    timeout=2,
                )
            except (OSError, asyncpg.PostgresError, asyncio.TimeoutError):
                await asyncio.sleep(0.5)

    return await asyncio.wait_for(attempt(), timeout=settings.db_wait_seconds)


async def database_alive(pool: asyncpg.Pool, timeout: float = 1.0) -> bool:
    try:
        async with asyncio.timeout(timeout):
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
    except Exception:
        return False
    return True

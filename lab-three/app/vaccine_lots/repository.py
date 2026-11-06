import asyncpg

from app.vaccine_lots.exceptions import LotNumberTaken
from app.vaccine_lots.schemas import VaccineLotIn

COLUMNS = "id, lot_number, vaccine, doses, temperature_regime, manufactured_on, expires_on, created_at, updated_at"


class VaccineLotRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def get(self, lot_id: int) -> asyncpg.Record | None:
        return await self.pool.fetchrow(f"SELECT {COLUMNS} FROM vaccine_lots WHERE id = $1", lot_id)

    async def page(self, vaccine: str | None, limit: int, offset: int) -> tuple[list[asyncpg.Record], int]:
        where, params = ("WHERE vaccine = $1", [vaccine]) if vaccine is not None else ("", [])
        n = len(params)
        async with self.pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
            total = await conn.fetchval(f"SELECT count(*) FROM vaccine_lots {where}", *params)
            rows = await conn.fetch(
                f"SELECT {COLUMNS} FROM vaccine_lots {where} ORDER BY id LIMIT ${n + 1} OFFSET ${n + 2}",
                *params, limit, offset,
            )
        return rows, total

    async def create(self, lot: VaccineLotIn) -> asyncpg.Record:
        try:
            return await self.pool.fetchrow(
                f"INSERT INTO vaccine_lots (lot_number, vaccine, doses, temperature_regime, manufactured_on, expires_on) "
                f"VALUES ($1, $2, $3, $4, $5, $6) RETURNING {COLUMNS}",
                lot.lot_number, lot.vaccine, lot.doses, lot.temperature_regime, lot.manufactured_on, lot.expires_on,
            )
        except asyncpg.UniqueViolationError:
            raise LotNumberTaken(lot.lot_number) from None

    async def replace(self, lot_id: int, lot: VaccineLotIn) -> asyncpg.Record | None:
        try:
            return await self.pool.fetchrow(
                f"UPDATE vaccine_lots SET lot_number = $2, vaccine = $3, doses = $4, temperature_regime = $5, "
                f"manufactured_on = $6, expires_on = $7, updated_at = now() WHERE id = $1 RETURNING {COLUMNS}",
                lot_id, lot.lot_number, lot.vaccine, lot.doses, lot.temperature_regime,
                lot.manufactured_on, lot.expires_on,
            )
        except asyncpg.UniqueViolationError:
            raise LotNumberTaken(lot.lot_number) from None

    async def delete(self, lot_id: int) -> bool:
        return await self.pool.fetchval("DELETE FROM vaccine_lots WHERE id = $1 RETURNING id", lot_id) is not None

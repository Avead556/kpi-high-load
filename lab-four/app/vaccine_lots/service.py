from app.vaccine_lots.exceptions import LotNotFound
from app.vaccine_lots.repository import VaccineLotRepository
from app.vaccine_lots.schemas import VaccineLotIn, VaccineLotOut, VaccineLotPage


class VaccineLotService:
    def __init__(self, repository: VaccineLotRepository):
        self.repository = repository

    async def get(self, lot_id: int) -> VaccineLotOut:
        row = await self.repository.get(lot_id)
        if row is None:
            raise LotNotFound(lot_id)
        return VaccineLotOut.model_validate(dict(row))

    async def list(self, vaccine: str | None, limit: int, offset: int) -> VaccineLotPage:
        rows, total = await self.repository.page(vaccine, limit, offset)
        return VaccineLotPage(
            items=[VaccineLotOut.model_validate(dict(r)) for r in rows], total=total, limit=limit, offset=offset,
        )

    async def create(self, lot: VaccineLotIn) -> VaccineLotOut:
        return VaccineLotOut.model_validate(dict(await self.repository.create(lot)))

    async def replace(self, lot_id: int, lot: VaccineLotIn) -> VaccineLotOut:
        row = await self.repository.replace(lot_id, lot)
        if row is None:
            raise LotNotFound(lot_id)
        return VaccineLotOut.model_validate(dict(row))

    async def delete(self, lot_id: int) -> None:
        if not await self.repository.delete(lot_id):
            raise LotNotFound(lot_id)

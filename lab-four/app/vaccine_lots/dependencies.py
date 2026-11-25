from fastapi import Request

from app.vaccine_lots.repository import VaccineLotRepository
from app.vaccine_lots.service import VaccineLotService


async def lot_service(request: Request) -> VaccineLotService:
    return VaccineLotService(VaccineLotRepository(request.app.state.pool))

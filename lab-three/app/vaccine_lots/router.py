from fastapi import APIRouter, Depends, Path, Query, Response

from app.responses import as_json
from app.schemas import ErrorOut
from app.vaccine_lots.dependencies import lot_service
from app.vaccine_lots.schemas import DEFAULT_LIMIT, MAX_INT8, MAX_LIMIT, VaccineLotIn, VaccineLotOut, VaccineLotPage
from app.vaccine_lots.service import VaccineLotService

router = APIRouter(prefix="/vaccine-lots", tags=["vaccine-lots"])

LotId = Path(ge=1, le=MAX_INT8)
INVALID = {400: {"model": ErrorOut}}
NOT_FOUND = {404: {"model": ErrorOut}}
CONFLICT = {409: {"model": ErrorOut}}


@router.post("", status_code=201, response_model=VaccineLotOut, responses=INVALID | CONFLICT)
async def create_lot(body: VaccineLotIn, service: VaccineLotService = Depends(lot_service)):
    return as_json(await service.create(body), 201)


@router.get("", response_model=VaccineLotPage, responses=INVALID)
async def list_lots(
    vaccine: str | None = Query(default=None, min_length=1, max_length=100, pattern=r"^[^\x00]+$"),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0, le=MAX_INT8),
    service: VaccineLotService = Depends(lot_service),
):
    return as_json(await service.list(vaccine, limit, offset))


@router.get("/{lot_id}", response_model=VaccineLotOut, responses=INVALID | NOT_FOUND)
async def get_lot(lot_id: int = LotId, service: VaccineLotService = Depends(lot_service)):
    return as_json(await service.get(lot_id))


@router.put("/{lot_id}", response_model=VaccineLotOut, responses=INVALID | NOT_FOUND | CONFLICT)
async def replace_lot(body: VaccineLotIn, lot_id: int = LotId, service: VaccineLotService = Depends(lot_service)):
    return as_json(await service.replace(lot_id, body))


@router.delete("/{lot_id}", status_code=204, responses=INVALID | NOT_FOUND)
async def delete_lot(lot_id: int = LotId, service: VaccineLotService = Depends(lot_service)):
    await service.delete(lot_id)
    return Response(status_code=204)

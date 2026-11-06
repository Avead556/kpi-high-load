from fastapi import APIRouter, Request

from app.config import INSTANCE
from app.database import database_alive
from app.responses import as_json
from app.schemas import HealthOut

router = APIRouter(tags=["health"])


@router.get("/healthz", response_model=HealthOut, responses={503: {"model": HealthOut}})
async def healthz(request: Request):
    if not await database_alive(request.app.state.pool):
        return as_json(HealthOut(status="database unavailable"), 503)
    return as_json(HealthOut(status="ok", instance=INSTANCE))

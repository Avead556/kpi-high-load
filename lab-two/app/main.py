from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import health, metrics
from app.config import Settings
from app.database import create_pool
from app.vaccine_lots import router as vaccine_lots
from app.vaccine_lots.exceptions import LotNotFound, LotNumberTaken


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool(Settings.from_env())
    yield
    await app.state.pool.close()


app = FastAPI(title="vaccine-lots", lifespan=lifespan)
app.include_router(health.router)
app.include_router(metrics.router)
app.include_router(vaccine_lots.router)
app.add_middleware(metrics.MetricsMiddleware)


def validation_failed(details):
    return JSONResponse(status_code=400, content={"error": "validation failed", "details": details})


@app.exception_handler(RequestValidationError)
async def request_invalid(request, exc):
    details = []
    for err in exc.errors():
        loc = [str(part) for part in err["loc"] if part not in ("body", "query", "path")]
        details.append({"field": ".".join(loc) or "body", "message": err["msg"].removeprefix("Value error, ")})
    return validation_failed(details)


@app.exception_handler(asyncpg.DataError)
async def data_invalid(request, exc):
    return validation_failed([{"field": "body", "message": str(exc)}])


@app.exception_handler(LotNotFound)
async def lot_not_found(request, exc):
    return JSONResponse(status_code=404, content={"error": str(exc)})


@app.exception_handler(LotNumberTaken)
async def lot_number_taken(request, exc):
    return JSONResponse(status_code=409, content={"error": str(exc)})

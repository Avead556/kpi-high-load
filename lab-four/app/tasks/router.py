from fastapi import APIRouter, Path, Query, Request

from app.responses import as_json
from app.schemas import ErrorOut
from app.tasks.exceptions import TaskFinished, TaskNotFound
from app.tasks.repository import TaskRepository, decode_result
from app.tasks.schemas import TaskOut, TaskPage, TaskStatus
from app.vaccine_lots.exceptions import LotNotFound
from app.vaccine_lots.schemas import DEFAULT_LIMIT, MAX_INT8, MAX_LIMIT

router = APIRouter(tags=["tasks"])

INVALID = {400: {"model": ErrorOut}}
NOT_FOUND = {404: {"model": ErrorOut}}


def repository(request: Request) -> TaskRepository:
    return TaskRepository(request.app.state.pool)


@router.post("/vaccine-lots/{lot_id}/tasks", status_code=202, response_model=TaskOut, responses=INVALID | NOT_FOUND)
async def create_task(request: Request, lot_id: int = Path(ge=1, le=MAX_INT8)):
    row = await repository(request).create(lot_id)
    if row is None:
        raise LotNotFound(lot_id)
    return as_json(TaskOut.model_validate(dict(row)), 202)


@router.get("/tasks/{task_id}", response_model=TaskOut, responses=INVALID | NOT_FOUND)
async def get_task(request: Request, task_id: int = Path(ge=1, le=MAX_INT8)):
    task, result = await repository(request).get(task_id)
    if task is None:
        raise TaskNotFound(task_id)
    return as_json(TaskOut.model_validate({**dict(task), "result": decode_result(result)}))


@router.get("/tasks", response_model=TaskPage, responses=INVALID)
async def list_tasks(
    request: Request,
    status: TaskStatus | None = Query(default=None),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0, le=MAX_INT8),
):
    rows, total = await repository(request).page(status, limit, offset)
    return as_json(TaskPage(items=[TaskOut.model_validate(dict(r)) for r in rows], total=total, limit=limit,
                            offset=offset))


@router.delete("/tasks/{task_id}", response_model=TaskOut,
               responses=INVALID | NOT_FOUND | {202: {"model": TaskOut}, 409: {"model": ErrorOut}})
async def cancel_task(request: Request, task_id: int = Path(ge=1, le=MAX_INT8)):
    """queued: 200 and status cancelled; running: 202, the worker stops at the next pass boundary."""
    row, status = await repository(request).cancel(task_id)
    if row is None:
        if status is None:
            raise TaskNotFound(task_id)
        raise TaskFinished(task_id, status)
    return as_json(TaskOut.model_validate(dict(row)), 200 if row["status"] == "cancelled" else 202)

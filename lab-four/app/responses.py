from fastapi import Response
from pydantic import BaseModel


def as_json(model: BaseModel, status_code: int = 200) -> Response:
    return Response(model.model_dump_json(), status_code=status_code, media_type="application/json")

from pydantic import BaseModel


class ErrorDetail(BaseModel):
    field: str
    message: str


class ErrorOut(BaseModel):
    error: str
    details: list[ErrorDetail] | None = None


class HealthOut(BaseModel):
    status: str
    instance: str | None = None

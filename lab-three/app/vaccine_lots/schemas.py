from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

MAX_INT4 = 2**31 - 1
MAX_INT8 = 2**63 - 1
DEFAULT_LIMIT = 20
MAX_LIMIT = 100

TemperatureRegime = Literal["+2..+8", "-25..-15", "-90..-60"]


class VaccineLotIn(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    lot_number: str = Field(min_length=1, max_length=32)
    vaccine: str = Field(min_length=1, max_length=100)
    doses: StrictInt = Field(gt=0, le=MAX_INT4)
    temperature_regime: TemperatureRegime
    manufactured_on: date
    expires_on: date

    @field_validator("lot_number", "vaccine")
    @classmethod
    def no_nul(cls, value):
        if "\x00" in value:
            raise ValueError("must not contain NUL characters")
        return value

    @field_validator("expires_on")
    @classmethod
    def expiry_after_manufacture(cls, value, info):
        made = info.data.get("manufactured_on")
        if made is not None and value <= made:
            raise ValueError(f"must be later than manufactured_on ({made.isoformat()})")
        return value


class VaccineLotOut(BaseModel):
    id: int
    lot_number: str
    vaccine: str
    doses: int
    temperature_regime: TemperatureRegime
    manufactured_on: date
    expires_on: date
    created_at: datetime
    updated_at: datetime


class VaccineLotPage(BaseModel):
    items: list[VaccineLotOut]
    total: int
    limit: int
    offset: int

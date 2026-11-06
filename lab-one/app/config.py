import os
import socket
from dataclasses import dataclass

INSTANCE = socket.gethostname()


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    pool_min: int
    pool_max: int
    db_wait_seconds: float

    @classmethod
    def from_env(cls):
        return cls(
            db_host=os.environ["DB_HOST"],
            db_port=int(os.environ.get("DB_PORT", "5432")),
            db_user=os.environ["DB_USER"],
            db_password=os.environ["DB_PASSWORD"],
            db_name=os.environ["DB_NAME"],
            pool_min=int(os.environ.get("DB_POOL_MIN", "2")),
            pool_max=int(os.environ.get("DB_POOL_MAX", "10")),
            db_wait_seconds=float(os.environ.get("DB_WAIT_SECONDS", "60")),
        )

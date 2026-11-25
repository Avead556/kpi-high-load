import os
import socket
from dataclasses import dataclass

POD_NAME = os.environ.get("POD_NAME") or socket.gethostname()


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    pool_size: int
    db_wait_seconds: float

    @classmethod
    def from_env(cls):
        return cls(
            db_host=os.environ["DB_HOST"],
            db_port=int(os.environ.get("DB_PORT", "5432")),
            db_user=os.environ["DB_USER"],
            db_password=os.environ["DB_PASSWORD"],
            db_name=os.environ["DB_NAME"],
            pool_size=int(os.environ["DB_POOL_SIZE"]),
            db_wait_seconds=float(os.environ.get("DB_WAIT_SECONDS", "60")),
        )

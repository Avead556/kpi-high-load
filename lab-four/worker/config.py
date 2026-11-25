import os
import socket
from dataclasses import dataclass

CLAIM_MODES = ("skip_locked", "for_update", "naive")
STORE_MODES = ("idempotent", "naive")


def flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    worker_id: str
    dsn: str
    web_url: str
    lease_seconds: float
    poll_seconds: float
    fail_rate: float
    claim_mode: str
    store_mode: str
    heartbeat: bool
    reap: bool
    q_lo: float
    q_hi: float
    slice_size: int
    grace_seconds: float
    metrics_port: int

    @classmethod
    def from_env(cls):
        claim_mode = os.environ.get("CLAIM_MODE", "skip_locked")
        store_mode = os.environ.get("STORE_MODE", "idempotent")
        if claim_mode not in CLAIM_MODES or store_mode not in STORE_MODES:
            raise ValueError(f"CLAIM_MODE in {CLAIM_MODES}, STORE_MODE in {STORE_MODES}")
        dsn = (f"host={os.environ['DB_HOST']} port={os.environ.get('DB_PORT', '5432')} "
               f"user={os.environ['DB_USER']} password={os.environ['DB_PASSWORD']} dbname={os.environ['DB_NAME']} "
               f"connect_timeout=3 application_name=worker")
        return cls(
            worker_id=os.environ.get("WORKER_ID") or socket.gethostname(),
            dsn=dsn,
            web_url=os.environ.get("WEB_URL", "http://web.vaccines.svc.cluster.local"),
            lease_seconds=float(os.environ.get("LEASE_SECONDS", "10")),
            poll_seconds=float(os.environ.get("POLL_MS", "10")) / 1000,
            fail_rate=float(os.environ.get("FAIL_RATE", "0")),
            claim_mode=claim_mode,
            store_mode=store_mode,
            heartbeat=flag("HEARTBEAT", "1"),
            reap=flag("REAP", "1"),
            q_lo=float(os.environ.get("Q_LO", "0.01")),
            q_hi=float(os.environ.get("Q_HI", "0.99")),
            slice_size=int(os.environ.get("SLICE_SIZE", "256")),
            grace_seconds=float(os.environ.get("GRACE_SECONDS", "25")),
            metrics_port=int(os.environ.get("METRICS_PORT", "9100")),
        )

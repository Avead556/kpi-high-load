import time

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from app.config import POD_NAME

BUCKETS = (0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024)

REQUESTS = Counter("http_requests_total", "HTTP requests handled", ["method", "path", "code"])
DURATION = Histogram("http_request_duration_seconds", "Time spent handling a request", ["method", "path"],
                     buckets=BUCKETS)
IN_PROGRESS = Gauge("http_requests_in_progress", "Requests being handled right now")

router = APIRouter(tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware:
    def __init__(self, app):
        self.app = app
        self.pod_header = (b"x-pod", POD_NAME.encode())

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status = 500

        async def send_with_pod(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [*message.get("headers", []), self.pod_header]
            await send(message)

        IN_PROGRESS.inc()
        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_with_pod)
        finally:
            elapsed = time.perf_counter() - start
            IN_PROGRESS.dec()
            route = scope.get("route")
            path = route.path if route is not None else "<unmatched>"
            method = scope["method"]
            REQUESTS.labels(method, path, str(status)).inc()
            DURATION.labels(method, path).observe(elapsed)

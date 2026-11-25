from prometheus_client import Counter, Gauge, Histogram

BUCKETS = (0.0005, 0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048, 4.096)

TASKS = Counter("worker_tasks_total", "Tasks finished by this worker", ["result"])
CLAIMS = Counter("worker_claims_total", "Claim attempts", ["result"])
DURATION = Histogram("worker_task_duration_seconds", "Duration of a task step", ["step"], buckets=BUCKETS)
IN_FLIGHT = Gauge("worker_tasks_in_flight", "Tasks being processed right now")
STALE = Counter("worker_stale_updates_total", "Updates that matched zero rows because the token was stale", ["op"])
REAPED = Counter("worker_reaped_total", "Expired leases returned by this worker", ["to"])

for result in ("done", "failed", "lost", "cancelled", "returned"):
    TASKS.labels(result)
for result in ("task", "empty"):
    CLAIMS.labels(result)
for step in ("claim", "fetch", "process", "store"):
    DURATION.labels(step)
for op in ("heartbeat", "check", "store", "release"):
    STALE.labels(op)
for to in ("queued", "failed", "cancelled"):
    REAPED.labels(to)

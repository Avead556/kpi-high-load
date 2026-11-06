# vaccine-lots

CRUD-сервіс партій вакцин на складі (`/vaccine-lots`) з метриками Prometheus: FastAPI, PostgreSQL, nginx, Prometheus, Docker Compose.

Звіт: [REPORT.md](REPORT.md). Dockerfile, конфігурація nginx і Prometheus - у теці `.docker/`.

## Запуск

```sh
cp .env.example .env
docker compose up -d --build
curl localhost:8018/healthz
```

Кілька екземплярів web (після зміни кількості перезапустити балансувальник):

```sh
docker compose up -d --scale web=4
docker compose restart lb
```

Prometheus: http://localhost:9090/targets

## Навантаження

```sh
pip install -r loadgen/requirements.txt
python3 loadgen/seed.py
python3 loadgen/steps.py --path /noop --max-rate 8000 --out results/noop
python3 loadgen/steps.py --g 8000 --out results/n1
python3 loadgen/prom_export.py --summary results/n1/summary.json --out results/n1/prometheus.json
python3 loadgen/analyze.py
```

## Перевірки

```sh
python3 checks/graceful_stop.py
python3 checks/verify.py
sh checks/db_isolation.sh
```

# vaccine-lots

CRUD-сервіс партій вакцин на складі (`/vaccine-lots`): FastAPI, PostgreSQL, nginx, Docker Compose.

Звіт: [REPORT.md](REPORT.md). Dockerfile і конфігурація nginx - у теці `.docker/`.

## Запуск

```sh
cp .env.example .env
docker compose up -d --build
curl localhost:8018/healthz
```

Два вебінстанси за балансувальником:

```sh
WEB_REPLICAS=2 docker compose up -d
```

## Навантаження

```sh
python3 loadgen/seed.py
python3 loadgen/series.py --label r1
python3 loadgen/loadgen.py --rate 500 --ids "$(cat results/ids.txt)"
```

## Перевірки

```sh
python3 checks/startup.py
python3 checks/verify.py
sh checks/db_isolation.sh
```

# vaccine-lots у Kubernetes (kind)

Сервіс із лабораторної роботи 2 (FastAPI + PostgreSQL + Prometheus) у локальному кластері kind: 1 вузол control-plane
з міткою `role=infra` (база, моніторинг) і 2 вузли worker (поди web). Звіт: [REPORT.md](REPORT.md).

Потрібні: Docker, kind, kubectl, helm (`brew install kind helm`).

## Розгортання і видалення

```sh
cp .env.example .env            # задати POSTGRES_PASSWORD
./scripts/deploy.sh             # кластер, образ, база, міграція, web, моніторинг
./scripts/destroy.sh            # повне видалення кластера
```

- web: http://localhost:8080 (NodePort 30080), Prometheus: http://localhost:9090 (NodePort 30090)
- маніфести: `k8s/` (db, web, HPA, PDB, PodMonitor, values Helm-чартів)

## Перевірки

```sh
./checks/crud.sh                # CRUD через localhost:8080, X-Pod кожного запиту
./checks/db_persistence.sh      # дані переживають kubectl delete pod postgres-0
./checks/password.sh            # пароль лише в Secret
./checks/monitoring.sh          # ряди в Prometheus і kubectl top
```

## Навантаження

```sh
python3.14 -m venv .venv && .venv/bin/pip install -r loadgen/requirements.txt
.venv/bin/python loadgen/seed.py --url http://127.0.0.1:8080 --out results/ids.txt
./scripts/series.sh 1 2 3                       # крок 7: ступінчасті прогони
./scripts/elastic.sh fixed <0.3·X1> <jump>      # крок 10
./scripts/elastic.sh hpa   <0.3·X1> <jump>      # крок 11
.venv/bin/python loadgen/drain.py --pdb no  --rate <rps> --out results/drain_nopdb   # Д4
.venv/bin/python loadgen/drain.py --pdb yes --rate <rps> --out results/drain_pdb
.venv/bin/python loadgen/analyze.py             # таблиці (results/analysis.json) і діаграми (docs/charts)
```

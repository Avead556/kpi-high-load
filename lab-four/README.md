# vaccine-lots: черга задач у PostgreSQL і пул воркерів (kind)

Сервіс із лабораторної роботи 3 (FastAPI + PostgreSQL + Prometheus у kind), доповнений таблицями `tasks` і
`task_results`, ендпоінтами задач і окремим воркером, що обробляє синтетичний зріз. Звіт: [REPORT.md](REPORT.md).

Потрібні: Docker, kind, kubectl, helm (`brew install kind helm`), Python 3.14 для генератора.

## Розгортання і видалення

```sh
cp .env.example .env            # задати POSTGRES_PASSWORD
helm repo update
./scripts/deploy.sh             # кластер, образи web і worker, база, міграція, web, worker, моніторинг
./scripts/destroy.sh            # повне видалення кластера
```

- web: http://localhost:8084 (NodePort 30080), Prometheus: http://localhost:9094 (NodePort 30090)
- `k8s/worker/worker.yaml` - ConfigMap і Deployment воркера (без `replicas`, 250m / 500m CPU, 512Mi)
- `db/schema.sql` - схема (міграція Job), `db/modes/*.sql` - тимчасові зміни для кроків 3-4 і очищення черги

## Перевірки і прогони

```sh
./checks/api.sh                                     # крок 1: схема, ендпоінти, скасування, gauges
python3.14 -m venv .venv && .venv/bin/pip install -r loadgen/requirements.txt
.venv/bin/python loadgen/seed.py --url http://127.0.0.1:8084 --out results/ids.txt
cd loadgen
../.venv/bin/python steps.py step2                  # t_io, t_cpu на 1 воркері
../.venv/bin/python steps.py step3                  # наївне захоплення, D для T_poll 10 і 1 мс
../.venv/bin/python steps.py step4                  # SKIP LOCKED / FOR UPDATE, W = 3
../.venv/bin/python steps.py step4x                 # те саме на W = 12
../.venv/bin/python steps.py step5                  # lease, heartbeat, fencing token (SIGSTOP на 2L)
../.venv/bin/python steps.py step6                  # X(W) на 1, 2, 3 воркерах
../.venv/bin/python steps.py step7                  # закон Літтла, 0.7 і 1.3 X(R)
../.venv/bin/python steps.py step8                  # rollout restart під навантаженням
../.venv/bin/python steps.py step8 --grace 0.5 --slice 2048 --rate 3 --duration 30 --restart-at 10
../.venv/bin/python steps.py step9_probe            # FAIL_RATE зі стандартним backoff kubelet
../.venv/bin/python steps.py step9_probe --out step9_capacity   # ємність пулу з FAIL_RATE
../.venv/bin/python steps.py step9 --literal        # FAIL_RATE 0.15 і Ф5 при 0.7·X(R), як у завданні
../.venv/bin/python steps.py step9                  # те саме при 0.7·X_f, черга стала
../.venv/bin/python steps.py step10                 # Д6: скасування задач
../.venv/bin/python analyze.py                      # results/analysis.json і docs/charts
```

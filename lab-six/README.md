# Розподілене навчання CNN на OCTMNIST (ЛР6, варіант 18)

Д6 OCTMNIST, ширина Ш2, P_max = 4, B = 96, K_c = 10, s_f = 25. PyTorch CPU, gloo, torchrun. Звіт: [REPORT.md](REPORT.md).

```sh
python3.14 -m venv .venv && .venv/bin/pip install torch numpy scikit-learn medmnist matplotlib
mkdir -p data && curl -L -o data/octmnist.npz "https://zenodo.org/records/10519652/files/octmnist.npz?download=1"

.venv/bin/python experiments.py 1 2 3 4 5 5_nowarm    # усі прогони -> results/ (швидкість: 5 раундів, ~6 хв)
.venv/bin/python analyze.py                           # results/tables.md, results/analysis.json, docs/charts
```

Окремий прогін вручну (на цій машині імʼя хоста не резолвиться, тому статична адреса замість `--standalone`):

```sh
GLOO_SOCKET_IFNAME=lo0 .venv/bin/torchrun --nnodes=1 --nproc_per_node=4 --rdzv-backend=static \
  --master-addr=127.0.0.1 --master-port=29500 --node-rank=0 train.py --mode ddp --task speed
```

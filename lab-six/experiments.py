import argparse
import itertools
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from common import B, K_C, LR0, N_TH, P_MAX, S_F

ROOT = Path(__file__).resolve().parent
RES = ROOT / "results"
PY = str(ROOT / ".venv" / "bin" / "python")
TORCHRUN = str(ROOT / ".venv" / "bin" / "torchrun")
ENV = {**os.environ, "GLOO_SOCKET_IFNAME": "lo0", "OMP_NUM_THREADS": str(N_TH)}
PORTS = itertools.count(29500)
REPEATS = 5


def torchrun(P, args, restarts=0):
    return [TORCHRUN, "--nnodes=1", f"--nproc_per_node={P}", f"--max-restarts={restarts}", "--rdzv-backend=static",
            "--master-addr=127.0.0.1", f"--master-port={next(PORTS)}", "--node-rank=0", "train.py", *args]


def run(cmd, out, log=None, timeout=900):
    out = RES / out
    out.parent.mkdir(parents=True, exist_ok=True)
    if log:
        (RES / log).unlink(missing_ok=True)
        cmd = cmd + ["--loss-log", str(RES / log)]
    res = subprocess.run(cmd + ["--out", str(out)], cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=timeout)
    (out.with_suffix(".log")).write_text(res.stdout + res.stderr)
    print(" ".join(cmd[cmd.index("train.py") - 0 if "train.py" in cmd else 0:]), "->", res.returncode, flush=True)
    if res.returncode != 0:
        print(res.stderr[-2000:])
    return res.returncode


def single(args):
    return [PY, "train.py", "--mode", "single", *args]


def speed_configs():
    b = B // P_MAX
    cfg = [("single_B96", 1, "single", B), (f"manual_P{P_MAX}_B{B}", P_MAX, "manual", B)]
    for P in range(1, P_MAX + 1):
        cfg += [(f"ddp_P{P}_B{B}", P, "ddp", B), (f"ddp_nosync_P{P}_B{B}", P, "ddp_nosync", B)]
    cfg += [(f"ddp_P{P}_B{P * b}", P, "ddp", P * b) for P in range(1, P_MAX)]
    return cfg


def speed_all():
    for rep in range(1, REPEATS + 1):
        for name, P, mode, batch in speed_configs():
            args = ["--mode", mode, "--task", "speed", "--batch", str(batch)]
            cmd = single(args[2:]) if mode == "single" else torchrun(P, args)
            run(cmd, f"speed/{name}_r{rep}.json")


def step1():
    run(single(["--task", "train"]), "s1_train.json", log="s1_train.loss.jsonl")


def step2():
    run(torchrun(P_MAX, ["--mode", "manual", "--task", "equiv"]), "s2_equiv_manual.json")
    run(torchrun(P_MAX, ["--mode", "ddp", "--task", "equiv"]), "s2_equiv_ddp.json")
    run(torchrun(P_MAX, ["--mode", "ddp", "--task", "train"]), "s2_train_ddp.json", log="s2_train_ddp.loss.jsonl")


def step3():
    speed_all()


def step4():
    b = B // P_MAX
    run(single(["--task", "train", "--batch", str(b)]), "s4_train_single_b.json", log="s4_train_single_b.loss.jsonl")
    lr_p = LR0 * (P_MAX * b) / b
    run(torchrun(P_MAX, ["--mode", "ddp", "--task", "train", "--lr", str(lr_p)]), "s4_train_ddp_linear.json",
        log="s4_train_ddp_linear.loss.jsonl")


def step5(extra=(), prefix="s5"):
    ckpt = RES / f"{prefix}_ckpt"
    shutil.rmtree(ckpt, ignore_errors=True)
    ckpt.mkdir(parents=True)
    run(torchrun(P_MAX, ["--mode", "ddp", "--task", "train", "--ckpt-dir", str(ckpt), "--ckpt-every", str(K_C),
                         "--fail-step", str(S_F), *extra], restarts=1), f"{prefix}_train_fail.json",
        log=f"{prefix}_train_fail.loss.jsonl")
    shutil.copy(ckpt / "failed.marker", RES / f"{prefix}_failed.marker.json")


def step5_nowarm():
    step5(("--no-warm-ddp",), "s5_nowarm")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("steps", nargs="*", default=["1", "2", "3", "4", "5"])
    a = p.parse_args()
    RES.mkdir(exist_ok=True)
    for s in a.steps:
        globals()[f"step{s}"]()


if __name__ == "__main__":
    main()

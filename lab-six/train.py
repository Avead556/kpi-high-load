import argparse
import json
import os
import statistics
import time
from datetime import timedelta

import torch
import torch.distributed as dist
from sklearn.metrics import roc_auc_score
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler

from common import B, EPOCHS, LR0, MEASURE_STEPS, MOMENTUM, N_TH, WARMUP_STEPS, build_model, data

perf = time.perf_counter


def evaluate(model, test):
    model.eval()
    with torch.no_grad():
        x, y = test.tensors
        prob = torch.softmax(model(x), dim=1).numpy()
    model.train()
    y = y.numpy()
    acc = float((prob.argmax(1) == y).mean())
    auc = float(roc_auc_score(y, prob, multi_class="ovr", average="macro"))
    return acc, auc


def weight_sum(model):
    return sum(p.detach().double().sum().item() for p in model.parameters())


class Trainer:
    def __init__(self, args):
        self.args = args
        self.distributed = args.mode != "single"
        if self.distributed:
            attempt = int(os.environ.get("TORCHELASTIC_RESTART_COUNT", 0))
            store = dist.TCPStore(os.environ["MASTER_ADDR"], int(os.environ["MASTER_PORT"]), is_master=False,
                                  timeout=timedelta(seconds=args.timeout))
            dist.init_process_group("gloo", store=dist.PrefixStore(f"attempt{attempt}", store),
                                    rank=int(os.environ["RANK"]), world_size=int(os.environ["WORLD_SIZE"]),
                                    timeout=timedelta(seconds=args.timeout))
        self.world = dist.get_world_size() if self.distributed else 1
        self.rank = dist.get_rank() if self.distributed else 0
        assert args.batch % self.world == 0, "B must split evenly"
        self.b = args.batch // self.world
        self.train_set, self.test_set = data()
        self.sampler = DistributedSampler(self.train_set, num_replicas=self.world, rank=self.rank, shuffle=True, seed=0)
        self.loader = DataLoader(self.train_set, batch_size=self.b, sampler=self.sampler, drop_last=True)
        self.base = build_model()
        if args.mode == "manual":
            for p in self.base.parameters():
                dist.broadcast(p.data, src=0)
        self.model = DDP(self.base) if args.mode in ("ddp", "ddp_nosync") else self.base
        self.opt = torch.optim.SGD(self.model.parameters(), lr=args.lr, momentum=MOMENTUM)
        self.loss_fn = nn.CrossEntropyLoss()
        self.t_sync = None

    def step(self, x, y):
        self.opt.zero_grad(set_to_none=True)
        if self.args.mode == "ddp_nosync":
            with self.model.no_sync():
                loss = self.loss_fn(self.model(x), y)
                loss.backward()
        else:
            loss = self.loss_fn(self.model(x), y)
            loss.backward()
        if self.args.mode == "manual":
            t0 = perf()
            for p in self.base.parameters():
                dist.all_reduce(p.grad, op=dist.ReduceOp.SUM)
                p.grad /= self.world
            self.t_sync = perf() - t0
        self.opt.step()
        return loss

    def global_loss(self, loss):
        v = loss.detach().clone()
        if self.distributed:
            dist.all_reduce(v)
            v /= self.world
        return float(v)

    def speed(self):
        self.sampler.set_epoch(0)
        it = iter(self.loader)
        batches = [next(it) for _ in range(WARMUP_STEPS + MEASURE_STEPS)]
        times, syncs = [], []
        for k, (x, y) in enumerate(batches):
            if k == WARMUP_STEPS:
                if self.distributed:
                    dist.barrier()
                t_start = perf()
            t0 = perf()
            self.step(x, y)
            t1 = perf()
            if k >= WARMUP_STEPS:
                times.append(t1 - t0)
                if self.t_sync is not None:
                    syncs.append(self.t_sync)
        t_total = perf() - t_start
        if self.distributed:
            dist.barrier()
        med = statistics.median(times)
        per_rank = [None] * self.world
        if self.distributed:
            dist.all_gather_object(per_rank, {"t_step_ms": med * 1000, "t_total_s": t_total})
        out = {"t_step_ms": med * 1000, "t_total_s": t_total, "X": MEASURE_STEPS * self.args.batch / t_total,
               "t_step_mean_ms": statistics.fmean(times) * 1000, "per_rank": per_rank}
        if syncs:
            out["t_sync_ms"] = statistics.median(syncs) * 1000
        if isinstance(self.model, DDP):
            log = self.model._get_ddp_logging_data()
            out["ddp_buckets"] = {k: log.get(k) for k in ("bucket_sizes", "num_buckets", "bucket_cap_bytes") if k in log}
        return out

    def equiv(self):
        out = {}
        if self.args.mode == "manual":
            x, y = self.train_set.tensors[0][:self.args.batch], self.train_set.tensors[1][:self.args.batch]
            ref = build_model()
            ref.load_state_dict(self.base.state_dict())
            self.loss_fn(ref(x), y).backward()
            g = [p.grad.clone() for p in ref.parameters()]
            idx = torch.arange(self.rank, self.args.batch, self.world)
            self.base.zero_grad(set_to_none=True)
            self.loss_fn(self.base(x[idx]), y[idx]).backward()
            for p in self.base.parameters():
                dist.all_reduce(p.grad, op=dist.ReduceOp.SUM)
                p.grad /= self.world
            ratios = [float((a - b).abs().max() / a.abs().max()) for a, b in zip(g, (p.grad for p in self.base.parameters()))]
            out["grad_ratio_per_tensor"] = ratios
            out["grad_ratio_max"] = max(ratios)
            self.base.zero_grad(set_to_none=True)
        self.sampler.set_epoch(0)
        for k, (xb, yb) in enumerate(self.loader):
            if k == 50:
                break
            self.step(xb, yb)
        sums = [None] * self.world
        dist.all_gather_object(sums, weight_sum(self.base))
        out["weight_sums"] = sums
        out["weight_sum_spread"] = max(sums) - min(sums)
        return out

    def log(self, record):
        if self.rank == 0 and self.args.loss_log:
            with open(self.args.loss_log, "a") as f:
                f.write(json.dumps(record) + "\n")

    def train(self):
        a = self.args
        attempt = int(os.environ.get("TORCHELASTIC_RESTART_COUNT", 0))
        start_epoch, skip, step = 0, 0, 0
        ckpt = os.path.join(a.ckpt_dir, "ckpt.pt") if a.ckpt_dir else None
        if ckpt and os.path.exists(ckpt):
            t0 = time.time()
            state = torch.load(ckpt, weights_only=True)
            self.base.load_state_dict(state["model"])
            self.opt.load_state_dict(state["opt"])
            start_epoch, skip, step = state["epoch"], state["step_in_epoch"], state["step"]
            if isinstance(self.model, DDP) and not a.no_warm_ddp:
                x0, y0 = self.train_set.tensors[0][:self.b], self.train_set.tensors[1][:self.b]
                self.loss_fn(self.model(x0), y0).backward()
                self.opt.zero_grad(set_to_none=True)
            self.log({"event": "restore", "t": t0, "step": step, "epoch": start_epoch, "skip": skip,
                      "attempt": attempt})
        marker = os.path.join(a.ckpt_dir, "failed.marker") if a.ckpt_dir else None
        n_e = len(self.loader)
        losses = []
        t_begin = time.time()
        for epoch in range(start_epoch, a.epochs):
            self.sampler.set_epoch(epoch)
            for i, (x, y) in enumerate(self.loader):
                if epoch == start_epoch and i < skip:
                    continue
                if a.fail_step and step + 1 == a.fail_step and self.rank == self.world - 1 \
                        and not os.path.exists(marker):
                    with open(marker, "w") as f:
                        json.dump({"t_f": time.time(), "step": step + 1, "rank": self.rank}, f)
                    os._exit(1)
                loss = self.step(x, y)
                step += 1
                value = self.global_loss(loss)
                losses.append({"step": step, "epoch": epoch, "i": i, "loss": value})
                self.log({"event": "step", "step": step, "epoch": epoch, "i": i, "loss": value, "t": time.time(),
                          "attempt": attempt})
                if a.ckpt_every and step % a.ckpt_every == 0:
                    if self.rank == 0:
                        state = {"model": self.base.state_dict(), "opt": self.opt.state_dict(), "epoch": epoch,
                                 "step_in_epoch": i + 1, "step": step}
                        torch.save(state, os.path.join(a.ckpt_dir, "ckpt.tmp"))
                        os.replace(os.path.join(a.ckpt_dir, "ckpt.tmp"), ckpt)
                        self.log({"event": "ckpt", "t": time.time(), "step": step, "attempt": attempt})
                    if self.distributed:
                        dist.barrier()
        out = {"steps_total": step, "steps_per_epoch": n_e, "train_seconds_this_attempt": time.time() - t_begin}
        if self.rank == 0:
            acc, auc = evaluate(self.base, self.test_set)
            last = [r["loss"] for r in losses if r["epoch"] == a.epochs - 1]
            out.update({"ACC": acc, "AUC": auc, "max_loss_last_epoch": max(last) if last else None,
                        "loss_first": losses[0]["loss"] if losses else None, "loss_last": losses[-1]["loss"] if losses else None})
        if self.distributed:
            sums = [None] * self.world
            dist.all_gather_object(sums, weight_sum(self.base))
            out["weight_sum_spread"] = max(sums) - min(sums)
        return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["single", "manual", "ddp", "ddp_nosync"], required=True)
    p.add_argument("--task", choices=["train", "speed", "equiv"], required=True)
    p.add_argument("--batch", type=int, default=B, help="global batch; process batch b = batch / P")
    p.add_argument("--lr", type=float, default=LR0)
    p.add_argument("--epochs", type=int, default=EPOCHS)
    p.add_argument("--ckpt-dir")
    p.add_argument("--ckpt-every", type=int, default=0)
    p.add_argument("--fail-step", type=int, default=0)
    p.add_argument("--loss-log")
    p.add_argument("--no-warm-ddp", action="store_true", help="resume without the throw-away DDP iteration")
    p.add_argument("--timeout", type=float, default=120)
    p.add_argument("--out")
    a = p.parse_args()
    torch.set_num_threads(N_TH)
    t = Trainer(a)
    res = {"train": t.train, "speed": t.speed, "equiv": t.equiv}[a.task]()
    res.update({"mode": a.mode, "task": a.task, "P": t.world, "B": a.batch, "b": t.b, "lr": a.lr, "epochs": a.epochs,
                "n_th": torch.get_num_threads(), "torch": torch.__version__})
    if t.rank == 0:
        print(json.dumps({k: v for k, v in res.items() if k not in ("grad_ratio_per_tensor",)}, default=str))
        if a.out:
            with open(a.out, "w") as f:
                json.dump(res, f, indent=1)
    if t.distributed:
        dist.barrier()
        dist.destroy_process_group()


if __name__ == "__main__":
    main()

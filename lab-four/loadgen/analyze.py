"""Tables (results/analysis.json) and charts (docs/charts) from the raw data of results/."""
import csv
import json
import math
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common import ROOT, prom_range

RES = ROOT / "results"
CHARTS = ROOT / "docs" / "charts"
POLL_S = 0.010
NS = 'namespace="vaccines"'


def load(path):
    return json.loads((RES / path).read_text())


def rows(path):
    with open(RES / path) as f:
        return list(csv.DictReader(f))


# universal scalability law, formulas of lab 2 with N -> W --------------------------------------------------------
def usl_fit(points):
    x1 = points[1]
    table = []
    for w, x in sorted(points.items()):
        c = x / x1
        table.append({"W": w, "X": x, "C": c, "y": w / c - 1, "u": w - 1, "v": w * (w - 1)})
    fit = [r for r in table if r["W"] > 1]
    suu = sum(r["u"] ** 2 for r in fit)
    svv = sum(r["v"] ** 2 for r in fit)
    suv = sum(r["u"] * r["v"] for r in fit)
    suy = sum(r["u"] * r["y"] for r in fit)
    svy = sum(r["v"] * r["y"] for r in fit)
    det = suu * svv - suv ** 2
    sigma = (svv * suy - suv * svy) / det
    kappa = (suu * svy - suv * suy) / det
    note = "two-parameter fit"
    if sigma < 0:
        sigma, kappa, note = 0.0, svy / svv, "sigma < 0 -> sigma = 0, kappa = Svy / Svv"
    if kappa < 0:
        sigma, kappa = max(suy / suu, 0.0) if note == "two-parameter fit" else 0.0, 0.0
        note += "; kappa < 0 -> kappa = 0" + ("" if sigma else ", sigma = 0")
    w_star = math.sqrt((1 - sigma) / kappa) if kappa > 0 and sigma < 1 else None
    return {"rows": table, "sigma": sigma, "kappa": kappa, "W_star": w_star, "note": note,
            "sums": {"Suu": suu, "Svv": svv, "Suv": suv, "Suy": suy, "Svy": svy, "det": det}}


def usl(w, lam, sigma, kappa):
    return lam * w / (1 + sigma * (w - 1) + kappa * w * (w - 1))


def plot_usl(s6, fit, s2, out):
    ws = sorted(int(w) for w in s6)
    xs = [s6[str(w)]["X"] for w in ws]
    sd = [s6[str(w)]["std"] for w in ws]
    x1 = xs[0]
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    grid = [1 + i * 0.05 for i in range(int((max(ws) + 1 - 1) / 0.05) + 1)]
    ax.plot(grid, [usl(w, x1, fit["sigma"], fit["kappa"]) for w in grid], color="#1f6feb",
            label=f"закон: σ = {fit['sigma']:.3f}, κ = {fit['kappa']:.4f}")
    ax.plot(grid, [w * s2["X1_estimate"] for w in grid], color="#8c959f", linestyle=":",
            label=f"W / (t_io + t_cpu + T_poll/2) = W · {s2['X1_estimate']:.1f}")
    ax.errorbar(ws, xs, yerr=sd, fmt="o", color="#d1242f", capsize=5, label="X(W) ± σ посекундної кількості")
    for w, x in zip(ws, xs):
        ax.annotate(f"{x:.1f}", (w, x), textcoords="offset points", xytext=(8, -12), fontsize=9)
    ax.set_xlabel("воркерів W")
    ax.set_ylabel("задач за секунду")
    ax.set_xticks(range(1, max(ws) + 2))
    ax.set_ylim(0, None)
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)


# step 9 timeline ------------------------------------------------------------------------------------------------
def iso(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def step9_series(s9, run="step9"):
    t0, t1 = s9["t0"], s9["t1"] + 60
    path = RES / run / "prometheus.json"
    if not path.exists():
        data = {m: prom_range(f"max({m}{{{NS}}})", t0 - 10, t1, 5) for m in ("tasks_queued", "tasks_running")}
        path.write_text(json.dumps(data) + "\n")
    return json.loads(path.read_text())


def plot_step9(s9, out, run="step9"):
    t0 = s9["t0"]
    prom = step9_series(s9, run)
    tasks = rows(f"{run}/tasks.csv")
    end = int(s9["t1"] - t0) + 40
    done = [0] * end
    for t in tasks:
        if t["finished"] and t["status"] == "done":
            k = int(float(t["finished"]) - t0)
            if 0 <= k < end:
                done[k] += 1
    crashes = [0] * end
    for line in (RES / run / "worker.log").read_text().splitlines():
        if "event=crash" in line:
            k = int(iso(line.split("ts=")[1].split()[0]) - t0)
            if 0 <= k < end:
                crashes[k] += 1
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 6.2), sharex=True, gridspec_kw={"height_ratios": [1.2, 1]})
    for m, color in (("tasks_queued", "#d1242f"), ("tasks_running", "#1f6feb")):
        pts = [(float(t) - t0, float(v)) for s in prom[m] for t, v in s["values"]]
        a1.step([p[0] for p in pts], [p[1] for p in pts], where="post", color=color, label=m)
    a1.set_ylabel("задач")
    a2.bar(range(end), done, width=1.0, color="#2da44e", alpha=0.7, label="завершено за секунду (done)")
    a2.plot([i + 0.5 for i in range(end)], crashes, color="#6e7781", linewidth=0.9, label="падінь воркера за секунду")
    a2.axhline(s9["rate"], color="#bf8700", linestyle="--", linewidth=1, label=f"λ = {s9['rate']:.2f}/с")
    a2.set_ylabel("за секунду")
    a2.set_xlabel("секунди від початку прогону")
    for ax in (a1, a2):
        for i, p in enumerate(s9["phases"]):
            ax.axvline(p["at"] - t0, color="black", linestyle="--", linewidth=1)
            ax.axvspan(p["at"] - t0, p["rolled"] - t0, color="#8c959f", alpha=0.18)
        ax.grid(alpha=0.3)
    labels = ["базовий: L = 10 с, heartbeat", "Ф5: L = t_cpu/2, без heartbeat", "Ф5: L = t_cpu/2, heartbeat"]
    edges = [0] + [p["at"] - t0 for p in s9["phases"]] + [s9["t1"] - t0]
    for i, lab in enumerate(labels):
        a1.text((edges[i] + edges[i + 1]) / 2, 1.02, lab, transform=a1.get_xaxis_transform(), ha="center", fontsize=9)
    if s9.get("literal"):
        a2.set_xlim(-5, s9["t1"] - t0 + 5)  # the queue is truncated after the run, not drained
    a1.legend(loc="upper left" if s9.get("literal") else "upper right", fontsize=9)
    a2.legend(loc="upper right", fontsize=8, ncol=3)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)


# step 7 ---------------------------------------------------------------------------------------------------------
def plot_step7(s7, out):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for ax, name in zip(axes, ("steady", "overload")):
        tasks = rows(f"step7/{name}/tasks.csv")
        t0 = min(float(t["created"]) for t in tasks)
        xs = [float(t["created"]) - t0 for t in tasks]
        ys = [(float(t["started"]) - float(t["created"])) * (1000 if name == "steady" else 1) for t in tasks]
        ax.scatter(xs, ys, s=2, color="#1f6feb", alpha=0.5)
        info = s7[name]
        if name == "steady":
            lo, hi = info["window"]
            ax.axvspan(lo - t0, hi - t0, color="#2da44e", alpha=0.1, label="вікно 120 с")
            ax.set_ylabel("W_q, мс")
            ax.set_yscale("log")
            ax.set_title(f"0.7·X(R) = {info['rate']:.1f}/с: λ·W_q = {info['lambda_Wq']:.2f}, L_q = {info['L_q_gauge']:.2f}",
                         fontsize=10)
            ax.legend(fontsize=9)
        else:
            ax.set_ylabel("W_q, с")
            ax.set_title(f"1.3·X(R) = {info['rate']:.1f}/с: черга на кінець {info['queue_at_end']}", fontsize=10)
        ax.set_xlabel("момент створення задачі, с")
        ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)


def generator_check():
    """Planned vs actual send rate of every task generator run: per-second counts and the send lag."""
    out = {}
    for path in sorted(RES.rglob("generator.csv")):
        name = str(path.parent.relative_to(RES))
        if name.startswith("rejected"):
            continue
        r = rows(path.relative_to(RES))
        planned = [float(x["planned"]) for x in r]
        sent = [float(x["sent"]) for x in r]
        lag = sorted(b - a for a, b in zip(planned, sent))
        t0, n = planned[0], int(math.ceil(planned[-1] - planned[0])) + 1
        per_plan, per_sent = [0] * (n + 5), [0] * (n + 5)
        for a in planned:
            per_plan[int(a - t0)] += 1
        for b in sent:
            per_sent[min(int(b - t0), n + 4)] += 1
        full = range(n - 1) if n > 1 else range(1)  # the last second may be partial
        worst = max(full, key=lambda i: abs(per_plan[i] - per_sent[i]))
        out[name] = {"tasks": len(r), "rate_planned": (len(r) - 1) / (planned[-1] - planned[0]) if len(r) > 1 else None,
                     "rate_sent": (len(r) - 1) / (sent[-1] - sent[0]) if len(r) > 1 else None,
                     "lag_p99_ms": lag[int(0.99 * (len(lag) - 1))] * 1000, "lag_max_ms": lag[-1] * 1000,
                     "worst_second": [worst, per_plan[worst], per_sent[worst]],
                     "non_2xx": sum(x["status"] != "202" for x in r)}
    return out


def main():
    CHARTS.mkdir(parents=True, exist_ok=True)
    out = {}
    s2 = load("step2/summary.json")
    out["step2"] = {k: s2[k] for k in ("t_io_ms", "t_cpu_ms", "fetch_ms", "store_ms", "t_cpu_p50_ms", "t_cpu_max_ms",
                                       "X1_estimate")}
    s3 = load("step3/summary.json")
    out["step3"] = {k: {"D_results": v["D_results_mean"], "D_logs": v["D_logs_mean"],
                        "per_run": [(r["dup_by_results"], r["dup_by_logs"], r["max_results_per_task"]) for r in v["runs"]],
                        "dup_total": v["dup_total"], "n_total": v["n_total"]} for k, v in s3.items()}
    s4 = load("step4/summary.json")
    s4x = load("step4x/summary.json")
    keys = ("D_results", "D_logs", "claim_p50_ms", "claim_p99_ms", "claims_empty", "claims_task", "lock_waiting_max_1s",
            "lock_waiting_max_10ms", "lock_waiting_share_10ms", "duration_s")
    out["step4"] = {f"{m}/{sr}": {k: v[k] for k in keys} for m, d in s4.items() for sr, v in d.items()}
    out["step4"].update({f"{m}/W12": {k: v[k] for k in keys} for m, v in s4x.items()})
    s5 = load("step5/summary.json")
    out["step5"] = {k: s5[k] for k in ("victim", "other", "task", "before", "after", "results", "victim_stale_lines",
                                       "state_while_stopped", "stop_seconds")}
    s6 = load("step6/summary.json")
    fit = usl_fit({int(w): s["X"] for w, s in s6.items()})
    out["step6"] = {"rows": {w: {k: s[k] for k in ("X", "std", "claim_p99_ms", "db_tps", "db_cpu_cores",
                                                   "worker_cpu_cores", "worker_throttled_share", "process_p50_ms",
                                                   "count", "drain_seconds")} for w, s in s6.items()},
                    "usl": fit, "X1_vs_estimate": {"X1": s6["1"]["X"], "estimate": s2["X1_estimate"]}}
    plot_usl(s6, fit, s2, CHARTS / "usl.png")
    s7 = load("step7/summary.json")
    out["step7"] = {"steady": {k: s7["steady"][k] for k in ("rate", "lambda", "W_q_s", "lambda_Wq", "L_q_gauge",
                                                            "L_q_timestamps", "diff_pct", "W_q_p50_ms", "W_q_p99_ms")},
                    "overload": {k: s7["overload"][k] for k in ("rate", "queue_at_end", "created", "W_q_first100_s",
                                                                "W_q_last100_s")}}
    plot_step7(s7, CHARTS / "little.png")
    s8 = load("step8/summary.json")
    s8g = load("step8_grace0.5_slice2048/summary.json")
    out["step8"] = {"exit_codes": s8["exit_codes"], "returned": len(s8["returned"]), "failed": s8["failed"],
                    "multi_result": s8["multi_result"], "attempts_gt1": s8["attempts_gt1"], "tasks": s8["tasks"],
                    "grace_check": {"exit_codes": s8g["exit_codes"], "returned": s8g["returned_tasks"],
                                    "sigterm_in_flight": [e.get("in_flight") for e in s8g["sigterm"]]}}
    s9 = load("step9/summary.json")
    cap = load("step9_capacity/summary.json")
    dflt = load("rejected/fail_rate_default_backoff/summary.json")
    out["step9"] = {"X_R": s9["X_R"], "X_f": s9["X_f"], "X_default_backoff": dflt["X"], "rate": s9["rate"],
                    "crash_exits": s9["crash_exits"], "crash_events_logged": s9["crash_events_logged"],
                    "status_all": s9["status_all"], "attempts_all": s9["attempts_all"],
                    "multi_result": s9["multi_result"], "per_phase": s9["per_phase"],
                    "capacity_crashes": sum(1 for e in cap["exits"] if e["exit_code"] == 1),
                    "capacity_duration": cap["duration"]}
    plot_step9(s9, CHARTS / "step9.png")
    s10 = load("step10/summary.json")
    out["step10"] = s10
    out["generator"] = generator_check()
    lit = RES / "step9_literal" / "summary.json"
    if lit.exists():
        out["step9_literal"] = json.loads(lit.read_text())
        plot_step9(out["step9_literal"], CHARTS / "step9_literal.png", "step9_literal")
    (RES / "analysis.json").write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps({k: out[k] for k in ("step2", "step3", "step6")}, ensure_ascii=False, indent=1, default=str)[:4000])
    print("USL", {k: fit[k] for k in ("sigma", "kappa", "W_star", "note")})


if __name__ == "__main__":
    main()

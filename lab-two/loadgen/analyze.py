import argparse
import csv
import json
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

QUANTILE = 0.99


def load_run(results, name):
    base = Path(results) / name
    summary = json.loads((base / "summary.json").read_text())
    seconds = list(csv.DictReader(open(base / "seconds.csv")))
    prom = json.loads((base / "prometheus.json").read_text()) if (base / "prometheus.json").exists() else None
    stats = [json.loads(line) for line in open(base / "stats.jsonl")] if (base / "stats.jsonl").exists() else []
    return {"name": name, "summary": summary, "seconds": seconds, "prom": prom, "stats": stats}


def bound(le):
    return math.inf if le == "+Inf" else float(le)


def buckets_at(prom, epoch, key="buckets", instance=None):
    best, best_dt = None, None
    for series in prom[key]:
        if instance is not None and series["metric"].get("instance") != instance:
            continue
        for ts, value in series["values"]:
            dt = abs(ts - epoch)
            if best_dt is None or dt < best_dt - 1e-9:
                best_dt, best = dt, ts
    if best is None:
        return None
    counts = {}
    for series in prom[key]:
        if instance is not None and series["metric"].get("instance") != instance:
            continue
        for ts, value in series["values"]:
            if ts == best:
                le = series["metric"]["le"]
                counts[le] = counts.get(le, 0.0) + float(value)
    return sorted(((bound(le), c) for le, c in counts.items()), key=lambda x: x[0])


def quantile(buckets, q):
    if not buckets:
        return None
    n = buckets[-1][1]
    if n <= 0:
        return None
    rank = q * n
    prev_b, prev_c = 0.0, 0.0
    finite = [b for b, _ in buckets if b != math.inf]
    for b, c in buckets:
        if c >= rank:
            if b == math.inf:
                return {"value_ms": finite[-1] * 1000, "over_last": True, "bucket_ms": (finite[-1] * 1000, math.inf)}
            value = prev_b + (b - prev_b) * (rank - prev_c) / (c - prev_c) if c > prev_c else b
            return {"value_ms": value * 1000, "over_last": False, "bucket_ms": (prev_b * 1000, b * 1000)}
        prev_b, prev_c = b, c
    return None


def share_le(buckets, threshold):
    total = buckets[-1][1]
    below = next((c for b, c in buckets if abs(b - threshold) < 1e-12), None)
    return below / total if total else None


def series_by_instance(prom, key):
    out = {}
    for s in prom[key]:
        out.setdefault(s["metric"].get("instance", "all"), []).extend((ts, float(v)) for ts, v in s["values"])
    return out


def window_mean(points, lo, hi):
    values = [v for ts, v in points if lo <= ts <= hi]
    return statistics.fmean(values) if values else None


def step_table(run, g):
    s = run["summary"]
    prom = run["prom"]
    rows = []
    for step in s["steps"]:
        lo, hi = step["window_epoch"]
        b = buckets_at(prom, hi) if prom else None
        server = quantile(b, QUANTILE) if b else None
        cpu = series_by_instance(prom, "cpu") if prom else {}
        inflight = series_by_instance(prom, "in_progress") if prom else {}
        cpu_means = [m for m in (window_mean(p, lo, hi + 5) for p in cpu.values()) if m is not None]
        inflight_means = [m for m in (window_mean(p, lo, hi + 5) for p in inflight.values()) if m is not None]
        rows.append({
            **step,
            "p99_server": server,
            "cpu_per_instance_pct": round(statistics.fmean(cpu_means) * 100, 1) if cpu_means else None,
            "in_progress_per_instance": round(statistics.fmean(inflight_means), 2) if inflight_means else None,
            "buckets": b,
        })
    return rows


def run_x(run, g):
    s = run["summary"]
    best = s["X"]
    last = s["steps"][-1]
    limited = best["achieved_mean"] >= 0.9 * g or last["achieved_mean"] >= 0.9 * last["target_rps"]
    return best, limited


def usl_fit(points, x1):
    rows = []
    for n, x in points:
        c = x / x1
        rows.append({"N": n, "X": x, "C": c, "y": n / c - 1, "u": n - 1, "v": n * (n - 1)})
    fit = [r for r in rows if r["N"] > 1 and not r.get("limited")]
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
        sigma, kappa, note = 0.0, svy / svv, "sigma < 0 set to 0, kappa = Svy / Svv"
    elif kappa < 0:
        sigma, kappa, note = suy / suu, 0.0, "kappa < 0 set to 0, sigma = Suy / Suu"
    n_star = math.sqrt((1 - sigma) / kappa) if kappa > 0 and sigma < 1 else None
    sums = {"Suu": suu, "Svv": svv, "Suv": suv, "Suy": suy, "Svy": svy, "det": det}
    return rows, sigma, kappa, n_star, note, sums


def usl(n, lam, sigma, kappa):
    return lam * n / (1 + sigma * (n - 1) + kappa * n * (n - 1))


def plot_timeline(run, out):
    s = run["summary"]
    t0 = s["start_epoch"]
    sec = [int(r["second"]) for r in run["seconds"]]
    fig, axes = plt.subplots(5, 1, figsize=(11, 15), sharex=True)
    ax = axes[0]
    ax.plot(sec, [float(r["target_rps"]) for r in run["seconds"]], label="задана", color="#555555", drawstyle="steps-post")
    ax.plot(sec, [int(r["sent"]) for r in run["seconds"]], label="фактично надіслано", color="#e3a008", linewidth=0.8)
    ax.plot(sec, [int(r["achieved"]) for r in run["seconds"]], label="досягнута (2xx)", color="#1f6feb", linewidth=1.0)
    ax.set_ylabel("запитів/с")
    ax.legend(loc="upper left")
    ax.set_title(f"{run['name']}: інтенсивність")
    ax = axes[1]
    p99c = [(int(r["second"]), float(r["p99_ms"])) for r in run["seconds"] if r["p99_ms"]]
    ax.plot([a for a, _ in p99c], [b for _, b in p99c], label="p99 клієнта (за секунду)", color="#d1242f", linewidth=0.9)
    prom = run["prom"]
    if prom:
        stamps = sorted({ts for srs in prom["buckets"] for ts, _ in srs["values"]})
        server = []
        for ts in stamps:
            q = quantile(buckets_at(prom, ts), QUANTILE)
            if q:
                server.append((ts - t0, q["value_ms"]))
        ax.plot([a for a, _ in server], [b for _, b in server], label="p99 сервера (вікно 15 с, крок 5 с)",
                color="#2da44e", marker="o", markersize=3)
    ax.set_yscale("log")
    ax.set_ylabel("мс")
    ax.legend(loc="upper left")
    ax.set_title("p99 клієнта і сервера")
    for ax, key, label, scale in ((axes[2], "rps", "запитів/с", 1), (axes[3], "cpu", "CPU, %", 100),
                                  (axes[4], "in_progress", "запитів в обробці", 1)):
        if prom:
            for i, (inst, pts) in enumerate(sorted(series_by_instance(prom, key).items())):
                pts.sort()
                ax.plot([ts - t0 for ts, _ in pts], [v * scale for _, v in pts], label=inst, linewidth=1.0)
        ax.set_ylabel(label)
        ax.legend(loc="upper left", fontsize=8)
    axes[2].set_title("кількість запитів за секунду по екземплярах")
    axes[3].set_title("використання CPU по екземплярах (ліміт 100%)")
    axes[4].set_title("кількість запитів в обробці по екземплярах")
    axes[4].set_xlabel("секунди від початку прогону")
    for ax in axes:
        ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="tables and charts for lab 2")
    parser.add_argument("--results", default="results")
    parser.add_argument("--charts", default="docs/charts")
    args = parser.parse_args()
    Path(args.charts).mkdir(parents=True, exist_ok=True)
    noop = load_run(args.results, "noop")
    g = noop["summary"]["X"]["achieved_mean"]
    runs = {n: load_run(args.results, f"n{n}") for n in (1, 2, 3, 4)}
    tables = {n: step_table(r, g) for n, r in runs.items()}
    xs = {}
    for n, r in runs.items():
        best, limited = run_x(r, g)
        row = next(t for t in tables[n] if t["step"] == best["step"])
        xs[n] = {"X": best["achieved_mean"], "std": best["achieved_std"], "step_target": best["target_rps"],
                 "p99_client_ms": best["p99_ms"], "p99_server": row["p99_server"],
                 "non_2xx_share": best["non_2xx_share"], "cpu_per_instance_pct": row["cpu_per_instance_pct"],
                 "in_progress_per_instance": row["in_progress_per_instance"], "generator_limited": limited,
                 "window_epoch": best["window_epoch"]}
    x1 = xs[1]["X"]
    points = [(n, xs[n]["X"]) for n in (1, 2, 3, 4)]
    rows, sigma, kappa, n_star, note, sums = usl_fit(points, x1)
    for r in rows:
        r["limited"] = xs[r["N"]]["generator_limited"]
    fit_points = [(r["N"], r["X"]) for r in rows if not r["limited"] or r["N"] == 1]
    rows, sigma, kappa, n_star, note, sums = usl_fit(fit_points, x1)
    first = tables[1][0]["p99_server"]["value_ms"] / 1000
    bounds_s = [0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024]
    threshold = min(bounds_s, key=lambda b: (abs(b - first), -b))
    m4 = {}
    for n in (1, 4):
        m4[n] = [{"target_rps": t["target_rps"], "share": share_le(t["buckets"], threshold)} for t in tables[n] if t["buckets"]]
    step9 = []
    for t in tables[1]:
        q = t["p99_server"]
        step9.append({"target_rps": t["target_rps"], "p99_client_ms": t["p99_ms"],
                      "p99_server_ms": None if q is None else round(q["value_ms"], 2),
                      "over_last": None if q is None else q["over_last"],
                      "bucket_ms": None if q is None else q["bucket_ms"],
                      "max_error_ms": None if q is None else (None if q["over_last"] else round(q["bucket_ms"][1] - q["bucket_ms"][0], 3))})
    lo, hi = xs[4]["window_epoch"]
    samples = [s for s in runs[4]["stats"] if lo <= s["epoch"] <= hi]
    containers = {}
    for s in samples:
        for name, c in s["containers"].items():
            containers.setdefault(name, []).append(c)
    step11 = {"samples": len(samples),
              "containers": {k: {"cpu_pct": round(statistics.fmean(c["cpu_pct"] for c in v), 1), "mem": v[-1]["mem"]}
                             for k, v in sorted(containers.items())},
              "generator_cpu_pct": round(statistics.fmean(s["generator"]["cpu_pct"] for s in samples), 1) if samples else None,
              "generator_rss_mb": samples[-1]["generator"]["rss_mb"] if samples else None}
    host_file = Path(args.results) / "host_probe" / "host.jsonl"
    if host_file.exists():
        host = [json.loads(line) for line in open(host_file)]
        step11["host_probe"] = {
            "samples": len(host),
            "host_cpu_busy_pct": round(statistics.fmean(h["host_cpu_busy_pct"] for h in host), 1),
            "processes_cpu_pct": {g: round(statistics.fmean(h["processes"][g]["cpu_pct"] for h in host), 1)
                                  for g in host[0]["processes"]},
        }
    for n, r in runs.items():
        if n in (1, 4):
            plot_timeline(r, Path(args.charts) / f"timeline_n{n}.png")
    fig, ax = plt.subplots(figsize=(8, 5))
    ns = [1, 2, 3, 4]
    ax.errorbar(ns, [xs[n]["X"] for n in ns], yerr=[xs[n]["std"] for n in ns], fmt="o", color="#1f6feb",
                capsize=4, label="виміряне X(N) ± розкид")
    grid = [i / 10 for i in range(10, 81)]
    ax.plot(grid, [usl(n, x1, sigma, kappa) for n in grid], color="#2da44e", label=f"USL: σ = {sigma:.3f}, κ = {kappa:.4f}")
    ax.plot(grid, [x1 * n for n in grid], color="#888888", linestyle="--", label="λN, λ = X(1)")
    if n_star:
        ax.axvline(n_star, color="#d1242f", linestyle=":", label=f"N* = {n_star:.2f}")
    ax.set_xlabel("N, екземплярів")
    ax.set_ylabel("X(N), запитів/с")
    ax.set_ylim(0, max(xs[n]["X"] for n in ns) * 2.2)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(Path(args.charts) / "usl.png", dpi=110)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 5))
    for n, color in ((1, "#1f6feb"), (4, "#d1242f")):
        pts = [(p["target_rps"], p["share"]) for p in m4[n] if p["share"] is not None]
        ax.plot([a for a, _ in pts], [b * 100 for _, b in pts], marker="o", color=color, label=f"N = {n}")
    ax.set_xlabel("задана інтенсивність, запитів/с")
    ax.set_ylabel(f"частка запитів ≤ {threshold * 1000:g} мс, %")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(Path(args.charts) / "m4.png", dpi=110)
    plt.close(fig)
    result = {"G": g, "noop_steps": len(noop["summary"]["steps"]), "X": xs, "usl_rows": rows, "sigma": sigma,
              "kappa": kappa, "n_star": n_star, "fit_note": note, "sums": sums, "m4_threshold_ms": threshold * 1000,
              "m4": m4, "step9": step9, "step11": step11,
              "steps": {n: [{k: v for k, v in t.items() if k != "buckets"} for t in tables[n]] for n in tables}}
    Path(args.results, "analysis.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    print(f"G = {g}")
    print(f"sigma = {sigma:.4f}, kappa = {kappa:.5f}, N* = {n_star:.2f} ({note})" if n_star else f"sigma = {sigma:.4f}, kappa = {kappa:.5f}")
    print(f"M4 threshold = {threshold * 1000:g} ms")
    print("N  X(N)    spread  step   p99 client  p99 server  non-2xx  CPU/inst  in-flight/inst  generator-limited")
    for n in (1, 2, 3, 4):
        x = xs[n]
        server = x["p99_server"] and round(x["p99_server"]["value_ms"], 2)
        print(f"{n}  {x['X']:<7} {x['std']:<7} {x['step_target']:<6} {x['p99_client_ms']:<11} {server:<11} "
              f"{x['non_2xx_share']:<8} {x['cpu_per_instance_pct']:<9} {x['in_progress_per_instance']:<15} {x['generator_limited']}")


if __name__ == "__main__":
    main()

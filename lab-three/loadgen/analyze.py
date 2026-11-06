import argparse
import bisect
import csv
import json
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = 3
REQUEST_CPU = 0.5
JUMP_BEGIN, JUMP_END, PROFILE_END = 30, 90, 180
LAB2 = Path(__file__).resolve().parents[2] / "lab-two" / "results" / "analysis.json"


def load_run(base):
    base = Path(base)
    if not (base / "summary.json").exists():
        return None
    run = {"name": base.name, "summary": json.loads((base / "summary.json").read_text()),
           "seconds": list(csv.DictReader(open(base / "seconds.csv")))}
    run["prom"] = json.loads((base / "prometheus.json").read_text()) if (base / "prometheus.json").exists() else None
    if (base / "pods.csv").exists():
        rows = list(csv.reader(open(base / "pods.csv")))
        run["pods_header"] = rows[0][1:]
        run["pods_rows"] = [[int(x) for x in r] for r in rows[1:]]
    return run


def series(prom, key, label="pod"):
    out = {}
    for s in prom.get(key, []):
        name = s["metric"].get(label, "all") if label else "all"
        out.setdefault(name, []).extend((float(ts), float(v)) for ts, v in s["values"])
    for v in out.values():
        v.sort()
    return out


def value_at(points, ts):
    if not points:
        return None
    i = bisect.bisect_right([p[0] for p in points], ts + 1e-6) - 1
    return points[i][1] if i >= 0 else None


def delta(points, lo, hi):
    a, b = value_at(points, lo), value_at(points, hi)
    if b is None:
        return None
    return b - (a or 0.0) if (a is None or b >= a) else b


def bound(le):
    return math.inf if le == "+Inf" else float(le)


def bucket_deltas(prom, lo, hi, pods=None):
    counts = {}
    for s in prom["buckets"]:
        pod = s["metric"].get("pod")
        if pods is not None and pod not in pods:
            continue
        pts = sorted((float(ts), float(v)) for ts, v in s["values"])
        d = delta(pts, lo, hi)
        if d is not None:
            le = s["metric"]["le"]
            counts[le] = counts.get(le, 0.0) + d
    return sorted(((bound(le), c) for le, c in counts.items()), key=lambda x: x[0])


def quantile(buckets, q=0.99):
    if not buckets or buckets[-1][1] <= 0:
        return None
    rank = q * buckets[-1][1]
    prev_b, prev_c = 0.0, 0.0
    finite = [b for b, _ in buckets if b != math.inf]
    for b, c in buckets:
        if c >= rank:
            if b == math.inf:
                return {"ms": finite[-1] * 1000, "over_last": True, "bucket_ms": [finite[-1] * 1000, None]}
            v = prev_b + (b - prev_b) * (rank - prev_c) / (c - prev_c) if c > prev_c else b
            return {"ms": round(v * 1000, 2), "over_last": False, "bucket_ms": [prev_b * 1000, b * 1000]}
        prev_b, prev_c = b, c
    return None


def throttling(prom, lo, hi):
    periods, throttled = series(prom, "cfs_periods"), series(prom, "cfs_throttled")
    out = {}
    for pod, pts in periods.items():
        dp, dt = delta(pts, lo, hi), delta(throttled.get(pod, []), lo, hi)
        if dp and dt is not None:
            out[pod] = dt / dp
    return out


def series_row(n, run):
    s = run["summary"]
    best = s["X"]
    last = s["steps"][-1]
    unreached = last["achieved_mean"] >= 0.9 * last["target_rps"]
    lo, hi = best["window_epoch"]
    prom = run["prom"]
    row = {"N": n, "X": best["achieved_mean"], "std": best["achieved_std"], "step": best["target_rps"],
           "p99_client_ms": best["p99_ms"], "non_2xx_share": best["non_2xx_share"], "unreached": unreached,
           "window_epoch": [lo, hi], "pods_rps_client": best["pods_rps"]}
    if prom:
        row["p99_server"] = quantile(bucket_deltas(prom, lo, hi))
        thr = throttling(prom, lo, hi)
        row["throttling"] = {p: round(v, 4) for p, v in sorted(thr.items())}
        row["throttling_mean"] = round(statistics.fmean(thr.values()), 4) if thr else None
        row["throttling_max"] = round(max(thr.values()), 4) if thr else None
        req = series(prom, "requests")
        per_pod = {p: delta(pts, lo, hi) / (hi - lo) for p, pts in req.items() if delta(pts, lo, hi)}
        row["pods_rps_server"] = {p: round(v, 1) for p, v in sorted(per_pod.items())}
        cpu = series(prom, "cpu_rate")
        row["cpu_cores"] = {p: round(statistics.fmean(v for t, v in pts if lo <= t <= hi), 3)
                            for p, pts in sorted(cpu.items()) if any(lo <= t <= hi for t, _ in pts)}
    rps = row.get("pods_rps_server") or row["pods_rps_client"]
    if n > 1 and rps:
        vals = list(rps.values()) + [0.0] * max(0, n - len(rps))
        row["imbalance"] = round(max(vals) / statistics.fmean(vals), 3)
        client = list(row["pods_rps_client"].values()) + [0.0] * max(0, n - len(row["pods_rps_client"]))
        row["imbalance_client"] = round(max(client) / statistics.fmean(client), 3)
        hi_pod, lo_pod = max(rps, key=rps.get), min(rps, key=rps.get)
        row["extremes"] = {}
        for tag, pod in (("most", hi_pod), ("least", lo_pod)):
            q = quantile(bucket_deltas(prom, lo, hi, {pod})) if prom else None
            row["extremes"][tag] = {"pod": pod, "rps": rps[pod], "p99_server": q,
                                    "throttling": row.get("throttling", {}).get(pod)}
    return row


def usl_fit(points):
    x1 = points[1]
    rows = []
    for n, x in sorted(points.items()):
        c = x / x1
        rows.append({"N": n, "X": x, "C": c, "y": n / c - 1, "u": n - 1, "v": n * (n - 1)})
    fit = [r for r in rows if r["N"] > 1]
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
    elif kappa < 0:
        sigma, kappa, note = suy / suu, 0.0, "kappa < 0 -> kappa = 0, sigma = Suy / Suu"
    n_star = math.sqrt((1 - sigma) / kappa) if kappa > 0 and sigma < 1 else None
    return {"rows": rows, "sigma": sigma, "kappa": kappa, "n_star": n_star, "note": note,
            "sums": {"Suu": suu, "Svv": svv, "Suv": suv, "Suy": suy, "Svy": svy, "det": det}}


def usl(n, lam, sigma, kappa):
    return lam * n / (1 + sigma * (n - 1) + kappa * n * (n - 1))


def c_err(xn, sn, x1, s1):
    c = xn / x1
    return c * math.sqrt((sn / xn) ** 2 + (s1 / x1) ** 2)


def plot_usl(k8s, k8s_fit, lab2, lab2_fit, out):
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    grid = [i / 20 for i in range(20, 121)]
    x1k, s1k = k8s[1]["X"], k8s[1]["std"]
    ns = sorted(k8s)
    ax.errorbar([n + 0.03 for n in ns], [k8s[n]["X"] / x1k for n in ns],
                yerr=[0 if n == 1 else c_err(k8s[n]["X"], k8s[n]["std"], x1k, s1k) for n in ns],
                fmt="o", color="#1f6feb", capsize=4, label="Kubernetes (kind), C(N) ± розкид")
    ax.plot(grid, [usl(n, 1, k8s_fit["sigma"], k8s_fit["kappa"]) for n in grid], color="#1f6feb",
            label=f"USL Kubernetes: σ = {k8s_fit['sigma']:.3f}, κ = {k8s_fit['kappa']:.4f}")
    x1c, s1c = lab2["1"]["X"], lab2["1"]["std"]
    nc = sorted(int(n) for n in lab2)
    ax.errorbar([n - 0.03 for n in nc], [lab2[str(n)]["X"] / x1c for n in nc],
                yerr=[0 if n == 1 else c_err(lab2[str(n)]["X"], lab2[str(n)]["std"], x1c, s1c) for n in nc],
                fmt="s", color="#d1242f", capsize=4, label="Compose (ЛР2), C(N) ± розкид")
    ax.plot(grid, [usl(n, 1, lab2_fit["sigma"], lab2_fit["kappa"]) for n in grid], color="#d1242f", linestyle="--",
            label=f"USL Compose: σ = {lab2_fit['sigma']:.3f}, κ = {lab2_fit['kappa']:.4f}")
    ax.plot(grid, grid, color="#888888", linestyle=":", label="лінійне C(N) = N")
    ax.set_xlabel("N, реплік")
    ax.set_ylabel("C(N) = X(N) / X(1)")
    ax.set_ylim(0, 3.2)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def smooth(values, k=5):
    out = []
    for i in range(len(values)):
        w = values[max(0, i - k + 1):i + 1]
        out.append(sum(w) / len(w))
    return out


def plot_pods(run, row, out):
    s = run["summary"]
    rows = run["pods_rows"]
    sec = [r[0] for r in rows]
    fig, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
    ax = axes[0]
    ax.plot(sec, [float(r["target_rps"]) for r in run["seconds"]][:len(sec)], color="#555555",
            drawstyle="steps-post", label="задана")
    ax.plot(sec, [int(r["achieved"]) for r in run["seconds"]][:len(sec)], color="#1f6feb", linewidth=0.8,
            label="досягнута (2xx)")
    for i, name in enumerate(run["pods_header"]):
        ax.plot(sec, smooth([r[i + 1] for r in rows]), linewidth=1.0, label=name)
    lo, hi = row["window_epoch"]
    t0 = s["start_epoch"]
    for a in axes:
        a.axvspan(lo - t0, hi - t0, color="#2da44e", alpha=0.12)
        a.grid(alpha=0.3)
    ax.set_ylabel("запитів/с")
    ax.set_title(f"N = {row['N']}: запитів за секунду на кожен под (за X-Pod, ковзне середнє 5 с); "
                 f"зелена смуга - сходинка X(N)")
    ax.legend(fontsize=8, loc="upper left")
    ax = axes[1]
    total = [max(int(r["achieved"]), 1) for r in run["seconds"]][:len(sec)]
    for i, name in enumerate(run["pods_header"]):
        ax.plot(sec, smooth([100 * r[i + 1] / t for r, t in zip(rows, total)]), linewidth=1.0, label=name)
    ax.axhline(100 / row["N"], color="#555555", linestyle=":", label="рівна частка")
    ax.set_ylabel("частка запитів, %")
    ax.set_xlabel("секунди від початку прогону")
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def per_second(prom, key, t0, T, label=None):
    pts = series(prom, key, label).get("all", [])
    return [value_at(pts, t0 + t) for t in range(T)]


def cpu_relative(prom, t0, T):
    cpu = series(prom, "cpu_rate")
    out_avg, per_pod = [], {}
    for t in range(T):
        vals = []
        for pod, pts in cpu.items():
            recent = [v for ts, v in pts if 0 <= (t0 + t) - ts < 25]
            if recent:
                vals.append(recent[-1])
                per_pod.setdefault(pod, []).append((t, recent[-1] / REQUEST_CPU * 100))
        out_avg.append(statistics.fmean(vals) / REQUEST_CPU * 100 if vals else None)
    return out_avg, per_pod


def elasticity(run, xs, p99_0, p99_jump_fixed):
    s = run["summary"]
    t0 = s["start_epoch"]
    T = PROFILE_END
    rows = run["seconds"][:T]
    z = [float(r["target_rps"]) for r in rows]
    d = [next((n for n in range(1, R + 1) if xs[n] >= zt), R) for zt in z]
    prom = run["prom"]
    s_t = [int(v) if v is not None else 0 for v in per_second(prom, "ready", t0, T, None)]
    desired = per_second(prom, "desired", t0, T, None)
    p99 = [float(r["p99_ms"]) if r["p99_ms"] else None for r in rows]
    res = {
        "T": T,
        "theta_U": sum(st < dt for st, dt in zip(s_t, d)) / T,
        "theta_O": sum(st > dt for st, dt in zip(s_t, d)) / T,
        "a_U": sum(max(dt - st, 0) for st, dt in zip(s_t, d)) / T,
        "a_O": sum(max(st - dt, 0) for st, dt in zip(s_t, d)) / T,
        "W": sum(s_t),
        "violations": sum(1 for v in p99 if v is not None and v > 2 * p99_0),
        "non_2xx": sum(int(r["non_2xx"]) + int(r["conn_errors"]) for r in rows),
        "segments": s["segments"],
    }
    res["series"] = {"z": z, "d": d, "s": s_t, "desired": desired, "p99": p99,
                     "achieved": [int(r["achieved"]) for r in rows]}
    res["series"]["cpu_avg"], res["series"]["cpu_pods"] = cpu_relative(prom, t0, T)
    if any(v is not None for v in desired):
        base = desired[JUMP_BEGIN]
        first_change = next((t for t in range(JUMP_BEGIN, T) if desired[t] is not None and desired[t] != base), None)
        peak = max(s_t)
        peak_at = next((t for t in range(JUMP_BEGIN, T) if s_t[t] == peak), None)
        threshold = 1.2 * p99_jump_fixed
        recover = None
        for t in range(JUMP_BEGIN, JUMP_END - 9):
            if all(p99[k] is not None and p99[k] < threshold for k in range(t, t + 10)):
                recover = t
                break
        ready_at_end = s_t[JUMP_END]
        down = next((t for t in range(JUMP_END, T) if s_t[t] < ready_at_end), None)
        res["hpa"] = {
            "desired_first_change_s": None if first_change is None else first_change - JUMP_BEGIN,
            "desired_first_change_to": None if first_change is None else desired[first_change],
            "peak_ready": peak,
            "peak_ready_s": None if peak_at is None or peak_at >= JUMP_END else peak_at - JUMP_BEGIN,
            "peak_ready_reached_at_s": None if peak_at is None else peak_at - JUMP_BEGIN,
            "p99_recover_s": None if recover is None else recover - JUMP_BEGIN,
            "p99_threshold_ms": round(threshold, 2),
            "scale_down_s": None if down is None else down - JUMP_END,
            "ready_at_jump_end": ready_at_end,
        }
    return res


def plot_elasticity(results, p99_0, out):
    names = [n for n in ("fixed", "hpa") if n in results]
    titles = {"fixed": "без автомасштабування (3 репліки)", "hpa": "HPA, ціль 80 % CPU"}
    fig, axes = plt.subplots(4, len(names), figsize=(7 * len(names), 13), sharex=True, sharey="row", squeeze=False)
    for j, name in enumerate(names):
        e = results[name]["series"]
        t = list(range(len(e["z"])))
        ax = axes[0][j]
        ax.plot(t, e["z"], color="#555555", drawstyle="steps-post", label="задана z(t)")
        ax.plot(t, e["achieved"], color="#1f6feb", linewidth=0.9, label="досягнута (2xx)")
        ax.set_title(titles[name])
        ax.set_ylabel("запитів/с")
        ax = axes[1][j]
        ax.plot(t, [v if v is not None else float("nan") for v in e["p99"]], color="#d1242f", linewidth=0.9,
                label="p99 клієнта за секунду")
        ax.axhline(2 * p99_0, color="#555555", linestyle=":", label=f"поріг 2·p99₀ = {2 * p99_0:.1f} мс")
        ax.set_yscale("log")
        ax.set_ylabel("мс")
        ax = axes[2][j]
        ax.plot(t, e["d"], color="#555555", drawstyle="steps-post", label="потрібно d(t)")
        ax.plot(t, e["s"], color="#2da44e", drawstyle="steps-post", linewidth=2, label="готові s(t)")
        if any(v is not None for v in e["desired"]):
            ax.plot(t, [v if v is not None else float("nan") for v in e["desired"]], color="#e3a008",
                    drawstyle="steps-post", linestyle="--", label="бажані (HPA)")
        ax.set_ylabel("реплік")
        ax.set_ylim(0, R + 0.7)
        ax = axes[3][j]
        for pod, pts in sorted(e["cpu_pods"].items()):
            ax.plot([a for a, _ in pts], [b for _, b in pts], linewidth=0.6, alpha=0.6)
        ax.plot(t, [v if v is not None else float("nan") for v in e["cpu_avg"]], color="#000000", linewidth=1.6,
                label="середнє за подами")
        ax.axhline(80, color="#e3a008", linestyle=":", label="ціль HPA 80 %")
        ax.set_ylabel("CPU / requests, %")
        ax.set_xlabel("секунди від початку профілю")
        for row in axes:
            row[j].axvspan(JUMP_BEGIN, JUMP_END, color="#e3a008", alpha=0.08)
            row[j].grid(alpha=0.3)
            row[j].legend(fontsize=7, loc="upper right")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def drain_result(base):
    base = Path(base)
    if not (base / "meta.json").exists():
        return None
    meta = json.loads((base / "meta.json").read_text())
    tl = list(csv.DictReader(open(base / "timeline.csv")))
    load = load_run(base / "load")
    seg = load["summary"]["segments"]
    start = meta["drain"]["start"]
    after = [r for r in tl if float(r["t"]) >= start]
    rows = load["seconds"]
    p99s = [float(r["p99_ms"]) for r in rows[int(start):] if r["p99_ms"]]
    below = [float(r["t"]) for r in after if int(r["ready"]) < 3]
    win = rows[int(start):int(start) + 30]
    win_p99 = [float(r["p99_ms"]) for r in win if r["p99_ms"]]
    late = rows[int(start) + 30:]
    return {
        "drain_window_30s": {
            "errors": sum(int(r["non_2xx"]) + int(r["conn_errors"]) for r in win),
            "achieved_mean": round(statistics.fmean(int(r["achieved"]) for r in win), 1),
            "p99_per_second_max_ms": max(win_p99) if win_p99 else None,
            "p99_per_second_median_ms": round(statistics.median(win_p99), 2) if win_p99 else None,
            "seconds_p99_over_1s": sum(v > 1000 for v in win_p99),
        },
        "after_window": {
            "errors": sum(int(r["non_2xx"]) + int(r["conn_errors"]) for r in late),
            "error_seconds": [int(r["second"]) for r in late if int(r["non_2xx"]) + int(r["conn_errors"]) > 0],
        },
        "meta": meta, "before": seg[0], "during": seg[1],
        "ready_min": min(int(r["ready"]) for r in after),
        "seconds_below_R": len(below),
        "recovered_at_s": next((round(float(r["t"]) - start, 1) for r in after
                                if float(r["t"]) > (below[-1] if below else start) and int(r["ready"]) == 3), None),
        "max_p99_per_second_ms": max(p99s) if p99s else None,
        "errors_by_kind": load["summary"]["error_kinds"],
        "timeline": tl, "seconds": rows,
    }


def plot_drain(results, out):
    fig, axes = plt.subplots(3, 1, figsize=(11, 10), sharex=True)
    colors = {"no": "#d1242f", "yes": "#1f6feb"}
    for key, r in results.items():
        start = r["meta"]["drain"]["start"]
        label = "з PDB" if key == "yes" else "без PDB"
        tl = r["timeline"]
        axes[0].plot([float(x["t"]) for x in tl], [int(x["ready"]) for x in tl], drawstyle="steps-post",
                     color=colors[key], linewidth=2, label=f"{label}: готові поди")
        sec = r["seconds"]
        axes[1].plot([int(x["second"]) for x in sec], [float(x["p99_ms"]) if x["p99_ms"] else float("nan") for x in sec],
                     color=colors[key], linewidth=0.9, label=f"{label}: p99 клієнта")
        axes[2].plot([int(x["second"]) for x in sec], [int(x["non_2xx"]) + int(x["conn_errors"]) for x in sec],
                     color=colors[key], linewidth=0.9, label=f"{label}: помилки за секунду")
        for ax in axes:
            ax.axvline(start, color=colors[key], linestyle=":")
            ax.axvline(r["meta"]["drain"]["end"], color=colors[key], linestyle="--", alpha=0.5)
    axes[0].set_ylabel("готових подів")
    axes[0].set_ylim(0, 3.5)
    axes[1].set_yscale("log")
    axes[1].set_ylabel("мс")
    axes[2].set_ylabel("не 2xx + помилки")
    axes[2].set_xlabel("секунди від початку навантаження (пунктир - початок drain, штрих - кінець)")
    for ax in axes:
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def strip(obj):
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k not in ("series", "timeline", "seconds")}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    return obj


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="results")
    parser.add_argument("--charts", default="docs/charts")
    args = parser.parse_args()
    res, charts = Path(args.results), Path(args.charts)
    charts.mkdir(parents=True, exist_ok=True)
    out = {}

    runs = {n: load_run(res / f"n{n}") for n in range(1, R + 1)}
    runs = {n: r for n, r in runs.items() if r}
    table = {n: series_row(n, r) for n, r in runs.items()}
    out["steps_series"] = table
    print("N  X(N)     std    step   p99c     p99s     thr_mean thr_max  I      unreached")
    for n, t in table.items():
        ps = t.get("p99_server") or {}
        print(f"{n}  {t['X']:<8} {t['std']:<6} {t['step']:<6.0f} {t['p99_client_ms']:<8} {ps.get('ms', '-'):<8} "
              f"{t.get('throttling_mean', '-'):<8} {t.get('throttling_max', '-'):<8} {t.get('imbalance', '-'):<6} "
              f"{t['unreached']}")
        for tag, e in t.get("extremes", {}).items():
            print(f"     {tag}: {e['pod']} {e['rps']} rps, p99 server {e['p99_server'] and e['p99_server']['ms']} ms, "
                  f"throttling {e['throttling']}")
    if R in runs:
        plot_pods(runs[R], table[R], charts / f"pods_n{R}.png")
    for n in runs:
        if n != R:
            plot_pods(runs[n], table[n], charts / f"pods_n{n}.png")

    if len(table) == R:
        fit = usl_fit({n: t["X"] for n, t in table.items() if not t["unreached"] or n == 1})
        out["usl"] = fit
        lab2 = json.loads(LAB2.read_text())
        lab2_fit = {"sigma": lab2["sigma"], "kappa": lab2["kappa"]}
        out["usl_lab2"] = {"sigma": lab2["sigma"], "kappa": lab2["kappa"], "n_star": lab2["n_star"],
                           "X": {n: {"X": v["X"], "std": v["std"]} for n, v in lab2["X"].items()}}
        plot_usl(table, fit, lab2["X"], lab2_fit, charts / "usl.png")
        print(f"USL k8s: sigma = {fit['sigma']:.4f}, kappa = {fit['kappa']:.5f}, N* = {fit['n_star']} ({fit['note']})")
        print(f"USL compose: sigma = {lab2['sigma']:.4f}, kappa = {lab2['kappa']:.5f}, N* = {lab2['n_star']:.2f}")

        xs = {n: t["X"] for n, t in table.items()}
        el = {}
        fixed = load_run(res / "fixed")
        if fixed and fixed["prom"]:
            p99_0 = fixed["summary"]["segments"][0]["p99_ms"]
            p99_jump = fixed["summary"]["segments"][1]["p99_ms"]
            out["p99_0_ms"], out["p99_jump_fixed_ms"] = p99_0, p99_jump
            el["fixed"] = elasticity(fixed, xs, p99_0, p99_jump)
            hpa = load_run(res / "hpa")
            if hpa and hpa["prom"]:
                el["hpa"] = elasticity(hpa, xs, p99_0, p99_jump)
            plot_elasticity(el, p99_0, charts / "elasticity.png")
            print(f"p99_0 = {p99_0} ms, p99 jump (fixed) = {p99_jump} ms")
            print("run    theta_U theta_O a_U    a_O    W    violations non2xx")
            for name, e in el.items():
                print(f"{name:<6} {e['theta_U']:<7.3f} {e['theta_O']:<7.3f} {e['a_U']:<6.3f} {e['a_O']:<6.3f} "
                      f"{e['W']:<4} {e['violations']:<10} {e['non_2xx']}")
                if "hpa" in e:
                    print("       ", e["hpa"])
        out["elasticity"] = el

    drains = {k: drain_result(res / f"drain_{'pdb' if k == 'yes' else 'nopdb'}") for k in ("no", "yes")}
    drains = {k: v for k, v in drains.items() if v}
    if drains:
        plot_drain(drains, charts / "drain.png")
        out["drain"] = drains
        for k, d in drains.items():
            print(f"drain pdb={k}: ready_min {d['ready_min']}, seconds<R {d['seconds_below_R']}, "
                  f"drain took {d['meta']['drain']['end'] - d['meta']['drain']['start']:.1f}s, "
                  f"before p99 {d['before']['p99_ms']} / during p99 {d['during']['p99_ms']} ms, "
                  f"errors {d['during']['non_2xx']}, max p99/s {d['max_p99_per_second_ms']}, kinds {d['errors_by_kind']}")

    attempts = {}
    for base in sorted(list(res.glob("rejected/*")) + [res / "fixed", res / "hpa"]):
        summary = base / "summary.json"
        if not summary.exists():
            summary = base / "load" / "summary.json"
        if base.is_dir() and summary.exists():
            s = json.loads(summary.read_text())
            attempts[str(base.relative_to(res))] = {"segments": s["segments"], "error_kinds": s["error_kinds"]}
    out["attempts"] = attempts
    print("attempts:")
    for name, a in attempts.items():
        print(f"  {name:<45}", " | ".join(f"{g['target_rps']:.0f}: {g['achieved_mean']} rps, p99 {g['p99_ms']} ms, "
                                           f"bad {g['non_2xx']}" for g in a["segments"]))

    (res / "analysis.json").write_text(json.dumps(strip(out), ensure_ascii=False, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()

"""Tables 1-6 of the assignment (medians of three runs) and charts from results/*.json."""
import json
import platform
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
CHARTS = ROOT / "docs" / "charts"
LABEL = {"global": "Глобальний замок", "rw": "RWLock"}
ROLE_UA = {"write": "Запис", "read": "Читання"}


def load(mode, lock):
    return [json.loads(p.read_text()) for p in sorted(RES.glob(f"{mode}_{lock}_*.json"))]


def med(values):
    return statistics.median(values)


def role_median(runs, role, key):
    return med([r["roles"][role][key] for r in runs])


def ratio(rw, g):
    """RW / G; when the global-lock value is zero, the difference RW - G (rule of the assignment)."""
    if g == 0:
        return f"{rw - g:+.4g} (різниця)"
    return f"{rw / g:.3f}"


def fmt(v, digits=2):
    if v is None:
        return "-"
    if abs(v) >= 1000:
        return f"{v:,.0f}".replace(",", " ")
    return f"{v:.{digits}f}"


def main():
    CHARTS.mkdir(parents=True, exist_ok=True)
    x1 = {lock: load("x1", lock) for lock in ("global", "rw")}
    runs = {lock: load("run", lock) for lock in ("global", "rw")}
    first = runs["global"][0]
    n, n_w, n_r, d = first["threads"], first["n_w"], first["n_r"], first["duration"]
    out, md = {}, []

    X1 = {lock: med([r["X_1"] for r in x1[lock]]) for lock in x1}
    t_cs = {role: med([r["roles"][role]["t_cs_us"] for r in x1["global"]]) for role in ("write", "read")}
    t_bar = (n_w * t_cs["write"] + n_r * t_cs["read"]) / n
    inv_ok = all(r["invariants_after"]["ok"] and r["invariants_before"]["ok"] for lock in runs for r in runs[lock] + x1[lock])
    ro = json.loads((RES / "check_readonly.json").read_text())

    md.append("Таблиця 1. Стенд\n")
    md.append("| n | n_w | n_r | Мова і версія | Логічних ядер | X_1, оп/с | Інваріанти після прогонів кроків 2 і 3 |")
    md.append("|---|---|---|---|---|---|---|")
    md.append(f"| {n} | {n_w} | {n_r} | Python {first['python']} (CPython, GIL увімкнено) | {first['logical_cpus']} | "
              f"{fmt(X1['global'])} (глобальний замок), {fmt(X1['rw'])} (RWLock) | "
              f"{'виконані в усіх ' + str(sum(len(v) for v in runs.values()) + sum(len(v) for v in x1.values())) + ' прогонах' if inv_ok else 'ПОРУШЕНІ'} |")

    def role_table(lock, with_tcs):
        rr = runs[lock]
        rows = []
        head = "| Роль | Операцій за D | X, оп/с | p50, мкс | p99, мкс | Найбільший t_wait, мс | w | cv |" + (" t_cs, мкс |" if with_tcs else "")
        rows.append(head)
        rows.append("|" + "---|" * (head.count("|") - 1))
        tab = {}
        for role in ("write", "read"):
            t = {k: role_median(rr, role, k) for k in ("ops", "X", "p50_us", "p99_us", "wait_max_ms", "w", "cv", "h",
                                                          "x_per_thread", "phi", "t_cs_us")}
            tab[role] = t
            rows.append(f"| {ROLE_UA[role]} | {fmt(t['ops'])} | {fmt(t['X'])} | {fmt(t['p50_us'])} | {fmt(t['p99_us'])} | "
                        f"{fmt(t['wait_max_ms'], 3)} | {fmt(t['w'], 3)} | {fmt(t['cv'], 4)} |"
                        + (f" {fmt(t_cs[role], 2)} |" if with_tcs else ""))
        tot_ops = med([r["roles"]["write"]["ops"] + r["roles"]["read"]["ops"] for r in rr])
        tot_x = med([r["X"] for r in rr])
        tab["total"] = {"ops": tot_ops, "X": tot_x}
        rows.append(f"| Разом | {fmt(tot_ops)} | {fmt(tot_x)} | | | | | |" + (f" t̄_cs = {fmt(t_bar, 2)} |" if with_tcs else ""))
        return rows, tab

    md.append("\nТаблиця 2. Глобальний замок\n")
    rows, g = role_table("global", True)
    md += rows
    md.append("\nТаблиця 3. RWLock\n")
    rows, rw = role_table("rw", False)
    md += rows

    md.append("\nТаблиця 4. Операції потоків за D секунд (медіана трьох прогонів для кожного потоку)\n")
    md.append("| Потік | Роль | Глобальний замок | RWLock |")
    md.append("|---|---|---|---|")
    per_thread = {}
    for j in range(1, n + 1):
        vals = {lock: med([next(t["ops"] for t in r["per_thread"] if t["thread"] == j) for r in runs[lock]])
                for lock in runs}
        per_thread[j] = vals
        md.append(f"| {j} | {'запис' if j % 2 else 'читання'} | {fmt(vals['global'])} | {fmt(vals['rw'])} |")

    S_g, S_rw = g["total"]["X"] / X1["global"], rw["total"]["X"] / X1["rw"]
    rho = g["total"]["X"] * t_bar / 1e6
    # load of the lock as actually measured: time between acquire and release over D, both roles
    rho_measured = med([sum(r["roles"][role]["t_cs_us"] * r["roles"][role]["ops"] for role in ("write", "read"))
                        / 1e6 / d for r in runs["global"]])
    phi_w = g["write"]["phi"]
    t_bar_mix = phi_w * t_cs["write"] + (1 - phi_w) * t_cs["read"]
    metrics = [
        ("S(n), глобальний замок", "X(G) / X_1", S_g),
        ("E(n), глобальний замок", "S(n) / n", S_g / n),
        ("S(n), RWLock", "X(RW) / X_1", S_rw),
        ("E(n), RWLock", "S(n) / n", S_rw / n),
        ("S_w", "X_w(RW) / X_w(G)", rw["write"]["X"] / g["write"]["X"]),
        ("S_r", "X_r(RW) / X_r(G)", rw["read"]["X"] / g["read"]["X"]),
        ("S_Σ", "X(RW) / X(G)", rw["total"]["X"] / g["total"]["X"]),
        ("ρ, завантаженість глобального замка", "X(G) · t̄_cs", rho),
    ]
    md.append("\nТаблиця 5. Метрики прискорення\n")
    md.append("| Метрика | Формула | Значення |")
    md.append("|---|---|---|")
    for name, formula, v in metrics:
        md.append(f"| {name} | {formula} | {v:.3f} |")
    md.append(f"\nМежа 1 / t̄_cs = {fmt(1e6 / t_bar)} оп/с при t̄_cs = {t_bar:.3f} мкс; X(G) = {fmt(g['total']['X'])} оп/с. "
              f"Виміряна частка часу, коли глобальний замок утримується: {rho_measured:.3f}. "
              f"t_cs, зважений фактичними частками операцій (φ_w = {phi_w:.3f}): {t_bar_mix:.3f} мкс, межа {fmt(1e6 / t_bar_mix)} оп/с.")

    md.append("\nТаблиця 6. Порівняння доступу до даних\n")
    md.append("| Показник | Глобальний замок: запис | Глобальний замок: читання | RWLock: запис | RWLock: читання | RW / G: запис | RW / G: читання |")
    md.append("|---|---|---|---|---|---|---|")
    rows6 = [("X, оп/с", "X", 0), ("x, на один потік ролі, оп/с", "x_per_thread", 0), ("p50, мкс", "p50_us", 2),
             ("p99, мкс", "p99_us", 2), ("h = p99 / p50", "h", 2), ("Найбільший t_wait, мс", "wait_max_ms", 3),
             ("w", "w", 3), ("cv", "cv", 4), ("φ, частка ролі в усіх операціях", "phi", 3)]
    tab6 = {}
    for label, key, dg in rows6:
        gw, gr, rww, rwr = g["write"][key], g["read"][key], rw["write"][key], rw["read"][key]
        tab6[key] = [gw, gr, rww, rwr]
        md.append(f"| {label} | {fmt(gw, dg)} | {fmt(gr, dg)} | {fmt(rww, dg)} | {fmt(rwr, dg)} | "
                  f"{ratio(rww, gw)} | {ratio(rwr, gr)} |")

    md.append("\nРозкид між трьома прогонами (мінімум - максимум)\n")
    md.append("| Спосіб | X, оп/с | X_w | X_r | p99 запису, мкс | p99 читання, мкс | найбільший t_wait запису, мс |")
    md.append("|---|---|---|---|---|---|---|")
    for lock in ("global", "rw"):
        rr = runs[lock]
        rng = lambda f: f"{fmt(min(map(f, rr)))} - {fmt(max(map(f, rr)))}"
        md.append(f"| {LABEL[lock]} | {rng(lambda r: r['X'])} | {rng(lambda r: r['roles']['write']['X'])} | "
                  f"{rng(lambda r: r['roles']['read']['X'])} | {rng(lambda r: r['roles']['write']['p99_us'])} | "
                  f"{rng(lambda r: r['roles']['read']['p99_us'])} | {rng(lambda r: r['roles']['write']['wait_max_ms'])} |")
    md.append(f"\nX_1 по прогонах: глобальний замок {[round(r['X_1']) for r in x1['global']]}, "
              f"RWLock {[round(r['X_1']) for r in x1['rw']]}; t_cs запису {[round(r['roles']['write']['t_cs_us'], 3) for r in x1['global']]} мкс, "
              f"читання {[round(r['roles']['read']['t_cs_us'], 3) for r in x1['global']]} мкс.")
    md.append(f"\nНавантаження хоста перед прогонами (load average за 1 хв): "
              f"{sorted(round(r['loadavg_before'][0], 2) for lock in runs for r in runs[lock] + x1[lock])}.")
    md.append(f"\nПеревірка читань (RWLock): {ro['readers']} потоки лише читання {ro['seconds']} с, "
              f"{ro['read_ops']} операцій; відбиток усіх {ro['nodes_fingerprinted']} вузлів до і після однаковий: "
              f"{ro['fingerprint_equal']}; версія під замком читання змінилась {ro['version_moved_under_read_lock']} разів. "
              f"З двома писцями одночасно: {ro['with_writers']['read_ops']} читань, версія під замком читання змінилась "
              f"{ro['with_writers']['version_moved_under_read_lock']} разів, інваріанти {ro['with_writers']['invariants']['ok']}.")

    (RES / "tables.md").write_text("\n".join(md) + "\n")
    out = {"X_1": X1, "t_cs_us": t_cs, "t_bar_cs_us": t_bar, "bound": 1e6 / t_bar, "rho_formula": rho,
           "rho_measured": rho_measured, "t_bar_mix_us": t_bar_mix, "global": g, "rw": rw,
           "metrics": {name: v for name, _, v in metrics}, "per_thread": per_thread, "readonly": ro,
           "invariants_ok": inv_ok}
    (RES / "analysis.json").write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n")
    plot(runs)
    print("\n".join(md))


def plot(runs):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=False)
    colors = {"global": "#1f6feb", "rw": "#d1242f"}
    for ax, role in zip(axes, ("write", "read")):
        for lock in ("global", "rw"):
            ordered = sorted(runs[lock], key=lambda r: r["X"])
            r = ordered[len(ordered) // 2]
            curve = r["roles"][role]["tau_curve_us"]
            qs = [i / 400 for i in range(len(curve))]
            ax.plot(curve, qs, color=colors[lock], label=f"{LABEL[lock]}: p50 {r['roles'][role]['p50_us']:.1f}, "
                                                          f"p99 {r['roles'][role]['p99_us']:.1f} мкс")
        ax.set_xscale("log")
        ax.set_xlabel("тривалість операції τ, мкс (логарифмічна шкала)")
        ax.set_ylabel("частка операцій ≤ τ")
        ax.set_title(f"{ROLE_UA[role]}", fontsize=11)
        ax.grid(alpha=0.3, which="both")
        ax.legend(fontsize=9, loc="lower right")
    fig.tight_layout()
    fig.savefig(CHARTS / "tau_cdf.png", dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    labels = ["X_w", "X_r", "X"]
    for i, lock in enumerate(("global", "rw")):
        rr = runs[lock]
        vals = [med([r["roles"]["write"]["X"] for r in rr]), med([r["roles"]["read"]["X"] for r in rr]),
                med([r["X"] for r in rr])]
        xs = [j + (i - 0.5) * 0.38 for j in range(3)]
        bars = ax.bar(xs, [v / 1000 for v in vals], width=0.38, color=colors[lock], label=LABEL[lock])
        for b, v in zip(bars, vals):
            ax.annotate(f"{v / 1000:.0f}", (b.get_x() + b.get_width() / 2, b.get_height()), ha="center",
                        va="bottom", fontsize=9)
    ax.set_xticks(range(3))
    ax.set_xticklabels(labels)
    ax.set_ylabel("тис. операцій за секунду")
    ax.grid(alpha=0.3, axis="y")
    ax.legend()
    fig.tight_layout()
    fig.savefig(CHARTS / "throughput.png", dpi=130)
    plt.close(fig)


if __name__ == "__main__":
    main()

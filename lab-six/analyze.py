import glob
import json
import os
import platform
import re
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import torch

from common import B, DATASET, DATASETS, K_C, LR0, N_TH, N_TRAIN, P_MAX, S_F, WIDTH, WIDTHS, C_PHYS, n_params_formula

ROOT = Path(__file__).resolve().parent
RES = ROOT / "results"
CHARTS = ROOT / "docs" / "charts"
med = statistics.median


def j(name):
    return json.loads((RES / name).read_text())


def speed_runs():
    g = defaultdict(dict)
    for p in glob.glob(str(RES / "speed" / "*.json")):
        m = re.match(r"(.+)_r(\d+)\.json$", os.path.basename(p))
        g[m.group(1)][int(m.group(2))] = json.loads(Path(p).read_text())
    return g


def steps_log(name):
    out = []
    for line in (RES / name).read_text().splitlines():
        out.append(json.loads(line))
    return out


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def f(v, d=2):
    if v is None:
        return "-"
    if isinstance(v, str):
        return v
    if abs(v) >= 1000:
        return f"{v:,.0f}".replace(",", " ")
    return f"{v:.{d}f}"


def main():
    CHARTS.mkdir(parents=True, exist_ok=True)
    md, out = [], {}
    name, K, C = DATASETS[DATASET]
    c1, c2, h = WIDTHS[WIDTH]
    n_params = n_params_formula(C, c1, c2, h, K)
    M = 4 * n_params
    sp = speed_runs()

    def t(name):
        return med([r["t_step_ms"] for r in sp[name].values()])

    def X(name):
        return med([r["X"] for r in sp[name].values()])

    def comm(sync_name, ns_name):
        rounds = sorted(set(sp[sync_name]) & set(sp[ns_name]))
        return med([sp[sync_name][r]["t_step_ms"] - sp[ns_name][r]["t_step_ms"] for r in rounds])

    cpu = sh("sysctl -n machdep.cpu.brand_string")
    perf = sh("sysctl -n hw.perflevel0.physicalcpu")
    eff = sh("sysctl -n hw.perflevel1.physicalcpu")
    mem = int(sh("sysctl -n hw.memsize")) / 1e9
    md += ["Таблиця 1. Стенд", "",
           "| Процесор | Фізичних ядер C_phys | Логічних ядер | Памʼять, ГБ | ОС | Python | PyTorch | n_th |",
           "|---|---|---|---|---|---|---|---|",
           f"| {cpu} ({perf} продуктивних + {eff} енергоефективних) | {C_PHYS} | {os.cpu_count()} | {mem:.1f} | "
           f"macOS {platform.mac_ver()[0]} | {platform.python_version()} | {torch.__version__} (CPU, gloo) | "
           f"⌊{C_PHYS} / {P_MAX}⌋ = {N_TH} |", "",
           "| Датасет | K | C | c1 / c2 / h | n_params | M, МіБ | N | B | P_max |",
           "|---|---|---|---|---|---|---|---|---|",
           f"| {DATASET} {name} | {K} | {C} | {c1} / {c2} / {h} | {n_params:,} | {M / 2**20:.2f} ({M:,} Б) | "
           f"{N_TRAIN} | {B} | {P_MAX} |".replace(",", " ")]

    s1, s2 = j("s1_train.json"), j("s2_train_ddp.json")
    eqm, eqd = j("s2_equiv_manual.json"), j("s2_equiv_ddp.json")
    man = f"manual_P{P_MAX}_B{B}"
    ddp, ns = f"ddp_P{P_MAX}_B{B}", f"ddp_nosync_P{P_MAX}_B{B}"
    t_sync = med([r["t_sync_ms"] for r in sp[man].values()])
    t_comm = comm(ddp, ns)
    rows2 = [("1 процес, батч B", t("single_B96"), 0.0, 0.0, X("single_B96")),
             (f"{P_MAX} процеси, ручна синхронізація", t(man), t_sync, t_sync / t(man), X(man)),
             (f"{P_MAX} процеси, DDP", t(ddp), t_comm, t_comm / t(ddp), X(ddp)),
             (f"{P_MAX} процеси, DDP, no_sync", t(ns), 0.0, 0.0, X(ns))]
    md += ["", "Таблиця 2. Синхронізація градієнтів", "",
           "| Режим | t_step, мс | Час обміну на крок, мс | Частка кроку в обміні | X, прикл./с |",
           "|---|---|---|---|---|"]
    md += [f"| {r[0]} | {r[1]:.2f} | {r[2]:.2f} | {r[3]:.3f} | {f(r[4])} |" for r in rows2]
    md += ["", "| max \\|g − ḡ\\| / max \\|g\\| | Різниця сум ваг між процесами після 50 кроків | ACC, 1 процес | "
               f"ACC, DDP на {P_MAX} | AUC, 1 процес | AUC, DDP на {P_MAX} |", "|---|---|---|---|---|---|",
           f"| {eqm['grad_ratio_max']:.2e} | {eqm['weight_sum_spread']:.1e} (ручна), {eqd['weight_sum_spread']:.1e} (DDP) | "
           f"{s1['ACC']:.3f} | {s2['ACC']:.3f} | {s1['AUC']:.4f} | {s2['AUC']:.4f} |"]
    md += ["", "Відношення max |g − ḡ| / max |g| за тензорами: " +
           ", ".join(f"{v:.1e}" for v in eqm["grad_ratio_per_tensor"])]

    x1 = X(f"ddp_P1_B{B}")
    rows3 = []
    md += ["", f"Таблиця 3. Strong scaling, B = {B}", "",
           "| P | b | t_step, мс | t_step,ns, мс | X, прикл./с | S | E | e | c |", "|---|---|---|---|---|---|---|---|---|"]
    for P in range(1, P_MAX + 1):
        a, n = f"ddp_P{P}_B{B}", f"ddp_nosync_P{P}_B{B}"
        x = X(a)
        S = x / x1
        E = S / P
        e = (1 / S - 1 / P) / (1 - 1 / P) if P > 1 else None
        tc = comm(a, n)
        c = tc / t(a)
        rows3.append({"P": P, "b": B // P, "t_step": t(a), "t_ns": t(n), "X": x, "S": S, "E": E, "e": e, "c": c,
                      "t_comm": tc})
        md.append(f"| {P} | {B // P} | {t(a):.2f} | {t(n):.2f} | {f(x)} | {S:.3f} | {E:.3f} | "
                  f"{'-' if e is None else f'{e:.3f}'} | {c:.3f} |")

    b = B // P_MAX
    x1w = X(f"ddp_P1_B{b}")
    rows4 = []
    md += ["", f"Таблиця 4. Weak scaling, b = {b}", "",
           "| P | B_P | t_step, мс | X, прикл./с | S_w | E_w |", "|---|---|---|---|---|---|"]
    for P in range(1, P_MAX + 1):
        a = f"ddp_P{P}_B{P * b}"
        x = X(a)
        rows4.append({"P": P, "B_P": P * b, "t_step": t(a), "X": x, "S_w": x / x1w, "E_w": x / x1w / P})
        md.append(f"| {P} | {P * b} | {t(a):.2f} | {f(x)} | {x / x1w:.3f} | {x / x1w / P:.3f} |"
                  + (" (з таблиці 3)" if P == P_MAX else ""))

    q1, q2, q3 = j("s4_train_single_b.json"), s2, j("s4_train_ddp_linear.json")
    md += ["", "Таблиця 5. Якість за різних глобальних батчів", "",
           "| Режим | B_P | η | Кроків оптимізатора за n_ep епох | ACC | AUC | Найбільша втрата за останню епоху |",
           "|---|---|---|---|---|---|---|",
           f"| P = 1 | {b} | {q1['lr']} | {q1['steps_total']} | {q1['ACC']:.3f} | {q1['AUC']:.4f} | {q1['max_loss_last_epoch']:.4f} |",
           f"| P = {P_MAX}, з кроку 2 | {B} | {q2['lr']} | {q2['steps_total']} | {q2['ACC']:.3f} | {q2['AUC']:.4f} | {q2['max_loss_last_epoch']:.4f} |",
           f"| P = {P_MAX} | {B} | {q3['lr']} | {q3['steps_total']} | {q3['ACC']:.3f} | {q3['AUC']:.4f} | {q3['max_loss_last_epoch']:.4f} |",
           f"| 1 процес, з кроку 1 (для порівняння) | {B} | {s1['lr']} | {s1['steps_total']} | {s1['ACC']:.3f} | {s1['AUC']:.4f} | {s1['max_loss_last_epoch']:.4f} |"]

    def failure(prefix):
        log = steps_log(f"{prefix}_train_fail.loss.jsonl")
        marker = j(f"{prefix}_failed.marker.json")
        t_f = marker["t_f"]
        ckpts = [e for e in log if e["event"] == "ckpt" and e["attempt"] == 0 and e["t"] <= t_f]
        last = ckpts[-1]
        first_after = next(e for e in log if e["event"] == "step" and e["attempt"] == 1)
        restore = next(e for e in log if e["event"] == "restore")
        done0 = [e["step"] for e in log if e["event"] == "step" and e["attempt"] == 0]
        redone = [s for s in done0 if s > last["step"]]
        t_r = first_after["t"] - t_f
        loss = {}
        for e in log:
            if e["event"] == "step":
                loss[(e["step"], e["attempt"])] = e["loss"]
        return {"last_ckpt": last["step"], "t_c": last["t"], "t_f": t_f, "redone": redone, "t_r": t_r,
                "t_f_minus_t_c": t_f - last["t"], "t_lost": (t_f - last["t"]) + t_r,
                "t_restore_read": restore["t"] - t_f, "loss": loss, "res": j(f"{prefix}_train_fail.json")}

    fw, fn = failure("s5"), failure("s5_nowarm")
    ref = {e["step"]: e["loss"] for e in steps_log("s2_train_ddp.loss.jsonl") if e["event"] == "step"}
    steps = list(range(S_F - 2, S_F + 3))

    def final_loss(fr, s):
        return fr["loss"].get((s, 1), fr["loss"].get((s, 0)))

    md += ["", "Таблиця 6. Відмова і відновлення", "",
           "| K_c | s_f | Крок останньої контрольної точки | Кроків виконано повторно | t_lost, с |", "|---|---|---|---|---|",
           f"| {K_C} | {S_F} | {fw['last_ckpt']} | {len(fw['redone'])} ({fw['redone'][0]}-{fw['redone'][-1]}) | "
           f"{fw['t_lost']:.2f} |", "",
           "| Крок | " + " | ".join(f"{s}" for s in steps) + " | ACC після n_ep епох |",
           "|---|" + "---|" * (len(steps) + 1),
           "| Втрата, прогін з відмовою | " + " | ".join(f"{final_loss(fw, s):.7f}" for s in steps) + f" | {fw['res']['ACC']:.3f} |",
           "| Втрата, прогін без відмови | " + " | ".join(f"{ref[s]:.7f}" for s in steps) + f" | {s2['ACC']:.3f} |",
           "| Втрата, відновлення без холостої ітерації DDP | " + " | ".join(f"{final_loss(fn, s):.7f}" for s in steps)
           + f" | {fn['res']['ACC']:.3f} |"]
    same_all = all(fw["loss"].get((s, 1), fw["loss"].get((s, 0))) == ref[s] for s in ref)
    diff_nowarm = [s for s in ref if final_loss(fn, s) != ref[s]]
    redo_equal = all(fw["loss"][(s, 0)] == fw["loss"][(s, 1)] for s in fw["redone"])
    md += ["", f"t_lost = (t_f − t_c) + t_r = {fw['t_f_minus_t_c']:.3f} + {fw['t_r']:.3f} = {fw['t_lost']:.3f} с; "
               f"від відмови до читання контрольної точки {fw['t_restore_read']:.3f} с. "
               f"Втрата всіх {len(ref)} кроків прогону з відмовою збігається з прогоном без відмови побітово: {same_all}; "
               f"повторно виконані кроки збігаються з першим виконанням: {redo_equal}. "
               f"Без холостої ітерації DDP відрізняються {len(diff_nowarm)} з {len(ref)} кроків, перший - {diff_nowarm[0]}, "
               f"найбільша різниця {max(abs(final_loss(fn, s) - ref[s]) for s in ref):.1e}; "
               f"t_lost цього прогону {fn['t_lost']:.2f} с."]

    md += ["", "Розкид t_step за раундами вимірювань (мс, мінімум - медіана - максимум)", "",
           "| Конфігурація | t_step | X, прикл./с |", "|---|---|---|"]
    for k in sorted(sp):
        ts = [r["t_step_ms"] for r in sp[k].values()]
        xs = [r["X"] for r in sp[k].values()]
        md.append(f"| {k} | {min(ts):.2f} - {med(ts):.2f} - {max(ts):.2f} | {f(min(xs))} - {f(med(xs))} - {f(max(xs))} |")
    md.append(f"\nКошики DDP: {sp[ddp][1].get('ddp_buckets')}")

    (RES / "tables.md").write_text("\n".join(md) + "\n")
    out.update({"n_params": n_params, "M": M, "table2": rows2, "t_sync_ms": t_sync, "t_comm_ms": t_comm,
                "equiv": {"manual": eqm["grad_ratio_max"], "sum_spread_manual": eqm["weight_sum_spread"],
                          "sum_spread_ddp": eqd["weight_sum_spread"]},
                "table3": rows3, "table4": rows4, "quality": {"single_b": q1, "ddp_lr0": q2, "ddp_linear": q3, "single_B": s1},
                "failure": {k: v for k, v in fw.items() if k not in ("loss", "res")},
                "failure_nowarm": {k: v for k, v in fn.items() if k not in ("loss", "res")},
                "failure_same_all": same_all, "nowarm_diff_steps": len(diff_nowarm)})
    (RES / "analysis.json").write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str) + "\n")
    plots(rows3, rows4, x1, x1w)
    print("\n".join(md))


def plots(rows3, rows4, x1, x1w):
    sp = speed_runs()
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    Ps = [r["P"] for r in rows3]
    ax.plot(Ps, [P * x1 for P in Ps], ":", color="#8c959f", label="ідеал P · X(1), strong")
    for rows, key, color, label in ((rows3, f"_B{B}", "#1f6feb", f"strong, B = {B}"),
                                    (rows4, None, "#2da44e", f"weak, b = {B // P_MAX}")):
        xs = [r["X"] for r in rows]
        names = [f"ddp_P{r['P']}_B{B}" if key else f"ddp_P{r['P']}_B{r['B_P']}" for r in rows]
        lo = [r["X"] - min(v["X"] for v in sp[n].values()) for r, n in zip(rows, names)]
        hi = [max(v["X"] for v in sp[n].values()) - r["X"] for r, n in zip(rows, names)]
        ax.errorbar([r["P"] for r in rows], xs, yerr=[lo, hi], fmt="o-", color=color, capsize=4,
                    label=f"{label} (медіана, мін-макс раундів)")
    ax.plot(Ps, [P * x1w for P in Ps], ":", color="#2da44e", alpha=0.5, label="ідеал P · X(1), weak")
    ax.set_xticks(Ps)
    ax.set_xlabel("процесів P")
    ax.set_ylabel("X, прикладів за секунду")
    ax.set_ylim(0, None)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(CHARTS / "scaling.png", dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 4.2))
    for name, label, color in (("s1_train.loss.jsonl", f"1 процес, B = {B}, η = {LR0}", "#8c959f"),
                               ("s2_train_ddp.loss.jsonl", f"DDP P = {P_MAX}, B = {B}, η = {LR0}", "#1f6feb"),
                               ("s4_train_ddp_linear.loss.jsonl", f"DDP P = {P_MAX}, B = {B}, η = {LR0 * P_MAX}", "#d1242f"),
                               ("s4_train_single_b.loss.jsonl", f"1 процес, b = {B // P_MAX}, η = {LR0}", "#2da44e")):
        log = [e for e in steps_log(name) if e["event"] == "step"]
        n_e = max(e["i"] for e in log) + 1
        ax.plot([(e["epoch"] + (e["i"] + 1) / n_e) for e in log], [e["loss"] for e in log], color=color, linewidth=0.8,
                label=label, alpha=0.85)
    ax.set_xlabel("епоха")
    ax.set_ylabel("втрата на кроці")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(CHARTS / "loss.png", dpi=130)
    plt.close(fig)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Plot bench/results/results.csv -> PNG graphs (needs matplotlib)."""
import csv
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "results", "results.csv")
outdir = os.path.dirname(src)

rows = list(csv.DictReader(open(src)))
COLORS = {"threaded": "#d95f02", "epoll": "#1b9e77"}
NAMES = {"threaded": "thread-per-client", "epoll": "epoll + thread pool"}


def series(exp, key):
    d = defaultdict(list)
    for r in rows:
        if r["experiment"] != exp or r[key] in ("", "None"):
            continue
        d[r["server"]].append((int(r["connected"]), float(r[key])))
    return {k: sorted(v) for k, v in d.items()}


def line(ax, exp, key, title, ylabel, logy=False):
    for srv, pts in series(exp, key).items():
        ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=NAMES[srv], color=COLORS[srv])
    ax.set_title(title)
    ax.set_xlabel("concurrent connections")
    ax.set_ylabel(ylabel)
    ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend()


fig, axes = plt.subplots(2, 2, figsize=(12, 8))
line(axes[0][0], "scaling", "p50_ms", "Median (p50) latency", "ms", logy=True)
line(axes[0][1], "scaling", "p99_ms", "Tail (p99) latency", "ms", logy=True)
line(axes[1][0], "scaling", "server_cpu_pct", "Server CPU (100% = one core)", "% of one core")
line(axes[1][1], "scaling", "server_rss_mb", "Server memory (peak RSS)", "MB")
fig.suptitle("thread-per-client vs epoll + thread pool (open loop, 2 msg/s per client)")
fig.tight_layout()
fig.savefig(os.path.join(outdir, "scaling.png"), dpi=130)

sat = [r for r in rows if r["experiment"] == "saturation"]
if sat:
    fig, ax = plt.subplots(figsize=(6, 4))
    xs = [NAMES[r["server"]] for r in sat]
    ax.bar(xs, [float(r["msgs_sent_per_s"]) for r in sat], color=[COLORS[r["server"]] for r in sat])
    ax.set_ylabel("messages / second (closed loop)")
    ax.set_title(f"Saturated throughput, {sat[0]['connected']} clients")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "throughput.png"), dpi=130)
print("wrote graphs to", outdir)

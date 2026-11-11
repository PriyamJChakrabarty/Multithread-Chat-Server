#!/usr/bin/env python3
"""Compare thread-per-client vs epoll + thread pool.

  python3 bench/run_bench.py --build-dir build            # full sweep
  python3 bench/run_bench.py --build-dir build --quick    # small smoke run

For every (server, connection-count) it starts a fresh server, drives it with
bench/loadtest.py, samples the *server process* CPU and RSS from /proc, and
appends a row to bench/results/results.csv. Then bench/plot.py makes graphs.

Experiments
  scaling   : N clients, each 2 msg/s (open loop). Latency / CPU / memory vs N.
  saturation: closed loop on a modest N. Peak msgs/s the server sustains.
"""
import argparse
import csv
import json
import os
import resource
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
CLK = os.sysconf("SC_CLK_TCK")


def proc_cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return (int(parts[11]) + int(parts[12])) / CLK  # utime + stime


def proc_rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def proc_threads(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("Threads:"):
                return int(line.split()[1])
    return 0


def raise_limits():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = 1_000_000 if hard == resource.RLIM_INFINITY else hard
    resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    return want


def start_server(kind, build_dir, port, workers, io_threads=1):
    exe = {"epoll": "chat_server", "threaded": "chat_server_threaded"}[kind]
    cmd = [os.path.join(build_dir, exe), "--port", str(port), "--max-conns", "100000",
           "--idle", "0", "--log-level", "warn"]
    if kind == "epoll":
        cmd += ["--workers", str(workers), "--io-threads", str(io_threads)]
    p = subprocess.Popen(cmd)
    for _ in range(100):  # wait for the listener
        time.sleep(0.05)
        try:
            import socket
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return p
        except OSError:
            continue
    p.kill()
    raise RuntimeError(f"{kind} server did not start")


def run_one(kind, args, conns, closed, port):
    srv = start_server(kind, args.build_dir, port, args.workers, args.io_threads)
    try:
        cmd = [sys.executable, os.path.join(HERE, "loadtest.py"), "--port", str(port),
               "--conns", str(conns), "--procs", str(args.procs),
               "--duration", str(args.duration), "--warmup", str(args.warmup),
               "--rate", str(args.rate)]
        if closed:
            cmd.append("--closed")
        cpu0, t0 = proc_cpu_seconds(srv.pid), time.time()
        lt = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        peak_rss, peak_thr = 0.0, 0
        while lt.poll() is None:
            peak_rss = max(peak_rss, proc_rss_mb(srv.pid))
            peak_thr = max(peak_thr, proc_threads(srv.pid))
            time.sleep(0.2)
        out = lt.stdout.read()
        cpu1, t1 = proc_cpu_seconds(srv.pid), time.time()
        res = json.loads(out.strip().splitlines()[-1])
        res["server"] = kind
        res["experiment"] = "saturation" if closed else "scaling"
        res["server_cpu_pct"] = round(100 * (cpu1 - cpu0) / (t1 - t0), 1)  # of one core
        res["server_rss_mb"] = round(peak_rss, 1)
        res["server_threads"] = peak_thr
        return res
    finally:
        srv.send_signal(signal.SIGINT)
        try:
            srv.wait(timeout=15)
        except subprocess.TimeoutExpired:
            srv.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("--servers", default="threaded,epoll")
    ap.add_argument("--conns", default="100,500,1000,2000,5000,10000")
    ap.add_argument("--sat-conns", type=int, default=200)
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--warmup", type=float, default=2)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--io-threads", type=int, default=4, help="epoll reactor loops")
    ap.add_argument("--procs", type=int, default=4, help="load generator processes")
    ap.add_argument("--port", type=int, default=19000)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default=os.path.join(RESULTS, "results.csv"))
    args = ap.parse_args()
    if args.quick:
        args.conns, args.duration, args.warmup, args.sat_conns = "50,200", 3, 1, 50

    got = raise_limits()
    print(f"# RLIMIT_NOFILE soft={got}", file=sys.stderr)
    os.makedirs(RESULTS, exist_ok=True)
    rows = []
    port = args.port
    plan = [(k, int(n), False) for n in args.conns.split(",") for k in args.servers.split(",")]
    plan += [(k, args.sat_conns, True) for k in args.servers.split(",")]
    for kind, n, closed in plan:
        port += 1
        label = f"{kind:8s} conns={n:6d} {'closed' if closed else 'open  '}"
        try:
            r = run_one(kind, args, n, closed, port)
            print(f"{label} connected={r['connected']:6d} p50={r['p50_ms']}ms p99={r['p99_ms']}ms "
                  f"msgs/s={r['msgs_sent_per_s']} cpu={r['server_cpu_pct']}% rss={r['server_rss_mb']}MB "
                  f"thr={r['server_threads']}", file=sys.stderr)
            rows.append(r)
        except Exception as e:  # keep sweeping; record the failure
            print(f"{label} FAILED: {e}", file=sys.stderr)
    if rows:
        keys = ["experiment", "server", "conns_requested", "connected", "failed", "connect_seconds",
                "msgs_sent_per_s", "deliveries_per_s", "p50_ms", "p90_ms", "p99_ms", "max_ms",
                "server_cpu_pct", "server_rss_mb", "server_threads"]
        with open(args.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()

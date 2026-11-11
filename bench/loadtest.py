#!/usr/bin/env python3
"""Load generator for the chat servers (asyncio, optionally multi-process).

Each simulated client connects, joins a room (clients are sharded into rooms of
--room-size so per-message fan-out stays constant as N grows), then sends
messages carrying a timestamp. The server broadcasts every message back to the
whole room *including the sender*, so each client measures its own round-trip
latency when it sees its own message come back.

Modes
  open loop   (default): each client sends one message every 1/--rate seconds.
  closed loop (--closed): each client sends the next message as soon as its
              previous one echoed back => measures saturated throughput.
  idle        (--rate 0, no --closed): only hold connections open.

Output: one JSON object on stdout.
"""
import argparse
import asyncio
import json
import multiprocessing as mp
import random
import resource
import struct
import sys
import time


def raise_nofile(n):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = n if hard == resource.RLIM_INFINITY else min(n, hard)
    if target > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]


def frame(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">I", len(b)) + b


async def read_frame(reader):
    hdr = await reader.readexactly(4)
    (n,) = struct.unpack(">I", hdr)
    return (await reader.readexactly(n)).decode(errors="replace")


class Stats:
    def __init__(self):
        self.connected = 0
        self.failed = 0
        self.sent = 0
        self.received = 0  # deliveries of any message
        self.latencies = []  # seconds
        self.measuring = False


async def client(idx, args, stats, connect_sem, start_evt, stop_at_box):
    room = f"r{(args.base + idx) // args.room_size}"
    tag = f"c{args.base + idx}x{args.pid}"
    try:
        async with connect_sem:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(args.host, args.port), timeout=args.connect_timeout)
    except Exception:
        stats.failed += 1
        return
    stats.connected += 1
    writer.write(frame(f"/nick {tag}") + frame(f"/join {room}"))
    got_echo = asyncio.Event()
    prefix = f"[{room}] {tag}: "

    async def rx():
        try:
            while True:
                msg = await read_frame(reader)
                if stats.measuring:
                    stats.received += 1
                if msg.startswith(prefix):
                    try:
                        t0 = int(msg[len(prefix):].split(":", 1)[0])
                    except ValueError:
                        continue
                    if stats.measuring:
                        stats.latencies.append((time.perf_counter_ns() - t0) / 1e9)
                    got_echo.set()
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass

    rx_task = asyncio.create_task(rx())
    await start_evt.wait()
    pad = "x" * args.size
    try:
        if args.closed:
            while time.monotonic() < stop_at_box[0]:
                got_echo.clear()
                writer.write(frame(f"{time.perf_counter_ns()}:{pad}"))
                stats.sent += stats.measuring
                await asyncio.wait_for(got_echo.wait(), timeout=10)
        elif args.rate > 0:
            period = 1.0 / args.rate
            await asyncio.sleep(random.random() * period)  # de-synchronise clients
            nxt = time.monotonic()
            while time.monotonic() < stop_at_box[0]:
                writer.write(frame(f"{time.perf_counter_ns()}:{pad}"))
                stats.sent += stats.measuring
                nxt += period
                await asyncio.sleep(max(0, nxt - time.monotonic()))
        else:
            while time.monotonic() < stop_at_box[0]:
                await asyncio.sleep(0.2)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        pass
    finally:
        rx_task.cancel()
        try:
            writer.close()
        except Exception:
            pass


async def run_async(args):
    stats = Stats()
    start_evt = asyncio.Event()
    stop_at_box = [float("inf")]
    sem = asyncio.Semaphore(args.connect_concurrency)
    tasks = [asyncio.create_task(client(i, args, stats, sem, start_evt, stop_at_box))
             for i in range(args.conns)]
    # wait for the connection storm to settle
    t_conn = time.monotonic()
    while stats.connected + stats.failed < args.conns:
        await asyncio.sleep(0.05)
    connect_time = time.monotonic() - t_conn
    await asyncio.sleep(args.settle)  # let /nick /join + welcome traffic drain
    # Synchronise the measurement window with the other processes.
    if args.start_at:
        await asyncio.sleep(max(0, args.start_at - time.time()))
    stop_at_box[0] = time.monotonic() + args.warmup + args.duration
    start_evt.set()
    await asyncio.sleep(args.warmup)
    stats.measuring = True
    t0 = time.monotonic()
    await asyncio.sleep(args.duration)
    stats.measuring = False
    elapsed = time.monotonic() - t0
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return stats, elapsed, connect_time


def worker(args):
    raise_nofile(args.conns + 256)
    stats, elapsed, connect_time = asyncio.run(run_async(args))
    lat = stats.latencies
    if len(lat) > 200000:
        lat = random.sample(lat, 200000)
    return dict(connected=stats.connected, failed=stats.failed, sent=stats.sent,
                received=stats.received, elapsed=elapsed, connect_time=connect_time,
                latencies=lat)


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    k = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--conns", type=int, default=100, help="total client connections")
    ap.add_argument("--procs", type=int, default=1, help="load-generator processes")
    ap.add_argument("--rate", type=float, default=1.0, help="msgs/sec per client (open loop); 0 = idle")
    ap.add_argument("--closed", action="store_true", help="closed loop: saturate")
    ap.add_argument("--room-size", type=int, default=10)
    ap.add_argument("--size", type=int, default=32, help="message body bytes")
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--warmup", type=float, default=2)
    ap.add_argument("--settle", type=float, default=1.0)
    ap.add_argument("--connect-concurrency", type=int, default=200)
    ap.add_argument("--connect-timeout", type=float, default=15)
    args = ap.parse_args()

    per = args.conns // args.procs
    jobs = []
    start_at = time.time() + 5 + args.conns / 2000.0  # shared start for all procs
    for p in range(args.procs):
        a = argparse.Namespace(**vars(args))
        a.conns = per + (args.conns - per * args.procs if p == args.procs - 1 else 0)
        a.base = p * per
        a.pid = p
        a.start_at = start_at
        jobs.append(a)
    if args.procs == 1:
        results = [worker(jobs[0])]
    else:
        with mp.Pool(args.procs) as pool:
            results = pool.map(worker, jobs)

    lat = sorted(x for r in results for x in r["latencies"])
    elapsed = max(r["elapsed"] for r in results)
    sent = sum(r["sent"] for r in results)
    recv = sum(r["received"] for r in results)
    out = dict(
        conns_requested=args.conns,
        connected=sum(r["connected"] for r in results),
        failed=sum(r["failed"] for r in results),
        connect_seconds=round(max(r["connect_time"] for r in results), 3),
        mode="closed" if args.closed else ("idle" if args.rate == 0 else "open"),
        msgs_sent_per_s=round(sent / elapsed, 1) if elapsed else 0,
        deliveries_per_s=round(recv / elapsed, 1) if elapsed else 0,
        latency_samples=len(lat),
        p50_ms=round(pct(lat, 50) * 1000, 3) if lat else None,
        p90_ms=round(pct(lat, 90) * 1000, 3) if lat else None,
        p99_ms=round(pct(lat, 99) * 1000, 3) if lat else None,
        max_ms=round(lat[-1] * 1000, 3) if lat else None,
    )
    json.dump(out, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Operating System Principles Project:
Concurrent Website Availability Checker & Concurrency Benchmark

Demonstrates:
1. Producer-Consumer bounded buffer with Mutex & Condition Variables
2. Semaphore-controlled network concurrency
3. Priority-based task scheduling (min-heap priority queue)
4. Graceful SIGINT signal handling without zombie workers
5. Sequential vs. Multi-Threaded vs. Multi-Process benchmarking
"""

import argparse
import heapq
import json
import multiprocessing as mp
import os
import signal
import socket
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

MAX_BUFFER_SIZE = 16
LOG_FILE = "availability.log"

shutdown_event = threading.Event()


@dataclass(order=True)
class CheckTask:
    priority: int
    seq: int
    name: str = field(compare=False)
    url: str = field(compare=False)
    timeout: float = field(compare=False, default=4.0)


class BoundedPriorityQueue:
    """Thread-safe bounded priority queue using Mutex + Condition Variables."""

    def __init__(self, capacity: int = MAX_BUFFER_SIZE):
        self.capacity = capacity
        self._heap: List[CheckTask] = []
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._closed = False

    def put(self, task: CheckTask) -> bool:
        with self._not_full:
            while len(self._heap) >= self.capacity and not self._closed:
                self._not_full.wait(timeout=0.5)
            if self._closed:
                return False
            heapq.heappush(self._heap, task)
            self._not_empty.notify()
            return True

    def get(self) -> Optional[CheckTask]:
        with self._not_empty:
            while len(self._heap) == 0 and not self._closed:
                self._not_empty.wait(timeout=0.5)
            if len(self._heap) == 0 and self._closed:
                return None
            task = heapq.heappop(self._heap)
            self._not_full.notify()
            return task

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()


def probe_url(target: dict) -> dict:
    """Perform a single HTTP HEAD/GET probe with strict socket timeout."""
    url = target["url"]
    name = target["name"]
    timeout = float(target.get("timeout", 4.0))
    priority = int(target.get("priority", 2))

    start = time.perf_counter()
    status_code = 0
    up = False
    error_msg = ""

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "OS-Availability-Checker/1.0"},
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status_code = resp.getcode()
            up = 200 <= status_code < 400
    except urllib.error.HTTPError as e:
        status_code = e.code
        error_msg = f"HTTP {e.code}"
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as e:
        error_msg = str(getattr(e, "reason", e))

    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    return {
        "name": name,
        "url": url,
        "priority": priority,
        "up": up,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "error": error_msg,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


class AvailabilityMonitor:
    """Coordinates Producer-Consumer threads, Semaphore, Mutex metrics, and logging."""

    def __init__(self, targets: List[dict], num_workers: int = 4):
        self.targets = targets
        self.num_workers = num_workers
        self.queue = BoundedPriorityQueue(capacity=MAX_BUFFER_SIZE)
        self.net_semaphore = threading.Semaphore(num_workers)
        self.metrics_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.metrics: Dict[str, dict] = {
            t["url"]: {"name": t["name"], "total": 0, "up": 0, "latencies": []}
            for t in targets
        }

    def _record_result(self, res: dict) -> None:
        # Critical Section 1: Update shared metrics dictionary
        with self.metrics_lock:
            entry = self.metrics[res["url"]]
            entry["total"] += 1
            if res["up"]:
                entry["up"] += 1
                entry["latencies"].append(res["latency_ms"])

        # Critical Section 2: Thread-safe log file append & console output
        state = "UP  " if res["up"] else "DOWN"
        detail = f"HTTP {res['status_code']}" if res["up"] else (res["error"] or "Unreachable")
        line = (
            f"[{res['timestamp']}] [P{res['priority']}] [{state}] "
            f"{res['name']:<30} | {res['latency_ms']:>7.2f} ms | {detail} ({res['thread']})"
        )
        with self.log_lock:
            print(line)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def _producer(self, cycles: int, interval: float) -> None:
        seq = 0
        cycle = 0
        while not shutdown_event.is_set():
            if cycles > 0 and cycle >= cycles:
                break
            cycle += 1
            for t in self.targets:
                if shutdown_event.is_set():
                    break
                seq += 1
                task = CheckTask(
                    priority=int(t.get("priority", 2)),
                    seq=seq,
                    name=t["name"],
                    url=t["url"],
                    timeout=float(t.get("timeout", 4.0)),
                )
                self.queue.put(task)

            if cycles > 0 and cycle >= cycles:
                break
            shutdown_event.wait(timeout=interval)

        self.queue.close()

    def _consumer(self) -> None:
        while True:
            task = self.queue.get()
            if task is None:
                break
            with self.net_semaphore:
                res = probe_url(
                    {
                        "name": task.name,
                        "url": task.url,
                        "priority": task.priority,
                        "timeout": task.timeout,
                    }
                )
            self._record_result(res)

    def run(self, cycles: int = 1, interval: float = 5.0) -> float:
        start = time.perf_counter()
        workers = [
            threading.Thread(target=self._consumer, name=f"Worker-{i+1}", daemon=False)
            for i in range(self.num_workers)
        ]
        producer = threading.Thread(
            target=self._producer, args=(cycles, interval), name="Scheduler-Producer"
        )

        for w in workers:
            w.start()
        producer.start()

        producer.join()
        for w in workers:
            w.join()

        return time.perf_counter() - start

    def print_summary(self) -> None:
        print("\n=== Uptime & Latency Summary ===")
        with self.metrics_lock:
            for url, m in self.metrics.items():
                total = m["total"]
                up = m["up"]
                pct = (up / total * 100.0) if total > 0 else 0.0
                avg_lat = (
                    sum(m["latencies"]) / len(m["latencies"]) if m["latencies"] else 0.0
                )
                print(
                    f"- {m['name']:<30} | Uptime: {pct:5.1f}% ({up}/{total}) | Avg Latency: {avg_lat:6.2f} ms"
                )


def run_benchmark(targets: List[dict], workers: int) -> None:
    """Compare Sequential vs. Multi-Threaded vs. Multi-Process execution."""
    print(f"\nBenchmarking {len(targets)} endpoints with {workers} workers...\n")

    # 1. Sequential
    t0 = time.perf_counter()
    seq_results = [probe_url(t) for t in targets]
    t_seq = time.perf_counter() - t0

    # 2. Multi-Threaded
    monitor = AvailabilityMonitor(targets, num_workers=workers)
    t_thread = monitor.run(cycles=1, interval=0)

    # 3. Multi-Process
    t0 = time.perf_counter()
    with mp.Pool(processes=workers) as pool:
        proc_results = pool.map(probe_url, targets)
    t_proc = time.perf_counter() - t0

    up_count = sum(1 for r in proc_results if r["up"])
    print("\n=== OS Concurrency Benchmark Results ===")
    print(f"Endpoints Probed : {len(targets)} ({up_count} UP, {len(targets) - up_count} DOWN)")
    print(f"1. Sequential    : {t_seq:.3f} s  (1.00x baseline)")
    print(f"2. Multi-Thread  : {t_thread:.3f} s  ({t_seq / max(t_thread, 1e-6):.2f}x speedup)")
    print(f"3. Multi-Process : {t_proc:.3f} s  ({t_seq / max(t_proc, 1e-6):.2f}x speedup)")


def handle_sigint(signum, frame) -> None:
    print("\n[SIGINT] Graceful shutdown requested — draining active workers...")
    shutdown_event.set()


def main() -> None:
    signal.signal(signal.SIGINT, handle_sigint)

    parser = argparse.ArgumentParser(description="OS Concurrent Website Availability Checker")
    parser.add_argument("--config", default="urls.json", help="Path to JSON target list")
    parser.add_argument(
        "--mode",
        choices=["thread", "benchmark"],
        default="thread",
        help="Execution mode: thread monitor or concurrency benchmark",
    )
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads/processes")
    parser.add_argument("--cycles", type=int, default=1, help="Monitoring cycles (0 = infinite until Ctrl+C)")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between cycles")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        targets = json.load(f)

    if args.mode == "benchmark":
        run_benchmark(targets, args.workers)
    else:
        monitor = AvailabilityMonitor(targets, num_workers=args.workers)
        elapsed = monitor.run(cycles=args.cycles, interval=args.interval)
        monitor.print_summary()
        print(f"\nCompleted in {elapsed:.3f} s | Logs saved to {LOG_FILE}")


if __name__ == "__main__":
    main()

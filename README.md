# Operating System Principles: Concurrent Website Availability Checker

A multi-threaded and multi-process **Website Availability Checker** built in Python (`threading` + `multiprocessing`) to demonstrate core **Operating System Principles** in action:

* **Producer-Consumer Architecture & Bounded Buffer:** A scheduler producer enqueues target URLs into a fixed-capacity priority queue while a pool of `N` worker threads dequeues and monitors endpoints concurrently.
* **Synchronization Primitives:** Uses `threading.Lock` (Mutex), `threading.Condition` (Condition Variables for `not_full` / `not_empty` signaling), and `threading.Semaphore` (concurrency limiter) to eliminate race conditions and prevent deadlocks on shared metrics and log files.
* **Priority CPU/Task Scheduling:** Critical endpoints (`Priority 1`) are dispatched ahead of standard (`Priority 2`) and low-priority (`Priority 3`) targets using a thread-safe min-heap priority buffer, paired with strict per-request socket timeouts so hung servers never starve the worker pool.
* **Thread vs. Process Concurrency Benchmarking (`--mode benchmark`):** Compares **Sequential**, **Multi-Threaded (`threading`)**, and **Multi-Process (`multiprocessing`)** execution models—measuring wall-clock execution time, throughput (checks/sec), and concurrency speedup.
* **Graceful Signal Handling (`SIGINT`):** Catches `Ctrl+C` (`signal.SIGINT`) cleanly, sets an atomic shutdown flag, wakes blocked condition variables, drains active workers without zombie processes, and prints a final availability report.

## Repository Structure

| File | Description |
| :--- | :--- |
| `monitor.py` | Main OS concurrent monitor implementing bounded priority queue, `Lock`/`Condition`/`Semaphore` synchronization, `SIGINT` handler, logging, and Thread vs. Process benchmarking. |
| `urls.json` | Sample target endpoints configured with priority levels (`1` = Critical, `2` = Normal, `3` = Low) and timeout thresholds. |
| `README.md` | Architecture overview, OS concepts mapping, and usage instructions. |

## Quick Start (Zero External Dependencies — Python 3.8+ Stdlib Only)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aunhaiderniazi-lab/os-website-availability-checker.git
   cd os-website-availability-checker
   ```
2. **Run a single concurrent scan (Thread Pool mode):**
   ```bash
   python monitor.py --mode thread --workers 4 --cycles 1
   ```
3. **Run continuous monitoring (press `Ctrl+C` to test graceful `SIGINT` shutdown):**
   ```bash
   python monitor.py --mode thread --workers 4 --interval 5
   ```
4. **Benchmark Sequential vs. Multi-Threaded vs. Multi-Process execution:**
   ```bash
   python monitor.py --mode benchmark --workers 4
   ```

## OS Concepts Demonstrated (Viva Quick Reference)
* **Critical Section & Mutual Exclusion:** Shared metrics dictionary and `availability.log` writes are serialized via `threading.Lock`.
* **Bounded-Buffer Synchronization:** Producers block on `not_full` (`Condition.wait()`) when queue size reaches `MAX_BUFFER_SIZE`; consumers block on `not_empty` until items arrive or shutdown is signaled.
* **Process vs. Thread Trade-offs:** Threads share heap memory with low creation/context-switch overhead (ideal for I/O-bound HTTP checks), whereas `multiprocessing` spawns isolated OS processes with separate address spaces and IPC serialization overhead.

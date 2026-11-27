#!/usr/bin/env bash
# ==============================================================================
# Operating System Principles Project:
# Concurrent Website Availability Checker (POSIX / Bash Shell Implementation)
#
# Demonstrates:
# 1. Process Creation & Job Control (&, PID tracking, wait)
# 2. Counting Semaphore via Named Pipe (FIFO) + File Descriptors (exec 3<>)
# 3. Critical Section Mutual Exclusion via flock(1) on shared log & metrics
# 4. Priority-based scheduling (sorted input queue: P1 -> P2 -> P3)
# 5. Graceful Signal Handling (trap cleanup INT TERM EXIT) without zombie PIDs
# 6. Sequential vs. Concurrent Multi-Process Benchmarking (--mode benchmark)
# ==============================================================================

set -u

CONFIG_FILE="urls.txt"
LOG_FILE="availability_sh.log"
MODE="concurrent"
WORKERS=4
CYCLES=1
INTERVAL=5

TMP_DIR=$(mktemp -d "/tmp/os_mht.XXXXXX")
FIFO_PATH="$TMP_DIR/sem.fifo"
LOCK_FILE="$TMP_DIR/write.lock"
METRICS_FILE="$TMP_DIR/metrics.dat"
CHILD_PIDS=()
STOP_REQUESTED=0

cleanup() {
    local exit_code=$?
    if [[ ${#CHILD_PIDS[@]} -gt 0 ]]; then
        for pid in "${CHILD_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
        wait "${CHILD_PIDS[@]}" 2>/dev/null || true
    fi
    exec 3>&- 2>/dev/null || true
    rm -rf "$TMP_DIR"
    exit "$exit_code"
}

handle_sigint() {
    STOP_REQUESTED=1
    printf "\n[SIGINT] Signal caught — draining active child processes & cleaning up...\n"
    cleanup
}

trap handle_sigint INT TERM
trap cleanup EXIT

usage() {
    cat <<EOF
Usage: ./monitor.sh [OPTIONS]
  -c, --config FILE     Target URL list (default: urls.txt)
  -m, --mode MODE       Execution mode: concurrent | benchmark (default: concurrent)
  -w, --workers N       Max concurrent worker processes via FIFO semaphore (default: 4)
  -n, --cycles N        Monitoring cycles; 0 = infinite until Ctrl+C (default: 1)
  -i, --interval SEC    Seconds between cycles (default: 5)
  -h, --help            Show help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--config)   CONFIG_FILE="$2"; shift 2 ;;
        -m|--mode)     MODE="$2"; shift 2 ;;
        -w|--workers)  WORKERS="$2"; shift 2 ;;
        -n|--cycles)   CYCLES="$2"; shift 2 ;;
        -i|--interval) INTERVAL="$2"; shift 2 ;;
        -h|--help)     usage; exit 0 ;;
        *)             echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

if [[ ! "$WORKERS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --workers must be a positive integer." >&2
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Config file '$CONFIG_FILE' not found." >&2
    exit 1
fi

# Initialize Counting Semaphore using Named Pipe (FIFO)
init_semaphore() {
    rm -f "$FIFO_PATH"
    mkfifo "$FIFO_PATH"
    exec 3<>"$FIFO_PATH"
    rm -f "$FIFO_PATH"
    for ((i = 0; i < WORKERS; i++)); do
        printf "token\n" >&3
    done
}

# Portable millisecond timestamp
now_ms() {
    if date +%s%3N 2>/dev/null | grep -qv 'N'; then
        date +%s%3N
    else
        python3 -c 'import time; print(int(time.time()*1000))'
    fi
}

# Probe single endpoint and write synchronized log + metrics
probe_endpoint() {
    local priority="$1"
    local name="$2"
    local url="$3"
    local timeout="$4"
    local log_enabled="$5"

    local start_ms end_ms elapsed_ms http_code state detail ts line
    start_ms=$(now_ms)
    http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$timeout" --connect-timeout "$timeout" -A "OS-Bash-Monitor/1.0" "$url" || true)
    end_ms=$(now_ms)
    elapsed_ms=$((end_ms - start_ms))

    if [[ "$http_code" =~ ^[23][0-9]{2}$ ]]; then
        state="UP  "
        detail="HTTP $http_code"
    else
        state="DOWN"
        detail="HTTP ${http_code:-000} (Timeout/Unreachable)"
    fi

    ts=$(date "+%Y-%m-%d %H:%M:%S")
    line=$(printf "[%s] [P%s] [%s] %-28s | %6d ms | %s (PID:%s)" "$ts" "$priority" "$state" "$name" "$elapsed_ms" "$detail" "$BASHPID")

    # Critical Section protected by flock(1)
    {
        flock -x 9
        if [[ "$log_enabled" -eq 1 ]]; then
            printf "%s\n" "$line"
            printf "%s\n" "$line" >> "$LOG_FILE"
        fi
        printf "%s|%s|%s|%d\n" "$name" "$url" "$state" "$elapsed_ms" >> "$METRICS_FILE"
    } 9>"$LOCK_FILE"
}

# Load & sort targets by priority (1 = Highest priority first)
load_sorted_targets() {
    grep -vE '^[[:space:]]*(#|$)' "$CONFIG_FILE" | sort -t'|' -k1,1n
}

run_concurrent_cycle() {
    local log_enabled="$1"
    CHILD_PIDS=()

    while IFS='|' read -r priority name url timeout; do
        [[ "$STOP_REQUESTED" -eq 1 ]] && break
        # Acquire semaphore token (blocks if WORKERS processes are already active)
        read -r -u 3 _token
        (
            trap 'printf "token\n" >&3' EXIT
            probe_endpoint "$priority" "$name" "$url" "$timeout" "$log_enabled"
        ) &
        CHILD_PIDS+=($!)
    done < <(load_sorted_targets)

    for pid in "${CHILD_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    CHILD_PIDS=()
}

print_summary() {
    printf "\n=== OS Shell Monitor Summary ===\n"
    if [[ ! -s "$METRICS_FILE" ]]; then
        echo "No checks recorded."
        return
    fi

    awk -F'|' '
    {
        gsub(/ /, "", $3)
        total[$1]++
        if ($3 == "UP") {
            up[$1]++
            lat[$1] += $4
        }
    }
    END {
        for (name in total) {
            u = up[name] + 0
            t = total[name]
            pct = (u / t) * 100
            avg = (u > 0) ? (lat[name] / u) : 0
            printf "- %-28s | Uptime: %5.1f%% (%d/%d) | Avg Latency: %6.1f ms\n", name, pct, u, t, avg
        }
    }' "$METRICS_FILE" | sort
}

run_benchmark() {
    local count
    count=$(load_sorted_targets | wc -l | tr -d ' ')
    printf "\nBenchmarking %s endpoints (Sequential vs. %s Concurrent Child Processes)...\n\n" "$count" "$WORKERS"

    # 1. Sequential Baseline
    : > "$METRICS_FILE"
    local s0 s1 seq_ms
    s0=$(now_ms)
    while IFS='|' read -r priority name url timeout; do
        probe_endpoint "$priority" "$name" "$url" "$timeout" 0
    done < <(load_sorted_targets)
    s1=$(now_ms)
    seq_ms=$((s1 - s0))

    # 2. Multi-Process FIFO Semaphore Pool
    : > "$METRICS_FILE"
    init_semaphore
    local c0 c1 conc_ms
    c0=$(now_ms)
    run_concurrent_cycle 0
    c1=$(now_ms)
    conc_ms=$((c1 - c0))

    awk -v seq="$seq_ms" -v conc="$conc_ms" -v w="$WORKERS" 'BEGIN {
        speedup = (conc > 0) ? (seq / conc) : 1.0
        printf "=== Bash Multi-Process Benchmark Results ===\n"
        printf "1. Sequential Execution          : %d ms (1.00x baseline)\n", seq
        printf "2. Concurrent (%d Child PIDs)    : %d ms (%.2fx speedup)\n", w, conc, speedup
    }'
}

if [[ "$MODE" == "benchmark" ]]; then
    run_benchmark
else
    : > "$METRICS_FILE"
    init_semaphore
    start_all=$(now_ms)
    cycle=0
    while [[ "$STOP_REQUESTED" -eq 0 ]]; do
        if [[ "$CYCLES" -gt 0 && "$cycle" -ge "$CYCLES" ]]; then
            break
        fi
        cycle=$((cycle + 1))
        run_concurrent_cycle 1
        if [[ "$CYCLES" -gt 0 && "$cycle" -ge "$CYCLES" ]]; then
            break
        fi
        sleep "$INTERVAL"
    done
    end_all=$(now_ms)
    print_summary
    printf "\nCompleted in %d ms | Logs saved to %s\n" "$((end_all - start_all))" "$LOG_FILE"
fi

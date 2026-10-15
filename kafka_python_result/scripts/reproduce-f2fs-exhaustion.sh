#!/usr/bin/env bash
# Temporary, focused reproducer for the F2FS 60% -> 80% prefill stall.
# The benchmark and dm-zns diagnostic commits containing this file are meant
# to be reverted after root-cause analysis.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DIAG_DELAY="${REPRO_DIAG_AFTER_SECONDS:-600}"
LOGICAL_PERCENT="${REPRO_LOGICAL_PERCENT:-65}"
GC_RESERVED_ZONES="${REPRO_GC_RESERVED_ZONES:-2}"
GC_LOW_WATERMARK="${REPRO_GC_LOW_WATERMARK:-5}"
GC_HIGH_WATERMARK="${REPRO_GC_HIGH_WATERMARK:-6}"
STAMP="$(date +%Y%m%d_%H%M%S)"
DIAG_DIR="$SCRIPT_DIR/results/f2fs_exhaustion_diag_$STAMP"
WATCHDOG_PID=""

mkdir -p "$DIAG_DIR"

capture_diagnostics() {
    local reason="$1"
    local out="$DIAG_DIR/$reason"
    mkdir -p "$out"
    date --iso-8601=seconds > "$out/date.txt"
    cat /proc/uptime > "$out/uptime.txt"
    findmnt /result/kafka-logs > "$out/findmnt.txt" 2>&1 || true
    df -B1 /result/kafka-logs > "$out/df.txt" 2>&1 || true
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS > "$out/lsblk.txt" 2>&1 || true
    ps -eo pid,ppid,stat,wchan:40,etime,comm,args > "$out/processes.txt"
    sudo dmsetup table kafka-zns > "$out/dm-table.txt" 2>&1 || true
    sudo dmsetup status kafka-zns > "$out/dm-status.txt" 2>&1 || true
    sudo blkzone report /dev/nvme0n1 > "$out/zones.txt" 2>&1 || true
    sudo journalctl -k -b -o short-monotonic > "$out/kernel.txt" 2>&1 || true
    sudo journalctl -k -b -o short-monotonic --no-pager 2>/dev/null |
        grep -E 'zns-base: (space diag|gc:|foreground allocation exhausted)' \
        > "$out/dm-space-gc.txt" || true
    while read -r pid; do
        [[ -n "$pid" ]] || continue
        sudo sh -c "cat /proc/$pid/stack" > "$out/task-$pid-stack.txt" 2>&1 || true
    done < <(ps -eo pid=,stat= | awk '$2 ~ /^D/ {print $1}')
}

cleanup() {
    if [[ -n "$WATCHDOG_PID" ]]; then
        kill "$WATCHDOG_PID" 2>/dev/null || true
        wait "$WATCHDOG_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'capture_diagnostics interrupted; exit 130' INT TERM

printf 'Focused F2FS exhaustion reproducer\n'
printf '  Flow        : reset F2FS, then 20/40/60/80%% with 1 KiB Kafka A+B\n'
printf '  Watchdog    : capture diagnostics after %ss\n' "$DIAG_DELAY"
printf '  Logical cap : %s%% of physical\n' "$LOGICAL_PERCENT"
printf '  GC zones    : reserve/low/high=%s/%s/%s\n' \
    "$GC_RESERVED_ZONES" "$GC_LOW_WATERMARK" "$GC_HIGH_WATERMARK"
printf '  F2FS discard: enabled (DM tombstone experiment)\n'
printf '  Diagnostics : %s\n\n' "$DIAG_DIR"

sudo -v
capture_diagnostics before
(
    sleep "$DIAG_DELAY"
    capture_diagnostics watchdog
) &
WATCHDOG_PID=$!

set +e
BENCH_FILESYSTEMS=f2fs \
BENCH_RECORD_SIZES=1024 \
BENCH_OCCUPANCY_POINTS=20,40,60,80 \
BENCH_ROUNDS=1 \
BENCH_SCENARIO_GROUP=baseline \
BENCH_FAIL_FAST_STALL_SECONDS="${BENCH_FAIL_FAST_STALL_SECONDS:-60}" \
BENCH_F2FS_DISCARD=1 \
DM_LOGICAL_CAPACITY_PERCENT="$LOGICAL_PERCENT" \
DM_GC_RESERVED_ZONES="$GC_RESERVED_ZONES" \
DM_GC_LOW_WATERMARK="$GC_LOW_WATERMARK" \
DM_GC_HIGH_WATERMARK="$GC_HIGH_WATERMARK" \
"$SCRIPT_DIR/scripts/run-benchmark.sh" 2
rc=$?
set -e

capture_diagnostics after
printf '\nReproducer exit code: %d\nDiagnostics saved: %s\n' "$rc" "$DIAG_DIR"
exit "$rc"

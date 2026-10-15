#!/usr/bin/env bash
# Run inside the benchmark guest. Resets the configured FEMU experiment device.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
fs=${1:-f2fs}
capacity=${2:-75}
discard=${3:-0}
[[ $fs == ext4 || $fs == f2fs ]] || { echo 'filesystem must be ext4 or f2fs' >&2; exit 2; }
[[ $capacity =~ ^([1-9]|[1-9][0-9])$ ]] || {
    echo 'capacity must be an integer from 1 to 99' >&2; exit 2;
}
[[ $discard == 0 || $discard == 1 ]] || { echo 'discard must be 0 or 1' >&2; exit 2; }
export BENCH_FILESYSTEMS=$fs DM_LOGICAL_CAPACITY_PERCENT=$capacity
export BENCH_EXT4_DISCARD=$discard BENCH_F2FS_DISCARD=$discard
export BENCH_LONG_WARMUP_SECONDS=300
export BENCH_LONG_DURATION_SECONDS=${DIAG_DURATION_SECONDS:-600}
export BENCH_RETENTION_SEGMENT_BYTES=134217728 BENCH_RETENTION_SEGMENT_MS=60000
export DM_GC_LOW_WATERMARK=4 DM_GC_HIGH_WATERMARK=5 DM_GC_RESERVED_ZONES=2
export DM_GC_DIAG_BUDGET=${DM_GC_DIAG_BUDGET:-8}
export BENCH_INTEGRITY_DIAG=1
module=${DM_ZNS_MODULE_PATH:-$HOME/dm-zns-base/src/dm-zns-base.ko}
export DM_ZNS_SOURCE_DIR=${DM_ZNS_SOURCE_DIR:-$HOME/dm-zns-base}
modinfo -p "$module" | grep -q '^gc_diag_budget:' || {
    echo 'Build the diagnostic dm-zns-base module first.' >&2; exit 2;
}
if ! sudo -n true; then
    echo 'Passwordless sudo is required for unattended diagnostic execution.' >&2
    exit 1
fi
mkdir -p "$PWD/results/gc-diagnostics"
diag_dir=$(mktemp -d "$PWD/results/gc-diagnostics/run.XXXXXXXX")
start=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
finish() {
    rc=$?
    trap - EXIT
    set +e
    sudo journalctl -k -b --since "$start" --until "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" \
        -o short-iso-precise --no-pager > "$diag_dir/kernel.log"
    journal_rc=$?
    # Copy rotations as well; they may include earlier runs. Use timestamps/run ID.
    mkdir -p "$diag_dir/broker-logs"
    sudo cp -a "$HOME/kafka-4.2.0-src/logs/." "$diag_dir/broker-logs/"
    broker_rc=$?
    printf 'benchmark_exit=%s journal_exit=%s broker_copy_exit=%s\n' "$rc" "$journal_rc" "$broker_rc" > "$diag_dir/status.txt"
    echo "Diagnostics: $diag_dir (kernel log collection status=$journal_rc)"
    exit "$rc"
}
trap finish EXIT
{
    date --iso-8601=seconds
    uname -a
    git rev-parse HEAD
    printf 'DM_ZNS_GIT_BRANCH=%s\n' "$(git -C "$DM_ZNS_SOURCE_DIR" branch --show-current)"
    printf 'DM_ZNS_GIT_COMMIT=%s\n' "$(git -C "$DM_ZNS_SOURCE_DIR" rev-parse HEAD)"
    printf 'DM_ZNS_GIT_STATUS_SHA256='; git -C "$DM_ZNS_SOURCE_DIR" status --short | sha256sum | cut -d' ' -f1
    sha256sum "$DM_ZNS_SOURCE_DIR/src/dm-zns-base-main.c" "$module" \
        "$HOME/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar"
    env | LC_ALL=C sort | grep -E '^(BENCH_|DM_|DIAG_)'
} > "$diag_dir/environment.txt"
echo "WARNING: resets configured FEMU ZNS data; diagnostic timing is not a performance baseline."
bash ./scripts/run-benchmark.sh 4 2>&1 | tee "$diag_dir/console.log"

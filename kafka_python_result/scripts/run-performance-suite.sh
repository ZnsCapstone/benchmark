#!/usr/bin/env bash
# Six independent Kafka workload evaluations on the currently running FEMU.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"
exec 9>/tmp/team4-performance-suite.lock
flock -n 9 || { echo 'Another performance suite is running'; exit 2; }
SUITE_FS="${1:-ext4}"
case "$SUITE_FS" in ext4|f2fs) ;; *) exit 2 ;; esac
SUITE_DIR="$SCRIPT_DIR/results/performance-evaluation/cns-$SUITE_FS/six-workloads-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$SUITE_DIR"
printf 'suite=%s\n' "$SUITE_DIR"
for fill_mode in prefill kafka-only; do
  for scenario in write read-write read-write-topics; do
    python3 src/performance_evaluation.py --backend cns --filesystem "$SUITE_FS" \
      --workload "$fill_mode" --scenario "$scenario" \
      --rates 1000,2000,3000 --rounds 1 --warmup 60 --measure 180 \
      --output-dir "$SUITE_DIR/$fill_mode-$scenario"
  done
done
printf 'All six evaluations completed.\n'

#!/usr/bin/env bash
set -euo pipefail
cd /home/zns/kafka_python_result
BASE=/home/zns/kafka_python_result/results/final
MODULE=/home/zns/dm-zns-metrics-build-20261012/src/dm-zns-base.ko
test ! -e "$BASE/fio f2fs max 1"
test ! -e "$BASE/fio f2fs gc 1"
exec >"$BASE/fio f2fs 1.launch.log" 2>&1
trap 'rc=$?; echo "$rc" > "$BASE/fio f2fs 1.exit"' EXIT
date -Is
BENCH_F2FS_DISCARD=0 bash scripts/run-fio-evaluation.sh --prepare --backend dm-zns --filesystem f2fs --module-path "$MODULE" --workloads seq-read,seq-write,rand-read,rand-write --qds 1,8,32 --warmup 20 --measure 120 --rounds 1 --file-gib 1 --output-dir "$BASE/fio f2fs max 1" --run
BENCH_F2FS_DISCARD=0 bash scripts/run-fio-evaluation.sh --prepare --backend dm-zns --filesystem f2fs --module-path "$MODULE" --workloads seq-write,rand-write,mixed --qds 32 --warmup 30 --measure 600 --rounds 1 --file-gib 18 --output-dir "$BASE/fio f2fs gc 1" --run
date -Is

#!/usr/bin/env bash
set -euo pipefail
cd /home/zns/kafka_python_result
BASE=/home/zns/kafka_python_result/results/final
MODULE=/home/zns/dm-zns-metrics-build-20261012/src/dm-zns-base.ko
exec >"$BASE/fio long 1.launch.log" 2>&1
trap 'rc=$?; echo "$rc" > "$BASE/fio long 1.exit"' EXIT
date -Is
bash scripts/run-fio-evaluation.sh --prepare --backend dm-zns --filesystem ext4 --module-path "$MODULE" --workloads seq-read,seq-write,rand-read,rand-write --qds 1,8,32 --warmup 20 --measure 120 --rounds 1 --file-gib 1 --output-dir "$BASE/fio max 1" --run
bash scripts/run-fio-evaluation.sh --prepare --backend dm-zns --filesystem ext4 --module-path "$MODULE" --workloads seq-write,rand-write,mixed --qds 32 --warmup 30 --measure 600 --rounds 1 --file-gib 18 --output-dir "$BASE/fio gc 1" --run
date -Is

#!/usr/bin/env bash
set -euo pipefail
cd /home/zns/kafka_python_result
BASE=/home/zns/kafka_python_result/results/final
test ! -e "$BASE/fio gc 2"
exec >"$BASE/fio gc 2.launch.log" 2>&1
trap 'rc=$?; echo "$rc" > "$BASE/fio gc 2.exit"' EXIT
date -Is
bash scripts/run-fio-evaluation.sh --prepare --backend dm-zns --filesystem ext4 --module-path /home/zns/dm-zns-metrics-build-20261012/src/dm-zns-base.ko --workloads seq-write,rand-write,mixed --qds 32 --warmup 30 --measure 600 --rounds 1 --file-gib 18 --output-dir "$BASE/fio gc 2" --run
date -Is

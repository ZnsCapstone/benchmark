#!/usr/bin/env bash
# 첫 독립 suite가 정상 완료된 경우에만 동일 42조건을 한 번 더 실행한다.
set -euo pipefail
ROOT=/home/zns/kafka_python_result
FIRST="$ROOT/results/final/fio-controlled-20261013"
SECOND="$ROOT/results/final/fio-controlled-20261013-r2"
exec >"$ROOT/results/final/fio-controlled-20261013-r2.queue.log" 2>&1
date -Is
echo '첫 suite 정상 완료 대기'
while [[ ! -f "$FIRST/exit-code.txt" ]]; do sleep 60; done
test "$(tr -d '\n\r ' < "$FIRST/exit-code.txt")" = 0 || {
  echo '첫 suite 실패: 두 번째 실행을 시작하지 않음'
  exit 1
}
python3 - "$FIRST" <<'PY'
import json
import sys
from pathlib import Path
root = Path(sys.argv[1])
expected = []
for fs in ('ext4', 'f2fs'):
    for workload, bs in [('seq-read', '4k'), ('rand-read', '4k'), ('seq-read', '1M'),
                         ('seq-write', '4k'), ('rand-write', '4k'), ('seq-write', '1M')]:
        for qd in (1, 8, 32):
            expected.append(root / fs / 'low' / f'{workload}-{bs}-qd{qd}-r1' / 'summary.json')
    for workload, bs in [('seq-write', '4k'), ('rand-write', '4k'), ('seq-write', '1M')]:
        expected.append(root / fs / 'gc' / f'{workload}-{bs}-qd32-r1' / 'summary.json')
for path in expected:
    records = json.loads(path.read_text())
    if len(records) != 1 or not records[0].get('kernel_log_available') or records[0].get('kernel_storage_errors'):
        raise SystemExit(f'첫 실행 검증 실패: {path}')
print('42개 조건 정상 결과 확인')
PY
date -Is
BENCH_FIO_REPEATS=1 BENCH_FIO_SUITE_DIR="$SECOND" \
  bash "$ROOT/scripts/run-fio-controlled-suite.sh" --run

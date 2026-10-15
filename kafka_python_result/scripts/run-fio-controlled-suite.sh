#!/usr/bin/env bash
# 원인 분리 평가: FS/접근/크기/QD/반복별 독립 장치 초기화.
# --run 없이는 명령만 출력한다. 원래 benchmark.sh는 변경하지 않는다.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MODULE=${BENCH_FIO_MODULE:-/home/zns/dm-zns-metrics-build-20261012/src/dm-zns-base.ko}
SUITE_DIR=${BENCH_FIO_SUITE_DIR:-$ROOT/results/final/fio-controlled-$(date +%Y%m%d_%H%M%S)}
REPEATS=${BENCH_FIO_REPEATS:-1}
PROFILE=${BENCH_FIO_PROFILE:-full}
MAX_STEAL=${BENCH_FIO_MAX_STEAL_PCT:-1}
STEAL_POLICY=${BENCH_FIO_STEAL_POLICY:-warn}
RESUME=${BENCH_FIO_RESUME:-0}
[[ $PROFILE == full || $PROFILE == compact ]] || exit 2
[[ $STEAL_POLICY == warn || $STEAL_POLICY == stop ]] || exit 2
[[ $RESUME == 0 || $RESUME == 1 ]] || exit 2
RUN=0
if [[ ${1:-} == --run ]]; then RUN=1; elif [[ $# -gt 0 ]]; then exit 2; fi
[[ $REPEATS =~ ^[1-9][0-9]*$ ]] || exit 2
if ((RUN)); then
  if [[ $RESUME == 1 ]]; then
    test -d "$SUITE_DIR"
    (cd /; sha256sum --check "$SUITE_DIR/module.sha256")
    if [[ -f "$SUITE_DIR/exit-code.txt" ]]; then
      mv "$SUITE_DIR/exit-code.txt" "$SUITE_DIR/exit-code.before-resume-$(date +%Y%m%d_%H%M%S).txt"
    fi
  else
    test ! -e "$SUITE_DIR"
    mkdir -p "$SUITE_DIR"
  for repo in dm-zns-base Kafka-benchmark kafka_python_result; do
    if [[ -d /home/zns/$repo/.git ]]; then
      git -C "/home/zns/$repo" rev-parse HEAD > "$SUITE_DIR/$repo.commit.txt"
      git -C "/home/zns/$repo" status --short > "$SUITE_DIR/$repo.status.txt"
      git -C "/home/zns/$repo" diff --binary HEAD > "$SUITE_DIR/$repo.dirty.diff"
    fi
  done
  sha256sum "$MODULE" > "$SUITE_DIR/module.sha256"
  fi
  exec > >(tee -a "$SUITE_DIR/suite.log") 2>&1
  trap 'rc=$?; echo "$rc" > "$SUITE_DIR/exit-code.txt"' EXIT
fi
run_cell() {
  local fs=$1 group=$2 workload=$3 bs=$4 qd=$5 repeat=$6 size warm measure
  if [[ $group == low ]]; then size=1; warm=20; measure=120;
  else size=18; warm=30; measure=600; fi
  local out="$SUITE_DIR/$fs/$group/${workload}-${bs}-qd${qd}-r${repeat}"
  if [[ $RESUME == 1 && -e $out ]]; then
    python3 - "$out" "$fs" "$workload" "$bs" "$qd" "$size" "$warm" "$measure" <<'PY'
import json
import sys
from pathlib import Path
root = Path(sys.argv[1])
records = json.loads((root / 'summary.json').read_text())
if len(records) != 1 or not records[0].get('kernel_log_available') or records[0].get('kernel_storage_errors'):
    raise SystemExit(f'기존 결과 미완료 또는 오류: 수동 확인 필요 {root}')
manifest = json.loads((root / 'manifest.json').read_text())
expected = dict(filesystem=sys.argv[2], workloads=sys.argv[3], bs=sys.argv[4],
                qds=sys.argv[5], file_gib=int(sys.argv[6]), warmup=int(sys.argv[7]),
                measure=int(sys.argv[8]), rounds=1)
if any(manifest.get(key) != value for key, value in expected.items()):
    raise SystemExit(f'기존 조건 불일치: {root}')
raw = list(root.glob('*.fio.json'))
if len(raw) != 1 or any(job.get('error', 0) for job in json.loads(raw[0].read_text()).get('jobs', [])):
    raise SystemExit(f'기존 fio 원본 확인 실패: {root}')
print(f'완료 조건 보존·건너뜀: {root}')
PY
    return
  fi
  local -a command=(bash "$ROOT/scripts/run-fio-evaluation.sh"
    --prepare --backend dm-zns --filesystem "$fs" --module-path "$MODULE"
    --workloads "$workload" --bs "$bs" --qds "$qd" --rounds 1
    --file-gib "$size" --warmup "$warm" --measure "$measure"
    --output-dir "$out" --run)
  printf '%q ' "${command[@]}"; printf '\n'
  if ((RUN)); then
    date -Is
    BENCH_F2FS_DISCARD=0 "${command[@]}"
    python3 - "$out" "$MAX_STEAL" "$STEAL_POLICY" <<'PY'
import json
import sys
from pathlib import Path
records = json.loads((Path(sys.argv[1]) / 'summary.json').read_text())
if len(records) != 1 or not records[0].get('kernel_log_available'):
    raise SystemExit('커널 로그 수집 불완전: 후속 독립 조건을 중단하고 확인하세요.')
if records[0].get('kernel_storage_errors'):
    raise SystemExit('커널 저장장치 오류: 후속 조건 중단')
steal = records[0].get('os', {}).get('cpu_steal_pct')
if steal is None or steal > float(sys.argv[2]):
    (Path(sys.argv[1]) / 'HOST-INTERFERENCE.txt').write_text(
        f'CPU steal={steal}%, 탐색 간섭 기준={sys.argv[2]}%. 저장장치 실패가 아닌 환경 간섭 판정.\n')
    if sys.argv[3] == 'stop':
        raise SystemExit('CPU 간섭 기준 초과 또는 미계측: 추가 반복을 중단하고 환경을 확인하세요.')
    print(f'CPU steal={steal}%: 간섭 표시를 보존하고 실행 계속, 종료 후 영향 분석 필요')
PY
  fi
}
for ((repeat=1; repeat<=REPEATS; repeat++)); do
  if [[ $PROFILE == compact ]]; then
    for group in low gc; do
      for cell in seq-write:4k rand-write:4k seq-write:1M; do
        for fs in ext4 f2fs; do
          run_cell "$fs" "$group" "${cell%:*}" "${cell#*:}" 32 "$repeat"
        done
      done
    done
    continue
  fi
  for fs in ext4 f2fs; do
    for cell in seq-read:4k rand-read:4k seq-read:1M seq-write:4k rand-write:4k seq-write:1M; do
      for qd in 1 8 32; do
        run_cell "$fs" low "${cell%:*}" "${cell#*:}" "$qd" "$repeat"
      done
    done
  done
  for fs in ext4 f2fs; do
    for cell in seq-write:4k rand-write:4k seq-write:1M; do
      run_cell "$fs" gc "${cell%:*}" "${cell#*:}" 32 "$repeat"
    done
  done
done
if ((RUN)); then date -Is; fi

#!/usr/bin/env bash
# Run the existing benchmark stack directly on the team4 FEMU conventional SSD.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export BENCH_STORAGE_BACKEND=cns
export BENCH_CNS_EXPECTED_MODEL="${BENCH_CNS_EXPECTED_MODEL:-FEMU BlackBox-SSD Controller}"
export BENCH_CNS_EXPECTED_SECTORS="${BENCH_CNS_EXPECTED_SECTORS:-50331648}"

exec "$SCRIPT_DIR/scripts/run-benchmark.sh" "$@"

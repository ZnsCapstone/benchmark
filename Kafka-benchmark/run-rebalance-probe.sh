#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
if [[ $# != 2 || $2 != --allow-temp-topics ]]; then
  echo 'Usage: bash run-rebalance-probe.sh localhost:9092 --allow-temp-topics' >&2
  echo 'Requires an already running test broker; creates 3 unique small topics.' >&2
  exit 2
fi
jar=build/libs/kafka-benchmark-1.0.jar
[[ -f $jar ]] || { echo 'Build first: ./gradlew test jar' >&2; exit 2; }
command -v timeout >/dev/null
mkdir -p probe-results
probe_dir=$(mktemp -d "$PWD/probe-results/rebalance.XXXXXXXX")
{
  date -u '+%Y-%m-%d %H:%M:%S UTC'
  git rev-parse HEAD
  git status --short
  sha256sum "$jar"
  java -version 2>&1
} > "$probe_dir/environment.txt"
echo "Results: $probe_dir"
echo 'No formatting or device reset. 3 temporary topics are retained for inspection.'
echo 'The expired case deletes the first 11 records/partition ONLY in its own new topic.'
set +e
timeout --signal=TERM --kill-after=10s 600s java \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
  -cp "$jar" com.hanyang.cs.RebalanceProbe "$1" "$2" \
  2>&1 | tee "$probe_dir/console.log"
probe_status=("${PIPESTATUS[@]}")
set -e
printf 'probe_exit=%s tee_exit=%s\n' "${probe_status[0]}" "${probe_status[1]}" > "$probe_dir/status.txt"
echo "Results: $probe_dir"
if (( probe_status[0] != 0 )); then exit "${probe_status[0]}"; fi
exit "${probe_status[1]}"

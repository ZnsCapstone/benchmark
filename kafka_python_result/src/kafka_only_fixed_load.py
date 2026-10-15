#!/usr/bin/env python3
"""1만/2만 조건을 각각 초기화하고 동일 부하로 채우기와 순환을 측정한다."""
import argparse
import json
from pathlib import Path
import subprocess
import signal
import sys
import time


def command(args, rate, destination):
    return [sys.executable, '-u', str(Path(__file__).with_name('kafka_only_evaluation.py')),
            '--filesystem', args.filesystem, '--scenario', args.scenario,
            '--rates', str(rate), '--prepare-rate', str(rate),
            '--warmup', '60', '--measure', '600', '--retention-gib', '18',
            '--jar', args.jar, '--module-provenance', args.module_provenance,
            '--result-dir', str(destination), '--execute']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--result-dir', required=True, type=Path)
    p.add_argument('--filesystem', default='ext4', choices=['ext4', 'f2fs'])
    p.add_argument('--scenario', default='write', choices=['write', 'read-write'])
    p.add_argument('--jar', required=True)
    p.add_argument('--module-provenance', required=True)
    args = p.parse_args()
    root = args.result_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    results = []
    for rate in (10000, 20000):
        destination = root / f'{rate}ops'
        payload = {'current_rate': rate, 'phase': '실행 중', 'results': results}
        (root / 'series-status.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        with (root / f'{rate}ops.launch.log').open('w') as log:
            child = subprocess.Popen(command(args, rate, destination), stdout=log, stderr=subprocess.STDOUT)
            analyzer = None
            try:
                while child.poll() is None:
                    if analyzer is None and (destination / 'progress.jsonl').exists():
                        analyzer = subprocess.Popen(['nice', '-n', '19', sys.executable,
                            str(Path(__file__).with_name('analyze_kafka_fill.py')),
                            str(destination), '--output', str(destination / 'fill-analysis'), '--watch'],
                            stdout=log, stderr=subprocess.STDOUT)
                    time.sleep(5)
            finally:
                if child.poll() is None:
                    child.send_signal(signal.SIGINT)
                if analyzer and analyzer.poll() is None:
                    analyzer.terminate()
            code = child.returncode
        results.append({'rate': rate, 'exit_code': code, 'result_dir': str(destination)})
        clean = subprocess.run(['sudo', 'dmsetup', 'ls'], capture_output=True, text=True)
        if code != 0 or 'kafka-zns' in clean.stdout or not (destination / 'completed.json').exists():
            (root / 'series-status.json').write_text(json.dumps(
                {'phase': '오류 또는 정리 미완료: 다음 조건 보류', 'results': results}, ensure_ascii=False, indent=2))
            return 1
    (root / 'series-status.json').write_text(json.dumps(
        {'phase': '두 조건 완료', 'results': results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

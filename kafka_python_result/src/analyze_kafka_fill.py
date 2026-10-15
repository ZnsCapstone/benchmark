#!/usr/bin/env python3
"""완료된 준비 구간과 점유율 기록을 연결한다. 실행기/실험 장치는 변경하지 않는다."""
import argparse
import csv
import json
from pathlib import Path
import time


def build_rows(root):
    samples = []
    for line in (root / 'progress.jsonl').read_text().splitlines():
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue  # 실행 중 복사한 마지막 미완성 행은 사용하지 않는다.
        if '준비' in sample.get('phase', ''):
            samples.append(sample)
    samples.sort(key=lambda s: s['time'])
    previous_end = float('-inf')
    rows = []
    for path in sorted(root.glob('prepare-*.json')):
        try:
            result = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        end = path.stat().st_mtime
        interval = [s for s in samples if previous_end < s['time'] <= end]
        previous_end = end
        if not interval:
            continue
        metrics = result['metrics']
        values = [s['used_percent'] for s in interval]
        row = {
            'source': path.name,
            'occupancy_start_pct': values[0], 'occupancy_end_pct': values[-1],
            'occupancy_min_pct': min(values), 'occupancy_max_pct': max(values),
            'sample_start_epoch': interval[0]['time'], 'sample_end_epoch': interval[-1]['time'],
            'sample_count': len(interval), 'valid': result['validity']['valid'],
            'measurement_seconds': result['config']['measure_sec'],
            'target_ops': result['config']['target_ops'],
            'retention_partitions': interval[-1]['retention_partitions'],
        }
        for key in ('achieved_ops', 'achieved_pct', 'p95_ms', 'p99_ms',
                    'mapper_util_avg', 'raw_util_avg', 'mapper_await_avg',
                    'raw_wkB_s_avg', 'cpu_st_avg', 'cpu_wa_avg', 'send_errors',
                    'failed_requests', 'unresolved_after_drain', 'ack_stall_max_sec'):
            row[key] = metrics.get(key)
        rows.append(row)
    return rows


def milestones(rows):
    return [(point, next((r for r in rows if r['occupancy_max_pct'] >= point), None))
            for point in range(10, 91, 10)]


def render(rows):
    lines = ['# Kafka로 채우는 과정의 성능', '',
             '완료된 120초 준비 실행별 통계다. 고정 점유율 실험이나 정상상태 결과가 아니다.',
             '점유율 범위는 10초 표본과 결과 파일 저장 시각을 연결한 근사값이며,',
             'Java의 정확한 측정 시작·종료 시각과 수초 차이가 있을 수 있다.',
             '분석 입력을 복사할 때 prepare JSON의 수정 시각을 보존해야 한다(`scp -p`).',
             'p95/p99를 평균 내거나 정확히 10%인 순간의 값으로 해석하지 않는다.', '',
             '| 구간 | 점유율 범위(%) | ops/s | 달성률(%) | p95(ms) | p99(ms) | DM util(%) | CPU steal(%) | 유효 |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---|']
    def number(row, field):
        value = row.get(field)
        return f'{value:.2f}' if isinstance(value, (int, float)) else '미수집'
    for row in rows:
        cells = [row['source'].removesuffix('.json'),
                 f"{row['occupancy_min_pct']:.2f}~{row['occupancy_max_pct']:.2f}"]
        cells += [number(row, key) for key in ('achieved_ops', 'achieved_pct', 'p95_ms', 'p99_ms', 'mapper_util_avg', 'cpu_st_avg')]
        cells += [str(row['valid'])]
        lines.append('| ' + ' | '.join(cells) + ' |')
    lines += ['', '## 점유율별 최초 도달 구간', '',
              '각 지점에 최초로 도달한 것으로 관측된 완료 구간을 연결한다.',
              '점유율 표본 사이의 경계는 근사치이며, 미완료 구간은 포함하지 않는다.', '']
    for point, row in milestones(rows):
        lines.append(f"- {point}%: " + (row['source'] if row else '아직 완료된 구간 없음'))
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--watch', action='store_true', help='120초마다 완료 구간 표를 갱신')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    while True:
        rows = build_rows(args.input)
        (args.output / '채우기-구간별-성능.md').write_text(render(rows))
        if rows:
            with (args.output / 'fill-progress.csv').open('w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        print(f'완료 구간 {len(rows)}개 정리: {args.output}', flush=True)
        if not args.watch or any((args.input / f).exists() for f in ['completed.json', 'failure.json']):
            break
        time.sleep(120)


if __name__ == '__main__':
    main()

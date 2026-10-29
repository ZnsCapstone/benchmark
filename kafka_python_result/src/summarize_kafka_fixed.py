#!/usr/bin/env python3
"""완료된 고정부하 실행의 채우기/준비 및 순환 통계를 재현 가능하게 집계한다."""
import argparse
import csv
import json
from pathlib import Path
import re


def counters(row):
    return {k: int(v) for k, v in re.findall(r'\b([a-z_]+)=(\d+)', row['dm_status'])}


def interval(first, last):
    a, b = counters(first), counters(last)
    keys = ('logical_write_bytes', 'user_write_bytes', 'gc_write_bytes', 'wal_write_bytes',
            'sstable_write_bytes', 'gc_runs', 'gc_ns', 'gc_reclaims', 'gc_moved_bytes',
            'compaction_runs', 'compaction_ns', 'flush_runs')
    delta = {k: b[k] - a[k] for k in keys}
    if any(v < 0 for v in delta.values()) or delta['logical_write_bytes'] <= 0:
        raise ValueError('카운터 초기화 또는 양의 논리 쓰기량이 없는 구간')
    physical = sum(delta[k] for k in ('user_write_bytes', 'gc_write_bytes',
                                     'wal_write_bytes', 'sstable_write_bytes'))
    raw = (int(last['raw_block_stat'].split()[6]) - int(first['raw_block_stat'].split()[6])) * 512
    return dict(delta, start=first['time'], end=last['time'],
                seconds=last['time'] - first['time'], physical_write_bytes=physical,
                raw_write_bytes=raw, dm_wa=physical / delta['logical_write_bytes'],
                raw_wa=raw / delta['logical_write_bytes'])


def summarize(path):
    samples = [json.loads(line) for line in (path / 'progress.jsonl').read_text().splitlines()]
    fill = [r for r in samples if '준비' in r['phase']]
    cycle = [r for r in samples if '측정' in r['phase']]
    results = [(f, json.loads(f.read_text())) for f in sorted(path.glob('prepare-*.json'))]
    first_retention = next(r for r in fill if r['retention_partitions'] > 0)
    def aggregate(items):
        seconds = sum(r['config']['measure_sec'] for _, r in items)
        ack = sum(r['metrics']['eventual_ack_requests'] for _, r in items)
        return {'chunks': len(items), 'active_seconds': seconds, 'acks': ack,
                'ops': ack / seconds if seconds else None, 'payload_bytes': ack * 1024,
                'send_errors': sum(r['metrics']['send_errors'] for _, r in items),
                'failed': sum(r['metrics']['failed_requests'] for _, r in items)}
    result_file = next(path.glob('result-*.json'))
    result = json.loads(result_file.read_text())
    measure = [r for r in cycle if r['time'] >= cycle[0]['time'] + result['config']['warmup_sec']]
    readiness = json.loads((path / 'readiness.json').read_text())
    return {'input': str(path), 'target': result['config']['target_ops'],
            'prepare': aggregate(results),
            'before_first_retention_completed_chunks': aggregate([(f, r) for f, r in results if f.stat().st_mtime <= first_retention['time']]),
            'seconds_to_first_retention': first_retention['time'] - fill[0]['time'],
            'first_retention_occupancy': first_retention['used_percent'],
            'readiness_occupancy': readiness['sample']['used_percent'],
            'prepare_seconds_wall': fill[-1]['time'] - fill[0]['time'],
            'prepare_dm': interval(fill[0], fill[-1]),
            'cycle_dm_approx': interval(measure[0], measure[-1]),
            'cycle_metrics': result['metrics'], 'assessment': result['assessment'],
            'validity': result['validity'],
            'completed': json.loads((path / 'completed.json').read_text())}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('inputs', nargs='+', type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    rows = [summarize(path) for path in args.inputs]
    args.output.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + '\n')
    flat = []
    for row in rows:
        for phase, field in [('prepare', 'prepare_dm'), ('cycle_approx', 'cycle_dm_approx')]:
            flat.append({'target_ops': row['target'], 'phase': phase, **row[field]})
    with args.output.with_suffix('.csv').open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(flat[0]))
        writer.writeheader()
        writer.writerows(flat)
    for row in rows:
        view = dict(row)
        view['cycle_metrics'] = {k: row['cycle_metrics'][k] for k in
            ['achieved_ops', 'achieved_pct', 'p95_ms', 'p99_ms', 'ack_stall_max_sec',
             'ack_stall_total_sec', 'raw_util_avg', 'mapper_util_avg', 'cpu_st_avg', 'failed_requests']}
        print(json.dumps(view, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

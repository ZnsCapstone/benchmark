#!/usr/bin/env python3
"""제출본용 fio 최종 4회 전체를 집계한다. 정상 저속 회차를 제거하지 않는다."""
import csv
import hashlib
import json
from pathlib import Path
import statistics as st

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/submission-20261015'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    base = ROOT / 'results/analysis-fio-compact-20261013/analysis-records.json'
    extra = OUT / 'fio-extra-input.json'
    a = json.loads(base.read_text())
    b = json.loads(extra.read_text())
    for r in a:
        r['source_run'] = 'fio-compact-20261013'
    rows = a + b
    if len(rows) != 48:
        raise ValueError('최종 48회 전체가 필요합니다')
    for r in rows:
        if r['fio_error'] or r['kernel_storage_errors'] or not r['kernel_log_available']:
            raise ValueError('유효성 확인 실패: ' + str((r['fs'], r['cell'])))
    if len({r['module_sha256'] for r in rows}) != 1:
        raise ValueError('모듈이 다른 실행을 합산하지 않습니다')
    grouped = []
    for fs in ('ext4', 'f2fs'):
        for state in ('low', 'gc'):
            for workload, bs in [('seq-write', '4k'), ('rand-write', '4k'), ('seq-write', '1M')]:
                own = [r for r in rows if (r['fs'], r['group'], r['workload'], r['block_size']) == (fs, state, workload, bs)]
                if len(own) != 4:
                    raise ValueError('각 조건마다 4회가 필요합니다')
                g = dict(fs=fs, state=state, workload=workload, bs=bs, n=4)
                metrics = ['write_iops', 'write_mib_s', 'write_p95_ms', 'write_p99_ms', 'dm_wa_issued']
                for name in metrics:
                    values = [r[name] for r in own]
                    for key, fn in [('median', st.median), ('min', min), ('max', max), ('mean', st.mean)]:
                        g[f'{name}_{key}'] = fn(values)
                for name in ('gc_reclaims', 'gc_moved_bytes', 'gc_ns'):
                    values = [r['dm_delta'][name] for r in own]
                    g[name + '_median'] = st.median(values)
                    g[name + '_min'] = min(values)
                    g[name + '_max'] = max(values)
                grouped.append(g)
    fields = ['source_run', 'fs', 'group', 'workload', 'block_size', 'qd', 'repetition',
              'write_iops', 'write_mib_s', 'write_p95_ms', 'write_p99_ms', 'dm_wa_issued',
              'fio_error', 'kernel_log_available', 'module_sha256']
    with (OUT / 'fio-최종4회-원자료.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows({k: r[k] for k in fields} for r in rows)
    with (OUT / 'fio-최종4회-조건별집계.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(grouped[0]))
        w.writeheader()
        w.writerows(grouped)
    (OUT / 'fio-groups.json').write_text(json.dumps(grouped, indent=2))
    evidence = {'runs': 48, 'repetitions': 4, 'module_sha256': rows[0]['module_sha256'],
                'fio_errors': 0, 'kernel_errors': 0, 'missing_kernel_logs': 0,
                'excluded_normal_slow_runs': 0,
                'input_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (base, extra)}}
    (OUT / 'validation.json').write_text(json.dumps(evidence, indent=2))
    for g in grouped:
        print(g['fs'], g['state'], g['workload'], g['bs'],
              'IOPS median/range/mean', *(round(g['write_iops_' + k], 2) for k in ('median', 'min', 'max', 'mean')),
              'MiB/s', round(g['write_mib_s_median'], 2),
              'p95/p99', round(g['write_p95_ms_median'], 2), round(g['write_p99_ms_median'], 2),
              'WA', round(g['dm_wa_issued_median'], 2),
              'movedGiB median/range', *(round(g['gc_moved_bytes_' + k] / 2**30, 2) for k in ('median', 'min', 'max')))


if __name__ == '__main__':
    main()

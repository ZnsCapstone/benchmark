"""종료된 compact fio 결과를 로컬에서 분석한다. 원격 접속과 이상치 삭제는 하지 않는다."""
import argparse
import csv
import json
import math
import statistics as st
from pathlib import Path

from fio_evaluation import femu_delta, os_metrics


def correlation(xs, ys):
    if len(xs) < 3:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    numerator = sum((x-mx)*(y-my) for x, y in zip(xs, ys))
    denominator = math.sqrt(sum((x-mx)**2 for x in xs) * sum((y-my)**2 for y in ys))
    return numerator / denominator if denominator else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', required=True)
    parser.add_argument('--window-seconds', type=int, default=30)
    args = parser.parse_args()
    root = Path(args.results).resolve()
    records, windows = [], []
    for summary in sorted(root.glob('*/*/*/summary.json')):
        record = json.loads(summary.read_text())[0]
        folder = summary.parent
        fs, group, cell = folder.relative_to(root).parts
        manifest = json.loads((folder/'manifest.json').read_text())
        raw = json.loads(next(folder.glob('*.fio.json')).read_text())['jobs'][0]
        start, end = record['measurement_start_us_approx'], record['end_us']
        samples = [json.loads(line) for line in next(folder.glob('*.samples.jsonl')).read_text().splitlines()]
        samples = [row for row in samples if start <= row['epoch_us'] <= end]
        os = os_metrics(samples)
        nand = femu_delta(root/'zns-stats.csv', start, end)
        logical = record['dm_delta'].get('logical_write_bytes', 0)
        record.update(fs=fs, group=group, cell=cell, repetition=int(cell.rsplit('-r', 1)[1]),
                      os_recomputed=os, nand=nand, fio_error=raw.get('error'),
                      module_sha256=manifest.get('storage_setup', {}).get('module_sha256'))
        record['nand_wa_dm'] = nand.get('nand_program_bytes', 0)/logical if logical and nand.get('available') else None
        log = []
        for line in next(folder.glob('*.fio_iops.1.log')).read_text().splitlines():
            fields = [value.strip() for value in line.split(',')]
            timestamp = int(fields[0])*1000
            if len(fields) > 2 and fields[2] == '1' and start <= timestamp <= end:
                log.append((timestamp, float(fields[1])))
        record['explicit_zero_iops_samples'] = sum(v == 0 for _, v in log)
        record['iops_log_max_gap_seconds'] = max(((b[0]-a[0])/1e6 for a,b in zip(log,log[1:])), default=0)
        duration = manifest['measure']
        for number in range(math.ceil(duration/args.window_seconds)):
            left = start + number*args.window_seconds*1000000
            right = min(start+duration*1000000, left+args.window_seconds*1000000)
            sr = [row for row in samples if left <= row['epoch_us'] <= right]
            values = [v for t,v in log if left <= t < right]
            metrics = os_metrics(sr)
            row = dict(fs=fs, group=group, cell=cell, window=number+1,
                       offset_seconds=number*args.window_seconds,
                       fio_logged_iops_mean=st.mean(values) if values else None,
                       iops_samples=len(values), steal_pct=metrics.get('cpu_steal_pct'),
                       max_vcpu_steal_pct=max(metrics.get('per_cpu_steal_pct', {}).values(),default=0))
            device = metrics.get('devices', {}).get('dm-0', {})
            elapsed = metrics.get('sample_seconds', 0)
            row['dm_completed_write_mib_s'] = device.get('write_bytes', 0)/2**20/elapsed if elapsed else None
            if len(sr)>1 and 'dm' in sr[0] and 'dm' in sr[-1]:
                a,b=sr[0]['dm'],sr[-1]['dm']
                row.update(gc_reclaims=b.get('gc_reclaims',0)-a.get('gc_reclaims',0),
                           gc_write_mib=(b.get('gc_write_bytes',0)-a.get('gc_write_bytes',0))/2**20,
                           free_zones_min=min(r.get('dm',{}).get('free_zones',999) for r in sr))
            windows.append(row)
        own = [w for w in windows if w['fs']==fs and w['group']==group and w['cell']==cell]
        paired=[w for w in own if w['dm_completed_write_mib_s'] is not None and w['steal_pct'] is not None]
        record['window_steal_dm_bw_correlation']=correlation([w['steal_pct'] for w in paired],[w['dm_completed_write_mib_s'] for w in paired])
        record['window_gc_dm_bw_correlation']=correlation([w.get('gc_write_mib',0) for w in paired],[w['dm_completed_write_mib_s'] for w in paired])
        records.append(record)
    grouped=[]
    for fs in ('ext4','f2fs'):
        for group in ('low','gc'):
            for workload,bs in [('seq-write','4k'),('rand-write','4k'),('seq-write','1M')]:
                rs=[r for r in records if (r['fs'],r['group'],r['workload'],r['block_size'])==(fs,group,workload,bs)]
                if not rs: continue
                row=dict(fs=fs,group=group,workload=workload,bs=bs,repeats=len(rs))
                for key in ('write_iops','write_mib_s','write_p95_ms','write_p99_ms','write_p99.9_ms','dm_wa_issued','nand_wa_dm'):
                    vals=[r[key] for r in rs if r.get(key) is not None]
                    if vals:
                        row[key]=dict(median=st.median(vals),min=min(vals),max=max(vals),
                                      cv_pct=st.stdev(vals)/st.mean(vals)*100 if len(vals)>1 and st.mean(vals) else 0)
                row['steal_pct']=[round(r['os_recomputed'].get('cpu_steal_pct',0),3) for r in rs]
                row['gc_reclaims']=[r['dm_delta']['gc_reclaims'] for r in rs]
                row['gc_moved_gib']=[round(r['dm_delta']['gc_moved_bytes']/2**30,3) for r in rs]
                grouped.append(row)
    (root/'analysis-records.json').write_text(json.dumps(records,indent=2))
    (root/'analysis-groups.json').write_text(json.dumps(grouped,indent=2))
    with (root/'analysis-windows.csv').open('w',newline='') as stream:
        fields=sorted(set().union(*(w.keys() for w in windows)))
        writer=csv.DictWriter(stream,fields);writer.writeheader();writer.writerows(windows)
    print(json.dumps(grouped,indent=2))
    print('VALIDATION',len(records),'fio errors',sum(bool(r['fio_error']) for r in records),
          'kernel errors',sum(bool(r['kernel_storage_errors']) for r in records),
          'missing kernel',sum(not r['kernel_log_available'] for r in records))


if __name__ == '__main__':
    main()

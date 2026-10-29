#!/usr/bin/env python3
"""최종 fio 1-1/1-2 계획 및 단일 회차 명령 생성. 자동 접속·초기화·실행 없음."""
import argparse
import json
from pathlib import Path
import shlex

def cells(experiment, repetitions=None):
    repetitions = repetitions if repetitions is not None else (5 if experiment == '1-1' else 1)
    if experiment not in ('1-1', '1-2') or repetitions < 1:
        raise ValueError('invalid experiment/repetitions')
    out=[]
    for repeat in range(1, repetitions+1):
        for workload, bs in [('seq-write','4k'),('rand-write','4k'),('seq-write','1M')]:
            out.append(dict(index=len(out)+1,experiment=experiment,repeat=repeat,
                            workload=workload,bs=bs,qd=32,jobs=1,
                            file_gib=1 if experiment=='1-1' else 18,
                            warmup=20 if experiment=='1-1' else 0,
                            measure=120 if experiment=='1-1' else None,
                            total_write_gib=None if experiment=='1-1' else 54,
                            randseed=20261019))
    return out

def command(cell, backend, filesystem, output_root, module_path=None, wal_delay=None):
    if backend=='native-f2fs-zns' and filesystem!='f2fs':
        raise ValueError('native only supports F2FS')
    if wal_delay is not None and backend!='dm-zns':
        raise ValueError('WAL option only applies to DM')
    if backend=='dm-zns' and not module_path:
        raise ValueError('explicit guest-built module required')
    output=Path(output_root)/cell['experiment']/backend/filesystem/f"cell-{cell['index']:02}"
    if backend=='dm-zns':
        output=output.parent/('wal-default' if wal_delay is None else f'wal-{wal_delay}ms')/output.name
    cmd=['env','BENCH_STORAGE_BACKEND='+('dm-zns' if backend=='native-f2fs-zns' else backend),
         'BENCH_FILESYSTEMS='+filesystem,'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0',
         'DM_GC_RESERVED_ZONES=8','DM_GC_LOW_WATERMARK=16','DM_GC_HIGH_WATERMARK=20',
         'DM_LOGICAL_CAPACITY_PERCENT=75',
         'python3','src/fio_evaluation.py','--backend',backend,'--filesystem',filesystem,
         '--evaluation','performance' if cell['experiment']=='1-1' else 'gc-efficiency',
         '--workloads',cell['workload'],'--bs',cell['bs'],'--qds',str(cell['qd']),
         '--file-gib',str(cell['file_gib']),'--warmup',str(cell['warmup']),
         '--randseed',str(cell['randseed']),'--rounds','1','--output-dir',str(output)]
    if backend=='native-f2fs-zns':
        cmd+=['--mount','/result/native-f2fs','--devices','nvme0n1,nvme1n1']
    else:
        cmd+=['--prepare']
    if backend=='cns':cmd+=['--devices','nvme0n1']
    if module_path:cmd+=['--module-path',module_path]
    if wal_delay is not None:cmd+=['--wal-delay-ms',str(wal_delay)]
    if cell['total_write_gib'] is not None:
        cmd+=['--total-write-gib',str(cell['total_write_gib']),'--safety-timeout','10800']
    else:cmd+=['--measure',str(cell['measure'])]
    return cmd+['--run']

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--experiment',required=True,choices=['1-1','1-2'])
    p.add_argument('--backend',required=True,choices=['dm-zns','cns','native-f2fs-zns'])
    p.add_argument('--filesystem',required=True,choices=['ext4','f2fs'])
    p.add_argument('--module-path')
    p.add_argument('--wal-delay-ms',type=int,choices=[0,2])
    p.add_argument('--repetitions',type=int)
    p.add_argument('--output-root',required=True)
    a=p.parse_args()
    try:
        rows=[dict(cell,command=shlex.join(command(cell,a.backend,a.filesystem,a.output_root,
             a.module_path,a.wal_delay_ms))) for cell in cells(a.experiment,a.repetitions)]
    except ValueError as exc:p.error(str(exc))
    print(json.dumps(dict(experiment=a.experiment,cases=rows,
        prerequisite='각 회차 새 FEMU 및 독립 FS. Native는 별도 multi-device 준비 필수. 명령은 장치를 지우므로 실행 전 확인'),ensure_ascii=False,indent=2))

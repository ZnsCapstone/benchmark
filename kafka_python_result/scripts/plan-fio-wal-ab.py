#!/usr/bin/env python3
"""WAL A/B 진단 명령 생성기. 실행/접속/초기화는 하지 않는다."""
import argparse
import json
import shlex

def plan(filesystem):
    # 시간 흐름 편향을 줄이는 ABBA. 각 지연/워크로드별 독립 2회.
    return [dict(index=i+1,filesystem=filesystem,delay_ms=delay,workload=workload,
                 bs='4k',qd=32,file_gib=1,warmup=20,measure=120)
            for i,(delay,workload) in enumerate(
                (d,w) for w in ('seq-write','rand-write') for d in (2,0,0,2))]

def command(cell,module,output):
    return ['env','BENCH_STORAGE_BACKEND=dm-zns','BENCH_FILESYSTEMS='+cell['filesystem'],
            'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0',
            'DM_GC_RESERVED_ZONES=8','DM_GC_LOW_WATERMARK=16','DM_GC_HIGH_WATERMARK=20',
            'DM_LOGICAL_CAPACITY_PERCENT=75','DM_GC_DIAG_BUDGET=0',
            'python3','src/fio_evaluation.py','--prepare','--backend','dm-zns',
            '--module-path',module,'--filesystem',cell['filesystem'],
            '--wal-delay-ms',str(cell['delay_ms']),'--workloads',cell['workload'],
            '--bs',cell['bs'],'--qds',str(cell['qd']),'--file-gib',str(cell['file_gib']),
            '--warmup',str(cell['warmup']),'--measure',str(cell['measure']),
            '--rounds','1','--output-dir',f"{output}/cell-{cell['index']:02}",'--run']

def remaining_plan(filesystem):
    cells=[]
    for delay in (2,0,0,2):
        cells.append(dict(filesystem=filesystem,delay_ms=delay,workload='seq-write',
                          bs='1M',qd=32,file_gib=1,warmup=20,measure=120,group='short-1m'))
    for workload,bs,delays in [('seq-write','4k',(2,0)),
                               ('rand-write','4k',(0,2)),
                               ('seq-write','1M',(0,2))]:
        for delay in delays:
            cells.append(dict(filesystem=filesystem,delay_ms=delay,workload=workload,
                              bs=bs,qd=32,file_gib=18,warmup=30,measure=600,group='gc18'))
    return [dict(cell,index=i+1,repeat=1) for i,cell in enumerate(cells)]

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--filesystem',choices=['ext4','f2fs'],default='f2fs')
    p.add_argument('--module-path',required=True,help='게스트에서 빌드한 별도 진단 모듈')
    p.add_argument('--output-root',required=True)
    a=p.parse_args()
    print(json.dumps(dict(requirement='매 회차 새 team4 FEMU 32GiB, 512MiB×64 zone; 기존 실험 종료 및 실패 증거 보존 후 실행',
        cells=[dict(cell,command=shlex.join(command(cell,a.module_path,a.output_root)))
               for cell in plan(a.filesystem)]),ensure_ascii=False,indent=2))

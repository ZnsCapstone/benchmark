#!/usr/bin/env python3
"""WAL 단계 분해 진단: EXT4 순차4KiB/1MiB 각3회, 독립 FEMU, WAL0."""
import importlib.util
import json
import shlex
import sys
from pathlib import Path

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('monitor',HERE/'monitor-v11-then-short.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.INPUT=m.ROOT/'results/final/wal-stage-input-20261020'
m.REL='results/final/1.1v-wal-stage-diag-20261020'
m.REMOTE='/home/zns/wal-stage-diag-20261020/module-src'

def main():
    v=m.load_controller()
    base={c['bs']:c for c in v.plan() if c['experiment']=='1-1' and c['filesystem']=='ext4' and c['workload']=='seq-write' and c['repeat']==1}
    cells=[]
    for repeat in range(1,4):
        for bs in ('4k','1M'):
            cells.append(dict(base[bs],index=len(cells)+1,repeat=repeat))
    if '--run' not in sys.argv:
        print(json.dumps(cells,ensure_ascii=False,indent=2));return
    m.verify_inputs()
    import fcntl
    lock=open(m.INPUT/'monitor.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    v.c.active_guest_guard()
    v.BUILD='/home/zns/wal-stage-diag-20261020'
    v.RUNNER=v.BUILD+'/runner/src/fio_evaluation.py'
    v.c.call('guest','test ! -e '+shlex.quote(v.BUILD)+' && mkdir -p '+shlex.quote(v.BUILD+'/runner')+
             ' && cp -a /home/zns/fio-1.1v-20261019/runner/src '+shlex.quote(v.BUILD+'/runner/'))
    v.n.guest_scp([m.INPUT/'fio_evaluation.py'],v.RUNNER)
    v.n.check_hash('guest',v.RUNNER,'152871249ce3fcbb15787852e7b4b8b2941d9520b09dd02b49f1e7b24fbe70a7')
    m.start_short(v,cells)

if __name__=='__main__':
    try:main()
    except BaseException as exc:
        m.event(dict(state='stopped-error',error=str(exc)));raise

#!/usr/bin/env python3
"""team4 WAL 진단 8회. 독립 FEMU, 기존 저장소/모듈 보존, 실패 시 후속 실행 중단."""
import argparse
import csv
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
def module(name,file):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(file))
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value
d=module('dm_common','run-fio-dm512-controller.py')
p=module('ab_plan','plan-fio-wal-ab.py')
c=d.c
n=d.n
BUILD='/home/zns/wal-diag-20261019'
MODULE=BUILD+'/module-src/dm-zns-base.ko'
RUNNER=BUILD+'/runner/src/fio_evaluation.py'
NAME='wal-ab-f2fs-short-20261019'

def validate(row,manifest,cell):
    if manifest.get('wal_delay_ms')!=cell['delay_ms']:
        raise RuntimeError('지연 요청값 불일치')
    samples=row.get('wal_diagnostics')
    delta=row.get('dm_delta',{})
    if not samples or not delta.get('wal_diag_commit_count') or delta.get('wal_diag_io_errors',0):
        raise RuntimeError('진단 계측 누락 또는 WAL I/O 오류')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true')
    parser.add_argument('--suite',choices=['short','remaining'],default='short')
    args=parser.parse_args()
    cells=([dict(x,group='wal-ab',repeat=1) for x in p.plan('f2fs')]
           if args.suite=='short' else p.remaining_plan('f2fs'))
    name=NAME if args.suite=='short' else 'wal-ab-f2fs-remaining-20261019'
    print(json.dumps(cells,ensure_ascii=False,indent=2),flush=True)
    if not args.run:return
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    local=ROOT/'results/wal-ab'/name
    local.mkdir(parents=True,exist_ok=False)
    groot=c.GUEST_ROOT+'/results/wal-ab/'+name
    hroot=c.HOST_ROOT+'/results/'+name
    done=[]
    def status(state,**kw):
        c.save(local/'status.json',dict(state=state,completed=len(done),total=len(cells),**kw))
    try:
        c.active_guest_guard()
        # 기존 저장소의 baseline은 이번 실험에서 덮어쓰지 않는다.
        n.check_hash('guest','/home/zns/dm-zns-base/src/dm-zns-base-main.c',
                     'abf8db2075e851a6653c8d87deb34c01a2b731850d1877ed876799611bb7c118')
        module_sha=n.read_call('guest','sha256sum '+MODULE).stdout.split()[0]
        runner_sha=hashlib.sha256((ROOT/'src/fio_evaluation.py').read_bytes()).hexdigest()
        setup_sha=hashlib.sha256((ROOT/'src/system_setup.py').read_bytes()).hexdigest()
        n.check_hash('guest',RUNNER,runner_sha)
        n.check_hash('guest',BUILD+'/runner/src/system_setup.py',setup_sha)
        source_sha=hashlib.sha256((ROOT.parent/'dm-zns-base/src/dm-zns-base-main.c').read_bytes()).hexdigest()
        n.check_hash('guest',BUILD+'/module-src/dm-zns-base-main.c',source_sha)
        launcher_sha=hashlib.sha256((ROOT/'scripts/run-dm-zns-512m.sh').read_bytes()).hexdigest()
        config=dict(cells=cells,module_path=MODULE,module_sha256=module_sha,source_sha256=source_sha,
                    runner_sha256=runner_sha,setup_sha256=setup_sha,physical_gib=32,logical_gib=24,
                    zone_mib=512,gc_reserve_low_high=[8,16,20],fresh_femu_per_cell=True,
                    femu_sha256=c.EXPECTED_FEMU_SHA,launcher_sha256=launcher_sha,
                    baseline_preserved=True)
        if args.suite=='remaining':
            previous=ROOT/'results/wal-ab'/NAME
            if json.loads((previous/'status.json').read_text()).get('state')!='completed':
                raise RuntimeError('선행 short A/B 미완료')
            baseline=json.loads((previous/'plan.json').read_text())
            for key in ['module_sha256','source_sha256','runner_sha256','setup_sha256',
                        'femu_sha256','launcher_sha256']:
                if config[key]!=baseline[key]:raise RuntimeError('선행 short와 바이너리/소스 불일치: '+key)
        c.save(local/'plan.json',config)
        diff=subprocess.check_output(['git','-C',str(ROOT.parent/'dm-zns-base'),'diff','HEAD','--binary'])
        (local/'diagnostic-source.patch').write_bytes(diff)
        for index,cell in enumerate(cells,1):
            status('booting',index=index,cell=cell)
            n.shutdown()
            out=groot+f'/cell-{index:02}'
            host=hroot+f'/cell-{index:02}'
            d.boot(host,name+f'-{index:02}',launcher_sha)
            initial=list(csv.DictReader(n.read_call('host','head -2 '+shlex.quote(host+'/zns.vZNSSD0.csv')).stdout.splitlines()))
            if len(initial)!=1 or int(initial[0]['host_write_bytes']):
                raise RuntimeError('새 FEMU 초기 counter 불일치')
            boot=n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if boot in {r['boot_id'] for r in done}:raise RuntimeError('독립 부팅 ID 중복')
            n.check_hash('guest',MODULE,module_sha)
            n.check_hash('guest',RUNNER,runner_sha)
            n.check_hash('guest',BUILD+'/runner/src/system_setup.py',setup_sha)
            c.save(local/f'cell-{index:02}.provenance.json',n.provenance())
            command=p.command(cell,MODULE,groot)
            command[command.index('src/fio_evaluation.py')]=RUNNER
            shell=shlex.join(command)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; mkdir -p '+shlex.quote(out)+'; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'/wrapper-exit.json')
            session=f'wal-ab-cell-{index:02}'
            c.call('guest','mkdir -p '+shlex.quote(groot)+' && '+shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]))
            status('running',index=index,cell=cell,boot_id=boot,destination=out)
            print(f'[{index}/{len(cells)}] {cell}',flush=True)
            deadline=time.monotonic()+cell['file_gib']*600+cell['warmup']+cell['measure']+600
            while time.monotonic()<deadline:
                time.sleep(30)
                try:
                    r=c.call('guest','if test -s '+shlex.quote(out+'/wrapper-exit.json')+'; then cat '+shlex.quote(out+'/wrapper-exit.json')+'; elif tmux has-session -t '+session+' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi',timeout=20,check=False)
                except subprocess.TimeoutExpired:continue
                if r.returncode==255:continue
                if r.returncode:raise RuntimeError('회차 상태 조회 오류')
                value=r.stdout.strip()
                if value=='RUNNING':continue
                if value=='STOPPED' or json.loads(value).get('exit_code')!=0:
                    raise RuntimeError('회차 실패: VM 및 로그 보존; 다음 조건 중단')
                break
            else:raise RuntimeError('회차 제한 초과: 실행 보존')
            # 현재 파라미터와 원본을 둘 다 확인한다.
            code="from pathlib import Path; p=Path('/sys/module/dm_zns_base/parameters'); print((p/'wal_timing_enabled').read_text().strip(), (p/'wal_batch_delay_ms').read_text().strip())"
            actual=n.read_call('guest','python3 -c '+shlex.quote(code)).stdout.split()
            if actual!=['Y',str(cell['delay_ms'])]:raise RuntimeError('실제 모듈 파라미터 불일치')
            record=d.collect(local,out,host,cell,index)
            manifest=json.loads((local/f'cell-{index:02}/manifest.json').read_text())
            validate(record['summary'],manifest,cell)
            record['boot_id']=boot
            record['module_sha256']=module_sha
            done.append(record)
            c.save(local/'completed-cells.json',done)
            status('cell-completed',index=index,cell=cell)
            n.guest_scp([local/'plan.json',local/'status.json',local/'completed-cells.json'],groot+'/')
        status('completed')
        c.save(local/'exit-code.json',dict(exit_code=0))
        n.guest_scp([local/'status.json',local/'exit-code.json'],groot+'/')
    except BaseException as exc:
        status('failed',error=str(exc))
        c.save(local/'exit-code.json',dict(exit_code=1))
        raise

if __name__=='__main__':main()

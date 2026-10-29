#!/usr/bin/env python3
"""team4 기존 1.1v 종료를 600초마다 확인하고 보존된 수정 소스로 short만 실행.

기존 실행은 중지하지 않는다. 실패 시 자동 초기화/재시작하지 않고 증거를 보존한다.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
INPUT = ROOT/'results/final/wal-queue-fix-input-20261020'
REL = 'results/final/1.1v-wal-queue-fix-short-20261020'
REMOTE = '/home/zns/wal-queue-fix-short-20261020/module-src'

def load_controller():
    spec=importlib.util.spec_from_file_location('v11',HERE/'run-fio-v11-controller.py')
    v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
    return v

def stamp():
    return datetime.datetime.now().astimezone().isoformat()

def event(data):
    data=dict(at=stamp(),**data)
    with (INPUT/'monitor.jsonl').open('a') as f:f.write(json.dumps(data,ensure_ascii=False)+'\n')
    print(json.dumps(data,ensure_ascii=False),flush=True)

def verify_inputs():
    manifest=json.loads((INPUT/'input-hashes.json').read_text())
    for name,sha in manifest.items():
        if hashlib.sha256((INPUT/'module'/name).read_bytes()).hexdigest()!=sha:
            raise RuntimeError('보존 소스 SHA 변경: '+name)

def start_short(v, selected_cells=None):
    verify_inputs()
    # 완료된 기존 실행기가 lock과 collection을 모두 끝냈는지 기다린다.
    import fcntl
    with open('/tmp/kovalt-cns-fio-controller.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    v.c.active_guest_guard()
    v.c.call('guest','test ! -e '+shlex.quote(REMOTE)+' && mkdir -p '+shlex.quote(REMOTE))
    v.n.guest_scp([INPUT/'module'/name for name in json.loads((INPUT/'input-hashes.json').read_text())],REMOTE+'/')
    for name,sha in json.loads((INPUT/'input-hashes.json').read_text()).items():
        v.n.check_hash('guest',REMOTE+'/'+name,sha)
    result=v.c.call('guest','make -C '+shlex.quote(REMOTE)+' W=1 && sha256sum '+shlex.quote(REMOTE+'/dm-zns-base.ko'),timeout=180)
    (INPUT/'guest-build.log').write_text(result.stdout+result.stderr)
    sha=result.stdout.splitlines()[-1].split()[0]
    if len(sha)!=64:raise RuntimeError('guest 모듈 SHA 파싱 실패')
    v.n.check_hash('guest',REMOTE+'/dm-zns-base.ko',sha)
    original_plan=v.plan
    original_validate=v.validate_plan
    def short_plan():return original_plan()[:12] if selected_cells is None else selected_cells
    def validate(cells):
        original_validate(original_plan())
        if cells!=short_plan() or any(c['experiment']!='1-1' for c in cells):
            raise RuntimeError('short 계획 불일치')
    v.plan=short_plan;v.validate_plan=validate
    v.REL=REL;v.MODULE=REMOTE+'/dm-zns-base.ko';v.MODULE_SHA=sha
    v.LOCAL_MODULE_SOURCE=INPUT/'module/dm-zns-base-main.c'
    event(dict(state='starting-short',relative_output=REL,module_sha256=sha,cases=len(short_plan())))
    sys.argv=[str(HERE/'run-fio-v11-controller.py'),'--run']
    v.main()
    event(dict(state='short-completed',relative_output=REL))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true')
    parser.add_argument('--start-short-now',action='store_true',help='기존 실행 중단/보존 후 short만 즉시 시작')
    args=parser.parse_args()
    verify_inputs()
    v=load_controller()
    if not args.run:
        print(json.dumps(dict(interval_seconds=600,wait_for='1.1v all 18 cases',then=REL,cases=12)))
        return
    import fcntl
    monitor_lock=open(INPUT/'monitor.lock','a')
    fcntl.flock(monitor_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if args.start_short_now:
        start_short(v)
        return
    deadline=time.monotonic()+48*3600
    while time.monotonic()<deadline:
        folder=ROOT/'results/final/1.1v'
        status=json.loads((folder/'status.json').read_text())
        try:
            command="date -Is; ps -C fio -o pid,etime,stat,wchan:22,args --width 240"
            result=v.c.call('guest',command,timeout=20,check=False)
            event(dict(state='check',status=status,guest_rc=result.returncode,guest=result.stdout,stderr=result.stderr))
        except subprocess.TimeoutExpired:
            event(dict(state='ssh-timeout',status=status))
        if status['state']=='failed':raise RuntimeError('기존 실험 실패: 자동 전환 금지')
        if status['state']=='completed':
            done=json.loads((folder/'completed-cells.json').read_text())
            if len(done)!=18:raise RuntimeError('기존 실험 완료 수 불일치')
            v.validate_resume(folder,v.plan())
            event(dict(state='old-suite-preserved',completed=len(done)))
            # 완료 직후 전환하되 기존 실행기의 최종 scp/종료를 30초 기다린다.
            time.sleep(30)
            start_short(v)
            return
        time.sleep(600)
    raise RuntimeError('모니터 48시간 한도 초과: 원격 실행 보존')

if __name__=='__main__':
    try:main()
    except BaseException as exc:
        if INPUT.exists():event(dict(state='stopped-error',error=str(exc)))
        raise

#!/usr/bin/env python3
"""기존 F2FS 전체 성공 후에만 같은 조건의 EXT4 실행. 네트워크 대기 조회 없음."""
import datetime
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'results/zns-512m'
PREVIOUS=BASE/'dm512-f2fs-fio-short5-gc1-20261017'
STATUS=BASE/'dm512-ext4-queue-status.json'

def save(state,**extra):
    value=dict(state=state,time=datetime.datetime.now().astimezone().isoformat(),**extra)
    temporary=STATUS.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2))
    temporary.replace(STATUS)
    print(value,flush=True)

if __name__=='__main__':
    save('waiting-for-f2fs')
    while True:
        try:
            status=json.loads((PREVIOUS/'status.json').read_text())
        except (FileNotFoundError,json.JSONDecodeError):
            time.sleep(60)
            continue
        if status['state']=='failed':
            save('blocked',reason='F2FS 실패: 자동 초기화하지 않음')
            sys.exit(1)
        if status['state']=='completed':
            try:
                result=json.loads((PREVIOUS/'exit-code.json').read_text())
                completed=json.loads((PREVIOUS/'completed-cells.json').read_text())
            except (FileNotFoundError,json.JSONDecodeError):
                time.sleep(60)
                continue
            if result.get('exit_code')!=0 or len(completed)!=18:
                save('blocked',reason='F2FS 완료 검증 불일치')
                sys.exit(1)
            # 마지막 원격 결과 복사와 기존 controller lock 해제를 기다린다.
            time.sleep(30)
            break
        time.sleep(60)
    save('starting-ext4')
    result=subprocess.run([sys.executable,str(ROOT/'scripts/run-fio-dm512-controller.py'),
                           '--filesystem','ext4','--run'])
    save('finished' if result.returncode==0 else 'failed',exit_code=result.returncode)
    sys.exit(result.returncode)

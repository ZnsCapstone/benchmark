"""원격 tmux에서 제어기 비정상 종료를 복구한다. 신원 오류·명시적 STOP은 존중한다."""
import json
from pathlib import Path
import subprocess
import time

HERE=Path(__file__).resolve().parent
ROOT=Path('/home/zns-team4/results/final/missing-results-20261028')
for attempt in range(10):
    if (ROOT/'STOP').exists():break
    cmd=['python3','-u',str(HERE/'controller.py')]
    if (ROOT/'plan.json').exists():cmd+=['--resume']
    with (HERE/'controller.log').open('a') as log:
        result=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
    try:state=json.loads((ROOT/'status.json').read_text())
    except (FileNotFoundError,json.JSONDecodeError):state={}
    if result.returncode==0 or state.get('state')=='completed':break
    if state.get('error_type') in ['IdentityError','UserStop'] or (ROOT/'STOP').exists():break
    time.sleep(10)

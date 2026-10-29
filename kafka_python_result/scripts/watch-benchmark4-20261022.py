#!/usr/bin/env python3
"""team4 host: 읽기 전용 15분 점검, 자동 수정/재시작 없음."""
import datetime
import fcntl
import importlib.util
import json
from pathlib import Path
import shlex
import time
import os

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('base',HERE/'controller-base.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
RUN_ID=os.environ.get('BENCH4_RUN_ID','benchmark4-final-20261022')
if RUN_ID not in ('benchmark4-final-20261022','benchmark4-final-20261022-r2'):
    raise RuntimeError('허용되지 않은 실행 경로')
OUT=Path('/home/zns-team4/results/final')/RUN_ID


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    lock=(OUT/'watcher.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    while True:
        row={'at':datetime.datetime.now().astimezone().isoformat()}
        try:
            p=OUT/'status.json';status=json.loads(p.read_text())
            row.update(state=status.get('state'),completed=status.get('completed'),
                       cell=status.get('cell'),error=status.get('error'),
                       controller_status_age_sec=time.time()-p.stat().st_mtime)
            row['warnings']=[]
            if row['controller_status_age_sec']>180:
                row['warnings'].append('제어기 상태 갱신 180초 초과')
            if row['state'] not in ('completed','failed'):
                fs=(status.get('cell') or {}).get('filesystem')
                if fs not in ('ext4','f2fs'):raise RuntimeError('예상치 못한 조건')
                folder='/home/zns/kafka_python_result/results/final/'+RUN_ID+'/'+fs
                code="""import pathlib,json,subprocess
p=pathlib.Path(FOLDER)
log=pathlib.Path(str(p)+'.launch.log')
r={'log_tail':log.read_text(errors='replace')[-2500:] if log.exists() else None,
   'log_mtime':log.stat().st_mtime if log.exists() else None}
for name in ['java','fio']:
 v=subprocess.run(['pgrep','-a','-x',name],capture_output=True,text=True,timeout=5)
 r[name]=v.stdout[:2000]
v=subprocess.run(['sudo','-n','dmesg'],capture_output=True,text=True,timeout=10)
r['kernel_tail']='\\n'.join(v.stdout.splitlines()[-100:]);r['kernel_read_rc']=v.returncode
print(json.dumps(r))
""".replace('FOLDER',repr(folder))
                response=m.guest('python3 -c '+shlex.quote(code),timeout=25)
                row['guest']=json.loads(response.stdout)
                import re
                if re.search(r'I/O error|BUG:|Oops:|insufficient relocation workspace|journal.*abort',row['guest']['kernel_tail'],re.I):
                    row['warnings'].append('커널 tail 오류 의심: 원본 확인 필요')
            if row['state']=='failed':row['warnings'].append('실험 실패; 자동 수정/재시작하지 않음')
        except Exception as exc:
            row['check_error']=str(exc)
        with (OUT/'watch-15m.jsonl').open('a') as f:
            f.write(json.dumps(row,ensure_ascii=False)+'\n')
        m.save(OUT/'watch-latest.json',row)
        print(json.dumps(row,ensure_ascii=False),flush=True)
        if row.get('state') in ('completed','failed'):return
        time.sleep(900)


if __name__=='__main__':main()

#!/usr/bin/env python3
"""준비된 native F2FS에서 독립 fio 한 조건을 계측한다. 장치 초기화는 하지 않는다."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import fio_evaluation as fio


def snapshot(out, tag):
    start = time.time_ns() // 1000
    r = subprocess.run(['sudo', '-n', 'cat', '/sys/kernel/debug/f2fs/status'],
                       capture_output=True, text=True, timeout=15)
    (out / f'f2fs-{tag}.txt').write_text(r.stdout + r.stderr)
    return dict(start_us=start, end_us=time.time_ns() // 1000, available=r.returncode == 0)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True)
    p.add_argument('--cell-json', required=True)
    p.add_argument('--run', action='store_true')
    a = p.parse_args()
    c = json.loads(a.cell_json)
    if c['workload'] not in ('seq-write', 'rand-write') or c['bs'] not in ('4k', '1M'):
        p.error('지원하지 않는 부하')
    if c['qd'] != 32 or c['file_gib'] not in (1, 18) or min(c['measure'], c['repeat']) < 1:
        p.error('지원하지 않는 조건')
    if not a.run:
        print(json.dumps(c)); return
    out, mount = Path(a.output).resolve(), Path('/result/native-f2fs')
    if os.uname().nodename != 'zns-guest' or out.exists():
        raise RuntimeError('게스트/신규 결과 경로 불일치')
    if subprocess.check_output(['findmnt','-n','-o','SOURCE,FSTYPE,TARGET',str(mount)],text=True).split() != ['/dev/nvme0n1','f2fs',str(mount)]:
        raise RuntimeError('native F2FS 마운트 불일치')
    if out.parent.stat().st_dev == mount.stat().st_dev:
        raise RuntimeError('결과 디스크가 시험 장치와 같음')
    for cmd in (['pgrep','-x','fio'], ['pgrep','-f','kafka[.]Kafka']):
        if subprocess.run(cmd,stdout=subprocess.DEVNULL).returncode == 0:
            raise RuntimeError('다른 부하기가 실행 중')
    lock = open('/tmp/kovalt-fio-evaluation.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    size = c['file_gib'] * 1024**3
    usage = os.statvfs(mount)
    if size > usage.f_bavail*usage.f_frsize*.8:
        raise RuntimeError('사용자 가용 공간의 80% 초과')
    out.mkdir()
    manifest = dict(cell=c,backend='native-f2fs-zns',independent_repetitions=True,
        file_bytes=size,initial_available_bytes=usage.f_bavail*usage.f_frsize,
        requested_direct=True,effective_direct='buffered fallback; 사전 ftrace 및 커널 분기로 확인',
        native_mount=subprocess.check_output(['findmnt','-n','-o','OPTIONS',str(mount)],text=True).strip(),
        kernel=os.uname().release,fio_version=subprocess.check_output(['fio','--version'],text=True).strip(),
        hashes={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in
                [Path(__file__),Path(fio.__file__),Path('/home/zns/tools/f2fs-native-reserve-03aa49e/mkfs.f2fs')]})
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    data=mount/('.kovalt-'+out.name)
    data.mkdir()
    file=data/'data.bin'
    with file.open('xb') as f: f.truncate(size)
    try:
        (out/'phase.json').write_text('{"phase":"prepare-crc"}')
        fio.run_checked(fio.fio_command(file,size,'write','1M',4,0,0,out/'prepare.json',False),max(600,c['file_gib']*600))
        prep=json.loads((out/'prepare.json').read_text())
        if len(prep.get('jobs',[])) != 1 or prep['jobs'][0].get('error') or prep['jobs'][0]['write']['io_bytes'] != size or prep['jobs'][0]['read']['io_bytes'] != size:
            raise RuntimeError('준비/CRC 검증 불완전')
        before=snapshot(out,'before-warmup')
        measurement={}
        cancel=threading.Event()
        def snap_measurement():
            if not cancel.wait(c['warmup']):
                try: measurement.update(snapshot(out,'measurement-start'))
                except Exception as exc: measurement.update(available=False,error=str(exc))
        thread=threading.Thread(target=snap_measurement,daemon=True)
        sampler=fio.Sampler(mount,['nvme0n1','nvme1n1'],None,out/'samples.jsonl')
        start=time.time_ns()//1000
        (out/'phase.json').write_text(json.dumps(dict(phase='warmup-measure',start_us=start,cell=c)))
        sampler.thread.start(); thread.start()
        try:
            rw='randwrite' if c['workload']=='rand-write' else 'write'
            fio.run_checked(fio.fio_command(file,size,rw,c['bs'],32,c['warmup'],c['measure'],out/'measurement.fio.json'),c['warmup']+c['measure']+120,sampler)
        finally:
            cancel.set(); sampler.stop.set(); sampler.thread.join(timeout=5); thread.join(timeout=20)
        end=time.time_ns()//1000
        after=snapshot(out,'after-measurement')
        raw=json.loads((out/'measurement.fio.json').read_text())
        if raw['jobs'][0]['write']['runtime'] < c['measure']*1000*.95:
            raise RuntimeError('측정 runtime 불완전')
        kernel=subprocess.run(['sudo','-n','journalctl','-k','--no-pager','--since',f'@{start//1000000}','--until',f'@{end//1000000}'],capture_output=True,text=True,timeout=30)
        (out/'kernel.log').write_text(kernel.stdout+kernel.stderr)
        errors=re.findall(r'^.*(?:nvme.*(?:timeout|I/O error)|Buffer I/O error|F2FS-fs.*(?:ERROR|corrupt)|blk_update_request|BUG:|hung task).*$',kernel.stdout,re.M|re.I)
        if kernel.returncode or errors:
            raise RuntimeError('커널 로그 수집 실패 또는 저장장치 오류: '+str(errors))
        if not all(x.get('available') for x in [before,measurement,after]):
            raise RuntimeError('F2FS GC snapshot 수집 실패')
        r=dict(workload=c['workload'],block_size=c['bs'],start_us=start,
               measurement_start_us_approx=start+c['warmup']*1000000,end_us=end,
               **fio.parse_fio(raw),os=fio.os_metrics([x for x in sampler.rows if x['epoch_us']>=start+c['warmup']*1000000]),
               f2fs_snapshots=dict(before_warmup=before,measurement_start=measurement,after_measurement=after),
               kernel_log_available=True,kernel_storage_errors=errors,dm_available=False)
        (out/'summary.json').write_text(json.dumps([r],indent=2))
        (out/'phase.json').write_text('{"phase":"completed"}')
        (out/'exit-code.json').write_text('{"exit_code":0}')
    except BaseException as exc:
        (out/'failure.json').write_text(json.dumps(dict(error=str(exc))))
        (out/'exit-code.json').write_text('{"exit_code":1}')
        raise


if __name__=='__main__': main()

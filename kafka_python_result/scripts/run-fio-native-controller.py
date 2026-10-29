#!/usr/bin/env python3
"""Native F2FS 독립 fio 18회. 로컬 tmux에서 실행하며 team4 VM만 관리한다."""
import argparse
import csv
import datetime
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('common',Path(__file__).with_name('run-fio-cns-controller.py'))
common=importlib.util.module_from_spec(spec); spec.loader.exec_module(common)
spec=importlib.util.spec_from_file_location('fio_helpers',ROOT/'src/fio_evaluation.py')
fio=importlib.util.module_from_spec(spec); spec.loader.exec_module(fio)
LAUNCHER='/home/zns-team4/run-native-f2fs.sh'
LAUNCHER_SHA='a377dc516fcdbecc0d7856e33da91ac3beab2cef268a4a94b3f5c1ceedc605ee'
GROOT=common.GUEST_ROOT
FMT=GROOT.replace('/kafka_python_result','')+'/tools/f2fs-native-reserve-03aa49e/mkfs.f2fs'
FMT_SHA='28008104b1137467e2768c0c3ada39e7529c76a4f9ede2e8c09e0daa4ad1af34'
FIO_SHA='c49be6a5d6bf56bb52c7c5f1314b74bd6032fbe8e5d916f3ecfb440ceb17e629'


def plan():
    return [c for c in common.make_plan() if c['filesystem']=='f2fs']


def read_call(target,command,timeout=60):
    """읽기 전용 검사만 일시적 SSH 단절 시 재시도한다."""
    for attempt in range(5):
        try:
            r=common.call(target,command,timeout=timeout,check=False)
            if r.returncode==0: return r
            if r.returncode!=255: raise subprocess.CalledProcessError(r.returncode,command,r.stdout,r.stderr)
            error=subprocess.CalledProcessError(r.returncode,command,r.stdout,r.stderr)
        except subprocess.TimeoutExpired as exc: error=exc
        if attempt<4: time.sleep(5)
    raise error


def check_hash(target,path,expected):
    if read_call(target,'sha256sum '+shlex.quote(path)).stdout.split()[0]!=expected:
        raise RuntimeError('파일 변경 감지: '+path)


def guest_scp(sources,dest):
    subprocess.run(['scp',*common.GUEST[:-1],*map(str,sources),'zns@cns-team4:'+dest],check=True,timeout=60)


def shutdown():
    common.active_guest_guard()
    common.call('guest','if mountpoint -q /result/native-f2fs; then sudo -n umount /result/native-f2fs; fi',timeout=60)
    common.shutdown_guest()


def boot(host_cell,name,index):
    r=common.call('host',"pgrep -a -u zns-team4 -f '[q]emu-system-x86_64'",check=False)
    if r.returncode!=1: raise RuntimeError('다른 team4 FEMU가 존재하거나 호스트 확인 실패')
    check_hash('host',common.FEMU,common.EXPECTED_FEMU_SHA)
    check_hash('host',LAUNCHER,LAUNCHER_SHA)
    common.call('host','test ! -e '+shlex.quote(host_cell)+' && mkdir -p '+shlex.quote(host_cell))
    shell=shlex.join(['env','-u','FEMU_BIN','FEMU_STATS_FILE='+host_cell+'/cns.csv',
        'FEMU_ZNS_STATS_PREFIX='+host_cell+'/zns','bash',LAUNCHER])+' > '+shlex.quote(host_cell+'/console.log')+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(host_cell+'/exit-code.txt')
    common.call('host',shlex.join(['tmux','new-session','-d','-s',name+f'-{index:02}','bash','-lc',shell]))
    for _ in range(60):
        try:
            r=common.call('guest','test "$(hostname)" = zns-guest && '
                'test "$(cat /sys/block/nvme0n1/size)" = 2097152 && '
                'test "$(cat /sys/block/nvme1n1/size)" = 67108864 && '
                'test "$(cat /sys/block/nvme1n1/queue/chunk_sectors)" = 1048576 && sudo -n true',timeout=15,check=False)
            if r.returncode==0: return
        except subprocess.TimeoutExpired: pass
        time.sleep(3)
    raise RuntimeError('native VM 부팅 확인 제한 초과; 강제 종료하지 않음')


def fresh_stats(host_cell):
    for name in ['cns.csv','zns.vZNSSD0.csv']:
        text=read_call('host','head -2 '+shlex.quote(host_cell+'/'+name)).stdout
        rr=list(csv.DictReader(text.splitlines()))
        if len(rr)!=1 or int(rr[0]['host_write_bytes']):
            raise RuntimeError('새 FEMU 초기 통계 검증 실패')


def run_cell(command,out,session,deadline_seconds):
    shell=shlex.join(command)+' > '+shlex.quote(out+'.launch.log')+' 2>&1'
    r=common.call('guest',shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]),check=False)
    if r.returncode: raise RuntimeError('게스트 fio 세션 시작 실패; 중복 재시작하지 않음')
    deadline=time.monotonic()+deadline_seconds
    while time.monotonic()<deadline:
        time.sleep(30)
        cmd='if test -s '+shlex.quote(out+'/exit-code.json')+'; then cat '+shlex.quote(out+'/exit-code.json')+'; elif tmux has-session -t '+shlex.quote(session)+' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi'
        try: r=common.call('guest',cmd,check=False,timeout=20)
        except (subprocess.TimeoutExpired,OSError): continue
        if r.returncode==255: continue
        if r.returncode: raise RuntimeError('fio 상태 확인 실패')
        state=r.stdout.strip()
        if state=='RUNNING': continue
        if state=='STOPPED': raise RuntimeError('fio 결과 없이 세션 종료; 증거 보존')
        if json.loads(state).get('exit_code')!=0:
            raise RuntimeError('fio 회차 실패; 다음 조건 실행하지 않음')
        return
    raise RuntimeError('fio 전체 제한 초과; 현재 실행은 보존')


def validate(cell,manifest,row,raw,prep,exit_code):
    if manifest.get('cell')!=cell or manifest.get('backend')!='native-f2fs-zns':
        raise RuntimeError('회차 manifest 불일치')
    if exit_code.get('exit_code')!=0: raise RuntimeError('회차 exit 실패')
    for payload in [raw,prep]:
        if len(payload.get('jobs',[]))!=1 or payload['jobs'][0].get('error',0):
            raise RuntimeError('fio/준비 결과 불완전 또는 오류')
    size=cell['file_gib']*1024**3
    if any(prep['jobs'][0].get(d,{}).get('io_bytes')!=size for d in ['read','write']):
        raise RuntimeError('준비 CRC 검증량 불일치')
    if raw['jobs'][0]['write']['runtime']<cell['measure']*1000*.95:
        raise RuntimeError('fio 측정 시간 부족')
    if not row.get('kernel_log_available') or row.get('kernel_storage_errors') or row.get('dm_available'):
        raise RuntimeError('커널 로그/장치 경로 불일치')


def fs_delta(before,after):
    patterns={'gc_calls':r'GC calls:\s*(\d+)','gc_bg_calls':r'GC calls:\s*\d+\s*\(BG:\s*(\d+)\)',
        'cp_calls':r'CP calls:\s*(\d+)','move_attempt_blocks':r'Try to move\s+(\d+)\s+blocks',
        'reclaimed_segments_normal':r'Reclaimed segs\s*:\s*Normal\s*\((\d+)\)'}
    result={}
    for k,pattern in patterns.items():
        a,b=re.search(pattern,before),re.search(pattern,after)
        if not a or not b: raise RuntimeError('F2FS 계측 필드 누락: '+k)
        result[k]=int(b[1])-int(a[1])
        if result[k]<0: raise RuntimeError('F2FS 카운터 역행')
    return result


def provenance():
    code='''import json,hashlib,subprocess
from pathlib import Path
result={}
for name in ['kafka_python_result','dm-zns-base','Kafka-benchmark']:
 p=Path('/home/zns')/name
 def git(*args):
  r=subprocess.run(['git','-C',str(p),*args],capture_output=True)
  return r.stdout if r.returncode==0 else b''
 result[name]=dict(commit=git('rev-parse','HEAD').decode().strip(),
  status=git('status','--short').decode(),dirty_diff_sha256=hashlib.sha256(git('diff','HEAD','--binary')).hexdigest())
files=[Path('/home/zns/dm-zns-base/src/dm-zns-base.ko')]
files+=list(Path('/home/zns/Kafka-benchmark/build/libs').glob('*.jar'))
result['stack_artifacts_not_used_by_native_fio']={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}
print(json.dumps(result))'''
    return json.loads(read_call('guest','python3 -c '+shlex.quote(code),timeout=40).stdout)


def validate_completed(local,config):
    if json.loads((local/'plan.json').read_text())!=config:
        raise RuntimeError('재개 계획 또는 바이너리 불일치')
    done=json.loads((local/'completed-cells.json').read_text())
    if [r['cell'] for r in done]!=config['cells'][:len(done)]:
        raise RuntimeError('완료 결과가 계획의 연속 prefix가 아님')
    if len({r['boot_id'] for r in done})!=len(done):
        raise RuntimeError('완료 결과 boot_id 중복')
    for index,r in enumerate(done,1):
        d=local/f'cell-{index:02}'
        load=lambda name:json.loads((d/name).read_text())
        validate(r['cell'],load('manifest.json'),r['summary'],load('measurement.fio.json'),load('prepare.json'),load('exit-code.json'))
        if hashlib.sha256((d/'measurement.fio.json').read_bytes()).hexdigest()!=r['raw_sha256']:
            raise RuntimeError('완료 원본 hash 변경')
    return done


def check_unused_boot(host_cell,out):
    """초기화조차 시작하지 않은 해당 셀 VM만 재사용할 수 있다."""
    read_call('guest','test ! -e '+shlex.quote(out)+' && test ! -e '+shlex.quote(out+'-setup'))
    code='''import os,glob,csv
from pathlib import Path
cell=Path(HOST_CELL)
pids=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit(): continue
 try:
  if p.stat().st_uid!=os.getuid(): continue
  args=(p/'cmdline').read_bytes().split(b'\\0')
  if args and args[0]==b'/home/zns-team4/femu/src/build-femu/qemu-system-x86_64' and b'native-f2fs-team4' in args: pids.append(p)
 except (OSError,PermissionError): pass
assert len(pids)==1, '현재 native VM 식별 실패'
links=[]
for fd in (pids[0]/'fd').iterdir():
 try: links.append(os.readlink(fd))
 except OSError: pass
for name in ['cns.csv','zns.vZNSSD0.csv']:
 path=cell/name
 assert str(path) in links, '현재 VM의 통계 경로 불일치'
 with path.open() as f: rows=list(csv.DictReader(f))
 assert rows and int(rows[-1]['host_write_bytes'])==0, '현재 VM에 쓰기 흔적 존재'
print('UNUSED_BOOT_VERIFIED')'''.replace('HOST_CELL',repr(host_cell))
    read_call('host','python3 -c '+shlex.quote(code),timeout=30)


def collect(local,out,host_cell,cell,index):
    directory=local/f'cell-{index:02}'
    subprocess.run(['scp','-r',*common.GUEST[:-1],'zns@cns-team4:'+out,str(directory)],check=True,timeout=90)
    read=lambda name:json.loads((directory/name).read_text())
    summary=read('summary.json')
    if len(summary)!=1: raise RuntimeError('summary 수 불일치')
    r=summary[0]
    validate(cell,read('manifest.json'),r,read('measurement.fio.json'),read('prepare.json'),read('exit-code.json'))
    stats={}
    for name in ['cns.csv','zns.vZNSSD0.csv']:
        path=directory/name
        subprocess.run(['scp',*common.RELAY[:-3],'-P',common.RELAY[-2],
            'zns-team4@192.168.67.1:'+host_cell+'/'+name,str(path)],check=True,timeout=60)
        stats[name]=fio.femu_delta(path,r['measurement_start_us_approx'],r['end_us'])
        if not stats[name]['available']: raise RuntimeError('FEMU 측정 구간 통계 누락')
    stats['zns.vZNSSD0.csv']['interpretation_limit']='ZNS NAND 모델 한계; native F2FS GC/실제 NAND WA로 동일시하지 않음'
    r['femu']=stats
    r['f2fs_measurement_delta']=fs_delta((directory/'f2fs-measurement-start.txt').read_text(),(directory/'f2fs-after-measurement.txt').read_text())
    r['f2fs_warmup_measurement_delta']=fs_delta((directory/'f2fs-before-warmup.txt').read_text(),(directory/'f2fs-after-measurement.txt').read_text())
    common.save(directory/'summary-collected.json',[r])
    steal=r['os'].get('cpu_steal_pct')
    return dict(cell=cell,output=out,host_cell=host_cell,summary=r,
                raw_sha256=hashlib.sha256((directory/'measurement.fio.json').read_bytes()).hexdigest(),
                cpu_steal_pct=steal,host_interference_warning=steal is None or steal>1)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--name',default='native-fio-short5-gc1-20261017')
    p.add_argument('--run',action='store_true')
    p.add_argument('--resume',action='store_true',help='완료 원본 검증 후 미시작 조건부터 재개')
    a=p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+',a.name): p.error('실험 이름 오류')
    cells=plan(); local=ROOT/'results/native-zns'/a.name
    groot=GROOT+'/results/native-zns/'+a.name
    hroot=common.HOST_ROOT+'/results/'+a.name
    hashes={n:hashlib.sha256((ROOT/'scripts'/n).read_bytes()).hexdigest() for n in ['run-native-fio-cell.py','prepare-native-f2fs-user24g.sh']}
    config=dict(cells=cells,fresh_femu_per_cell=True,physical_zns_gib=32,user_gib=24,
        metadata_cns_gib=1,zone_mib=512,zone_count=64,gc_reserved_sections=8,mkfs_o=14.286,
        mount='mode=lfs,discard',requested_direct=True,effective_io='native zoned buffered fallback',
        femu_sha256=common.EXPECTED_FEMU_SHA,formatter_sha256=FMT_SHA,launcher_sha256=LAUNCHER_SHA,
        guest_scripts_sha256=hashes,fio_helper_sha256=FIO_SHA,
        interference_policy='steal>1% 경고 표기; 자동 제외·재실행하지 않음')
    print(json.dumps(config,ensure_ascii=False,indent=2),flush=True)
    if not a.run: return
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if a.resume:
        done=validate_completed(local,config)
        history=local/('resume-history-'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        history.mkdir()
        for name in ['status.json','exit-code.json','completed-cells.json']:
            if (local/name).exists(): shutil.copy2(local/name,history/name)
    else:
        local.mkdir(parents=True,exist_ok=False)
        common.save(local/'plan.json',config)
        done=[]
    try:
        common.active_guest_guard()
        check_hash('host',LAUNCHER,LAUNCHER_SHA); check_hash('host',common.FEMU,common.EXPECTED_FEMU_SHA)
        check_hash('guest',FMT,FMT_SHA); check_hash('guest',GROOT+'/src/fio_evaluation.py',FIO_SHA)
        launcher=read_call('host','cat '+LAUNCHER).stdout
        if a.resume and (local/'run-native-f2fs.sh.snapshot').read_text()!=launcher:
            raise RuntimeError('기존 실행기 snapshot 변경')
        if not a.resume: (local/'run-native-f2fs.sh.snapshot').write_text(launcher)
        # 첫 실행 전에 완료 진단을 보존한다. OS 디스크의 결과는 포맷하지 않는다.
        if not a.resume:
            subprocess.run(['scp','-r',*common.GUEST[:-1],
                'zns@cns-team4:'+GROOT+'/results/native-zns/fio-user24g-gc18-rand4k-20261016',
                str(local/'previous-gc-diagnostic')],check=True,timeout=90)
            guest_scp([ROOT/'scripts'/n for n in hashes],GROOT+'/scripts/')
        previous_boot=done[-1]['boot_id'] if done else None
        reuse=False
        if a.resume and len(done)<len(cells):
            next_host=hroot+f'/cell-{len(done)+1:02}'
            exists=common.call('host','test -d '+shlex.quote(next_host),check=False)
            if exists.returncode==0:
                check_unused_boot(next_host,groot+f'/cell-{len(done)+1:02}')
                reuse=True
            elif exists.returncode!=1: raise RuntimeError('재개 셀 존재 여부 확인 실패')
        if a.resume and (local/'exit-code.json').exists(): (local/'exit-code.json').unlink()
        for index,cell in enumerate(cells,1):
            if index<=len(done): continue
            status=dict(state='booting',index=index,total=len(cells),completed=len(done),cell=cell)
            common.save(local/'status.json',status)
            host_cell=hroot+f'/cell-{index:02}'; out=groot+f'/cell-{index:02}'
            if reuse:
                reuse=False
            else:
                shutdown()
                boot(host_cell,a.name,index)
            fresh_stats(host_cell)
            boot_id=read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if boot_id==previous_boot: raise RuntimeError('독립 VM boot_id 중복')
            previous_boot=boot_id
            for name,value in hashes.items(): check_hash('guest',GROOT+'/scripts/'+name,value)
            check_hash('guest',GROOT+'/src/fio_evaluation.py',FIO_SHA)
            check_hash('guest',FMT,FMT_SHA)
            common.call('guest','mkdir -p '+shlex.quote(groot))
            common.save(local/f'cell-{index:02}.provenance.json',provenance())
            setup=out+'-setup'
            log=common.call('guest',shlex.join(['bash',GROOT+'/scripts/prepare-native-f2fs-user24g.sh','--run',setup]),timeout=120).stdout
            (local/f'cell-{index:02}.setup.log').write_text(log)
            status.update(state='running',destination=out,boot_id=boot_id,host_statistics=host_cell)
            common.save(local/'status.json',status)
            print(f'[{index}/{len(cells)}] native 독립 fio: {cell}',flush=True)
            command=['python3','-u',GROOT+'/scripts/run-native-fio-cell.py','--output',out,
                     '--cell-json',json.dumps(cell,separators=(',',':')),'--run']
            run_cell(command,out,f'native-fio-cell-{index:02}',cell['file_gib']*600+cell['warmup']+cell['measure']+300)
            record=collect(local,out,host_cell,cell,index); record['boot_id']=boot_id
            subprocess.run(['scp','-r',*common.GUEST[:-1],'zns@cns-team4:'+setup,str(local/f'cell-{index:02}-setup')],check=True,timeout=30)
            done.append(record); common.save(local/'completed-cells.json',done)
            status.update(state='cell-completed',completed=len(done)); common.save(local/'status.json',status)
            guest_scp([local/'plan.json',local/'status.json',local/'completed-cells.json'],groot+'/')
        common.save(local/'status.json',dict(state='completed',completed=len(done),total=len(cells)))
        common.save(local/'exit-code.json',dict(exit_code=0))
        guest_scp([local/'status.json',local/'exit-code.json'],groot+'/')
    except BaseException as exc:
        common.save(local/'status.json',dict(state='failed',completed=len(done),total=len(cells),error=str(exc)))
        common.save(local/'exit-code.json',dict(exit_code=1))
        raise


if __name__=='__main__': main()

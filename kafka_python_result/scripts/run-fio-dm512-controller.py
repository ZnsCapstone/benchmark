#!/usr/bin/env python3
"""DM-ZNS+F2FS 512MiB zone 비교: short 독립5회, gc18 독립1회."""
import argparse
import csv
import datetime
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('native_common',Path(__file__).with_name('run-fio-native-controller.py'))
n=importlib.util.module_from_spec(spec); spec.loader.exec_module(n)
c=n.common
MODULE='/home/zns/dm-zns-base/src/dm-zns-base.ko'
MODULE_SHA='7170f69209d9cd6141bd617886cfeec10d629025d7fc7758269122b8214917d1'
LAUNCHER='/home/zns-team4/run-dm-zns-512m.sh'
ENV=['BENCH_STORAGE_BACKEND=dm-zns','BENCH_FILESYSTEMS=f2fs','BENCH_F2FS_DISCARD=0',
     'BENCH_EXT4_DISCARD=0','DM_GC_RESERVED_ZONES=8','DM_GC_LOW_WATERMARK=16',
     'DM_GC_HIGH_WATERMARK=20','DM_LOGICAL_CAPACITY_PERCENT=75','DM_GC_DIAG_BUDGET=0']


def boot(host_cell,session,launcher_sha):
    if c.call('host',"pgrep -a -u zns-team4 -f '[q]emu-system-x86_64'",check=False).returncode!=1:
        raise RuntimeError('다른 team4 VM 존재 또는 연결 오류')
    n.check_hash('host',c.FEMU,c.EXPECTED_FEMU_SHA); n.check_hash('host',LAUNCHER,launcher_sha)
    c.call('host','test ! -e '+shlex.quote(host_cell)+' && mkdir -p '+shlex.quote(host_cell))
    shell=shlex.join(['env','FEMU_ZNS_STATS_PREFIX='+host_cell+'/zns','bash',LAUNCHER])+' > '+shlex.quote(host_cell+'/console.log')+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(host_cell+'/exit-code.txt')
    c.call('host',shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]))
    for _ in range(60):
        try:
            r=c.call('guest','test "$(hostname)" = zns-guest && '
              'test "$(cat /sys/block/nvme0n1/size)" = 67108864 && '
              'test "$(cat /sys/block/nvme0n1/queue/zoned)" = host-managed && '
              'test "$(cat /sys/block/nvme0n1/queue/chunk_sectors)" = 1048576 && '
              'test "$(cat /sys/block/nvme0n1/queue/nr_zones)" = 64 && sudo -n true',timeout=15,check=False)
            if r.returncode==0: return
        except subprocess.TimeoutExpired: pass
        time.sleep(3)
    raise RuntimeError('DM512 guest 부팅 확인 실패; 강제 재시작하지 않음')


def check_result(cell,manifest,row,raw):
    expected=dict(backend='dm-zns',filesystem=cell['filesystem'],file_gib=cell['file_gib'],
      workloads=cell['workload'],bs=cell['bs'],qds='32',warmup=cell['warmup'],measure=cell['measure'])
    if any(manifest.get(k)!=v for k,v in expected.items()): raise RuntimeError('manifest 조건 불일치')
    if len(raw.get('jobs',[]))!=1 or raw['jobs'][0].get('error',0): raise RuntimeError('fio 오류 또는 결과 불완전')
    if raw['jobs'][0]['write']['runtime']<cell['measure']*1000*.95: raise RuntimeError('fio runtime 부족')
    if not row.get('kernel_log_available') or row.get('kernel_storage_errors') or not row.get('dm_available'):
        raise RuntimeError('kernel 오류 또는 DM 계측 누락')


def collect(local,out,host_cell,cell,index):
    d=local/f'cell-{index:02}'
    subprocess.run(['scp','-r',*c.GUEST[:-1],'zns@cns-team4:'+out,str(d)],check=True,timeout=90)
    load=lambda name:json.loads((d/name).read_text())
    rows=load('summary.json')
    if len(rows)!=1: raise RuntimeError('summary 수 오류')
    row=rows[0]; rawname=cell['workload']+'-qd32-r1.fio.json'
    check_result(cell,load('manifest.json'),row,load(rawname))
    code='''import json,subprocess
from pathlib import Path
p=Path('/sys/module/dm_zns_base/parameters')
r={k:int((p/k).read_text()) for k in ['gc_reserved_zones','gc_low_watermark','gc_high_watermark']}
r['logical_sectors']=int(subprocess.check_output(['sudo','-n','blockdev','--getsz','/dev/mapper/kafka-zns'],text=True))
r['zone_sectors']=int(Path('/sys/block/nvme0n1/queue/chunk_sectors').read_text())
r['nr_zones']=int(Path('/sys/block/nvme0n1/queue/nr_zones').read_text())
print(json.dumps(r))'''
    actual=json.loads(n.read_call('guest','python3 -c '+shlex.quote(code)).stdout)
    expected=dict(gc_reserved_zones=8,gc_low_watermark=16,gc_high_watermark=20,
                  logical_sectors=50331648,zone_sectors=1048576,nr_zones=64)
    if actual!=expected: raise RuntimeError('실제 DM 용량/GC 모듈 파라미터 불일치')
    c.save(d/'dm-environment-verified.json',actual)
    prep=load('prepare.json');size=cell['file_gib']*1024**3
    if len(prep.get('jobs',[]))!=1 or prep['jobs'][0].get('error',0) or any(prep['jobs'][0][x]['io_bytes']!=size for x in ['write','read']):
        raise RuntimeError('준비 CRC 검증 불완전')
    subprocess.run(['scp',*c.RELAY[:-3],'-P',c.RELAY[-2],
        'zns-team4@192.168.67.1:'+host_cell+'/zns.vZNSSD0.csv',str(d/'zns.csv')],check=True,timeout=60)
    row['femu']={'zns':n.fio.femu_delta(d/'zns.csv',row['measurement_start_us_approx'],row['end_us'])}
    if not row['femu']['zns']['available']: raise RuntimeError('FEMU 통계 누락')
    row['femu']['zns']['interpretation_limit']='ZNS 모델 NAND WA는 진단용; 실제 NAND WA로 확정하지 않음'
    c.save(d/'summary-collected.json',[row])
    steal=row['os'].get('cpu_steal_pct')
    return dict(cell=cell,output=out,host_cell=host_cell,summary=row,
        raw_sha256=hashlib.sha256((d/rawname).read_bytes()).hexdigest(),
        cpu_steal_pct=steal,host_interference_warning=steal is None or steal>1)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',action='store_true')
    p.add_argument('--filesystem',choices=['f2fs','ext4'],default='f2fs')
    p.add_argument('--resume',action='store_true',help='완료 원본을 보존하고 다음 조건부터 재개')
    p.add_argument('--restart-pending',action='store_true',help='미집계 회차를 보존하고 새 경로/VM에서 다시 측정')
    a=p.parse_args()
    if a.restart_pending and not a.resume: p.error('--restart-pending은 --resume과 함께 사용')
    name=f'dm512-{a.filesystem}-fio-short5-gc1-20261017'
    cells=[dict(cell,filesystem=a.filesystem) for cell in n.plan()]
    environment=[v for v in ENV if not v.startswith('BENCH_FILESYSTEMS=')]+['BENCH_FILESYSTEMS='+a.filesystem]
    local=ROOT/'results/zns-512m'/name; groot=c.GUEST_ROOT+'/results/zns-512m/'+name
    hroot=c.HOST_ROOT+'/results/'+name
    launcher_sha=hashlib.sha256((ROOT/'scripts/run-dm-zns-512m.sh').read_bytes()).hexdigest()
    config=dict(cells=cells,physical_gib=32,logical_gib=24,zone_mib=512,nr_zones=64,
        gc_reserve_low_high=[8,16,20],gc_reserve_low_high_gib=[4,8,10],discard=False,
        module_sha256=MODULE_SHA,femu_sha256=c.EXPECTED_FEMU_SHA,launcher_sha256=launcher_sha,
        fio_helper_sha256=n.FIO_SHA,fresh_femu_per_cell=True,
        effective_io=('DM 위 F2FS direct=1; native buffered fallback과 다름' if a.filesystem=='f2fs' else 'DM 위 EXT4 direct=1; native buffered fallback과 다름'))
    print(json.dumps(config,ensure_ascii=False,indent=2),flush=True)
    if not a.run: return
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if a.resume:
        if json.loads((local/'plan.json').read_text())!=config:
            raise RuntimeError('재개 계획·바이너리 불일치')
        done=json.loads((local/'completed-cells.json').read_text())
        if [r['cell'] for r in done]!=cells[:len(done)] or len({r['boot_id'] for r in done})!=len(done):
            raise RuntimeError('완료 prefix/독립 부팅 검증 실패')
        for index,r in enumerate(done,1):
            d=local/f'cell-{index:02}';read=lambda x:json.loads((d/x).read_text())
            rawname=r['cell']['workload']+'-qd32-r1.fio.json'
            check_result(r['cell'],read('manifest.json'),r['summary'],read(rawname))
            if hashlib.sha256((d/rawname).read_bytes()).hexdigest()!=r['raw_sha256']:
                raise RuntimeError('완료 원본 hash 변경')
        history=local/('resume-history-'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        history.mkdir()
        for name in ['status.json','exit-code.json','completed-cells.json']:
            if (local/name).exists(): shutil.copy2(local/name,history/name)
        name=f'dm512-{a.filesystem}-fio-short5-gc1-20261017'
        retry_index=len(done)+1
        retry_suffix='-retry-'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        if a.restart_pending and (local/f'cell-{retry_index:02}').exists():
            shutil.move(str(local/f'cell-{retry_index:02}'),str(history/f'unverified-cell-{retry_index:02}'))
    else:
        local.mkdir(parents=True,exist_ok=False);c.save(local/'plan.json',config);done=[]
    try:
        guest_off=False
        ready=c.call('guest','test "$(hostname)" = zns-guest',timeout=15,check=False)
        if ready.returncode:
            if not (a.resume and a.restart_pending) or c.call('host',"pgrep -a -u zns-team4 -f '[q]emu-system-x86_64'",check=False).returncode!=1:
                raise RuntimeError('게스트 연결 실패; 종료된 재개 VM임을 확인할 수 없음')
            guest_off=True
        if not guest_off:
            c.active_guest_guard()
            previous=json.loads(n.read_call('guest','cat '+c.GUEST_ROOT+'/results/native-zns/native-fio-short5-gc1-20261017/status.json').stdout)
            n.check_hash('guest',MODULE,MODULE_SHA);n.check_hash('guest',c.GUEST_ROOT+'/src/fio_evaluation.py',n.FIO_SHA)
        else:
            previous=json.loads((ROOT/'results/native-zns/native-fio-short5-gc1-20261017/status.json').read_text())
        if previous.get('state')!='completed': raise RuntimeError('Native fio suite가 미완료')
        n.check_hash('host',LAUNCHER,launcher_sha)
        launcher=n.read_call('host','cat '+LAUNCHER).stdout
        if a.resume and (local/'launcher.snapshot.sh').read_text()!=launcher:
            raise RuntimeError('기존 실행기 snapshot 변경')
        if not a.resume: (local/'launcher.snapshot.sh').write_text(launcher)
        if a.resume and not a.restart_pending and len(done)<18:
            index=len(done)+1;out=groot+f'/cell-{index:02}';host_cell=hroot+f'/cell-{index:02}'
            exists=c.call('guest','test -e '+shlex.quote(out),check=False)
            if exists.returncode==0:
                exit_code=json.loads(n.read_call('guest','cat '+shlex.quote(out+'/wrapper-exit.json')).stdout)
                if exit_code.get('exit_code')!=0:
                    raise RuntimeError('미완료/실패 셀은 자동 초기화하지 않음')
                if (local/f'cell-{index:02}').exists():
                    raise RuntimeError('부분 수집 디렉토리가 존재하므로 자동 덮어쓰기 거절')
                # 현재 VM이 해당 회차 통계 파일을 열고 있는지 확인 후 boot_id를 기록한다.
                code='''import os
from pathlib import Path
expected=EXPECTED
found=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit(): continue
 try:
  if p.stat().st_uid!=os.getuid(): continue
  args=(p/'cmdline').read_bytes().split(b'\\0')
  if args[0]!=b'/home/zns-team4/femu/src/build-femu/qemu-system-x86_64' or b'dm-zns-512m-team4' not in args: continue
  links=[]
  for fd in (p/'fd').iterdir():
   try: links.append(os.readlink(fd))
   except OSError: pass
  if expected in links: found.append(p.name)
 except (OSError,PermissionError): pass
assert len(found)==1, '현재 VM과 회차 통계 경로 불일치'
print('VERIFIED')'''.replace('EXPECTED',repr(host_cell+'/zns.vZNSSD0.csv'))
                n.read_call('host','python3 -c '+shlex.quote(code))
                record=collect(local,out,host_cell,cells[index-1],index)
                record['boot_id']=n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
                if record['boot_id'] in {r['boot_id'] for r in done}: raise RuntimeError('복구 셀 boot_id 중복')
                record['recovered_after_ssh_disconnect']=True
                done.append(record);c.save(local/'completed-cells.json',done)
                print(f'[{index}/18] 완료 원본 회수·검증; 재실행하지 않음',flush=True)
            elif exists.returncode!=1: raise RuntimeError('재개할 원본 존재 여부 확인 실패')
        previous_boot=done[-1]['boot_id'] if done else None
        if a.resume and (local/'exit-code.json').exists(): (local/'exit-code.json').unlink()
        for index,cell in enumerate(cells,1):
            if index<=len(done): continue
            status=dict(state='booting',index=index,total=18,completed=len(done),cell=cell)
            c.save(local/'status.json',status)
            if not guest_off: n.shutdown()
            host_cell=hroot+f'/cell-{index:02}';out=groot+f'/cell-{index:02}'
            if a.restart_pending and index==retry_index:
                host_cell+=retry_suffix;out+=retry_suffix
            boot(host_cell,name+f'-{index:02}'+(retry_suffix if a.restart_pending and index==retry_index else ''),launcher_sha)
            guest_off=False
            initial=list(csv.DictReader(n.read_call('host','head -2 '+shlex.quote(host_cell+'/zns.vZNSSD0.csv')).stdout.splitlines()))
            if len(initial)!=1 or int(initial[0]['host_write_bytes']): raise RuntimeError('초기 ZNS 쓰기 counter가 0이 아님')
            boot_id=n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if previous_boot==boot_id: raise RuntimeError('boot_id 중복')
            previous_boot=boot_id
            n.check_hash('guest',MODULE,MODULE_SHA);n.check_hash('guest',c.GUEST_ROOT+'/src/fio_evaluation.py',n.FIO_SHA)
            c.save(local/f'cell-{index:02}.provenance.json',n.provenance())
            c.call('guest','mkdir -p '+shlex.quote(groot))
            command=['env',*environment,'python3','-u',c.GUEST_ROOT+'/src/fio_evaluation.py',
                '--prepare','--backend','dm-zns','--module-path',MODULE,'--filesystem',a.filesystem,
                '--workloads',cell['workload'],'--bs',cell['bs'],'--qds','32','--rounds','1',
                '--file-gib',str(cell['file_gib']),'--warmup',str(cell['warmup']),
                '--measure',str(cell['measure']),'--output-dir',out,'--run']
            shell=shlex.join(command)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; mkdir -p '+shlex.quote(out)+'; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'/wrapper-exit.json')
            session=f'dm512-fio-cell-{index:02}'
            c.call('guest',shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]))
            status.update(state='running',destination=out,boot_id=boot_id)
            c.save(local/'status.json',status);print(f'[{index}/18] DM512 {cell}',flush=True)
            deadline=time.monotonic()+cell['file_gib']*600+cell['warmup']+cell['measure']+600
            while time.monotonic()<deadline:
                time.sleep(30)
                try:
                    r=c.call('guest','if test -s '+shlex.quote(out+'/wrapper-exit.json')+'; then cat '+shlex.quote(out+'/wrapper-exit.json')+'; elif tmux has-session -t '+session+' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi',timeout=20,check=False)
                except subprocess.TimeoutExpired: continue
                if r.returncode==255: continue
                if r.returncode: raise RuntimeError('회차 상태 확인 실패')
                if r.stdout.strip()=='RUNNING': continue
                if r.stdout.strip()=='STOPPED' or json.loads(r.stdout).get('exit_code')!=0:
                    raise RuntimeError('DM512 회차 실패; VM/로그 보존, 후속 실행 중단')
                break
            else: raise RuntimeError('회차 전체 제한 초과; 현재 VM 보존')
            record=collect(local,out,host_cell,cell,index);record['boot_id']=boot_id
            done.append(record);c.save(local/'completed-cells.json',done)
            status.update(state='cell-completed',completed=len(done));c.save(local/'status.json',status)
            n.guest_scp([local/'plan.json',local/'status.json',local/'completed-cells.json'],groot+'/')
        c.save(local/'status.json',dict(state='completed',completed=18,total=18));c.save(local/'exit-code.json',dict(exit_code=0))
        n.guest_scp([local/'status.json',local/'exit-code.json'],groot+'/')
    except BaseException as exc:
        c.save(local/'status.json',dict(state='failed',completed=len(done),total=18,error=str(exc)))
        c.save(local/'exit-code.json',dict(exit_code=1));raise


if __name__=='__main__': main()

#!/usr/bin/env python3
"""team4 fio 1.1v: WAL0 고정 DM+EXT4/F2FS 비교. 1-1 성능 / 1-2 동일 쓰기량 비용."""
import argparse
import csv
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
import subprocess
import shutil
import time

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('wal_common',Path(__file__).with_name('run-fio-wal-ab-controller.py'))
w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)
c,n,d=w.c,w.n,w.d
spec=importlib.util.spec_from_file_location('final_plan',Path(__file__).with_name('plan-fio-final.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
REL='results/final/1.1v'
BUILD='/home/zns/fio-1.1v-20261019'
RUNNER=BUILD+'/runner/src/fio_evaluation.py'
MODULE='/home/zns/wal-diag-20261019/module-src/dm-zns-base.ko'
MODULE_SHA='5dfd8d3c6b487459c584d1a9dc4349f2a803b97e4f3ecc0f740fe6f2da8841b9'

def plan():
    out=[]
    for phase in ['1-1','1-2']:
        for cell in p.cells(phase,2 if phase=='1-1' else 1):
            filesystems=('ext4','f2fs') if cell['repeat'] % 2 else ('f2fs','ext4')
            for filesystem in filesystems:
                out.append(dict(cell,index=len(out)+1,delay_ms=0,
                                filesystem=filesystem,backend='dm-zns'))
    return out

def validate_plan(cells):
    if not cells or any(c['delay_ms']!=0 or c['backend']!='dm-zns' for c in cells):
        raise RuntimeError('최종 FS 비교는 모든 회차 WAL0 고정이어야 함')
    for phase in ['1-1','1-2']:
        for fs in ['ext4','f2fs']:
            rows=[c for c in cells if c['experiment']==phase and c['filesystem']==fs]
            expected=6 if phase=='1-1' else 3
            if len(rows)!=expected or {(c['workload'],c['bs']) for c in rows}!={('seq-write','4k'),('rand-write','4k'),('seq-write','1M')}:
                raise RuntimeError('FS별 실험 조건 또는 반복 구성 불일치')

def relative_output(cell):
    return f"{cell['experiment']}/dm-zns/{cell['filesystem']}/wal-{cell['delay_ms']}ms/{cell['workload']}-{cell['bs']}/r{cell['repeat']}"

def validate(cell,manifest,row,raw,prep):
    if cell['delay_ms']!=0:raise RuntimeError('WAL0 고정 위반')
    expected=dict(backend='dm-zns',filesystem=cell['filesystem'],experiment_id=cell['experiment'],
                  file_gib=cell['file_gib'],workloads=cell['workload'],bs=cell['bs'],
                  qds='32',rounds=1,warmup=cell['warmup'],wal_delay_ms=cell['delay_ms'],
                  randseed=cell['randseed'])
    if any(manifest.get(k)!=v for k,v in expected.items()):raise RuntimeError('원본 manifest 조건 불일치')
    for payload in [prep,raw]:n.fio.parse_fio(payload)
    if any(prep['jobs'][0].get(k,{}).get('io_bytes')!=cell['file_gib']*2**30 for k in ['read','write']):
        raise RuntimeError('준비 CRC 검증 바이트 불일치')
    if not row.get('kernel_log_available') or row.get('kernel_storage_errors') or not row.get('dm_available'):
        raise RuntimeError('커널 오류/수집 실패 또는 DM 계측 누락')
    if row.get('experiment_id')!=cell['experiment']:raise RuntimeError('결과 실험 구분 불일치')
    w.validate(row,manifest,cell)
    if cell['experiment']=='1-1':
        if manifest.get('termination')!='fixed-time' or manifest.get('measure')!=cell['measure']:
            raise RuntimeError('1-1 고정 시간 설정 불일치')
        if raw['jobs'][0]['write']['runtime'] < cell['measure']*1000*.95:
            raise RuntimeError('1-1 측정 시간 미달')
    else:
        if manifest.get('termination')!='fixed-write-bytes' or manifest.get('total_write_gib')!=cell['total_write_gib']:
            raise RuntimeError('1-2 고정 쓰기량 설정 불일치')
        n.fio.validate_fixed_write(raw,cell['total_write_gib']*2**30)
        if not row.get('fixed_work_complete') or not row.get('fixed_work_costs'):
            raise RuntimeError('1-2 완료/비용 결과 누락')

def validate_mount(mount,filesystem):
    fields=mount.split()
    if len(fields)!=3 or fields[0]!='/dev/mapper/kafka-zns' or fields[1]!=filesystem:
        raise RuntimeError('실제 파일시스템/장치 불일치')
    options=set(fields[2].split(','))
    if 'discard' in options or (filesystem=='f2fs' and 'nodiscard' not in options):
        raise RuntimeError('discard OFF 조건 위반')

def environment(delay,filesystem):
    code='''import json,subprocess
from pathlib import Path
p=Path('/sys/module/dm_zns_base/parameters')
r={k:(p/k).read_text().strip() for k in ['metrics_enabled','wal_timing_enabled','wal_batch_delay_ms','gc_reserved_zones','gc_low_watermark','gc_high_watermark']}
r['logical_sectors']=subprocess.check_output(['sudo','-n','blockdev','--getsz','/dev/mapper/kafka-zns'],text=True).strip()
for k in ['chunk_sectors','nr_zones']:
 r[k]=Path('/sys/block/nvme0n1/queue/'+k).read_text().strip()
r['mount']=subprocess.check_output(['findmnt','-n','-o','SOURCE,FSTYPE,OPTIONS','--target','/result/kafka-logs'],text=True).strip()
print(json.dumps(r))'''
    value=json.loads(n.read_call('guest','python3 -c '+shlex.quote(code)).stdout)
    expected=dict(metrics_enabled='Y',wal_timing_enabled='Y',wal_batch_delay_ms=str(delay),
                  gc_reserved_zones='8',gc_low_watermark='16',gc_high_watermark='20',
                  logical_sectors='50331648',chunk_sectors='1048576',nr_zones='64')
    if delay!=0 or any(value.get(k)!=v for k,v in expected.items()):
        raise RuntimeError('실제 모듈/장치/mount 조건 불일치')
    validate_mount(value['mount'],filesystem)
    return value

def collect(local,out,host,cell):
    destination=local/relative_output(cell)
    destination.parent.mkdir(parents=True,exist_ok=True)
    subprocess.run(['scp','-r',*c.GUEST[:-1],'zns@cns-team4:'+out,str(destination)],check=True,timeout=120)
    load=lambda name:json.loads((destination/name).read_text())
    rows=load('summary.json')
    if len(rows)!=1:raise RuntimeError('단일 회차 summary 수 불일치')
    row=rows[0];rawname=cell['workload']+'-qd32-r1.fio.json'
    validate(cell,load('manifest.json'),row,load(rawname),load('prepare.json'))
    # DM의 fail-closed 오류도 FS/NVMe 오류와 별도로 선별한다.
    kernel=(destination/(cell['workload']+'-qd32-r1.kernel.log')).read_text()
    if re.search(r'insufficient relocation workspace|foreground allocation exhaust|metadata.*fail.closed|WAL append failed',kernel,re.I):
        raise RuntimeError('DM 진행성/저장 오류: 원본 보존')
    c.save(destination/'environment-verified.json',environment(cell['delay_ms'],cell['filesystem']))
    if cell['experiment']=='1-2' and not (destination/(cell['workload']+'-qd32-r1.boundaries.json')).is_file():
        raise RuntimeError('1-2 동기 계측 경계 누락')
    subprocess.run(['scp',*c.RELAY[:-3],'-P',c.RELAY[-2],
                    'zns-team4@192.168.67.1:'+host+'/zns.vZNSSD0.csv',str(destination/'femu-zns.csv')],check=True,timeout=90)
    subprocess.run(['scp',*c.GUEST[:-1],'zns@cns-team4:'+out+'.launch.log',str(destination/'launch.log')],check=True,timeout=60)
    row['femu']={'zns':n.fio.femu_delta(destination/'femu-zns.csv',row['measurement_start_us_approx'],row['end_us'])}
    if not row['femu']['zns']['available']:raise RuntimeError('FEMU 통계 누락')
    row['femu']['zns']['interpretation_limit']='모델 NAND WA는 진단용. 실제 NAND WA로 확정하지 않음'
    c.save(destination/'summary-collected.json',[row])
    steal=row.get('os',{}).get('cpu_steal_pct')
    return dict(cell=cell,output=out,host_cell=host,summary=row,module_sha256=MODULE_SHA,
                raw_sha256=hashlib.sha256((destination/rawname).read_bytes()).hexdigest(),
                cpu_steal_pct=steal,host_interference_warning=steal is None or steal>1)

def validate_resume(local,cells):
    done=json.loads((local/'completed-cells.json').read_text())
    if [r['cell'] for r in done]!=cells[:len(done)] or len({r['boot_id'] for r in done})!=len(done):
        raise RuntimeError('완료 prefix 또는 독립 boot_id 불일치')
    for record in done:
        folder=local/relative_output(record['cell'])
        load=lambda name:json.loads((folder/name).read_text())
        rawname=record['cell']['workload']+'-qd32-r1.fio.json'
        validate(record['cell'],load('manifest.json'),record['summary'],load(rawname),load('prepare.json'))
        if hashlib.sha256((folder/rawname).read_bytes()).hexdigest()!=record['raw_sha256']:
            raise RuntimeError('완료 원본 SHA 변경')
    return done

def shutdown_with_grace():
    # 측정 종료 뒤의 정리만 최대180초 허용. fio 측정 시간/공간 기준은 바꾸지 않는다.
    c.active_guest_guard()
    c.call('guest','set -e; if mountpoint -q /result/kafka-logs; then sudo -n umount /result/kafka-logs; fi; sudo -n sync',timeout=180)
    n.shutdown()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true')
    parser.add_argument('--resume',action='store_true',help='완료 원본/계획을 검증하고 다음 조건부터 재개')
    args=parser.parse_args()
    cells=plan();validate_plan(cells);print(json.dumps(cells,ensure_ascii=False,indent=2),flush=True)
    if not args.run:return
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    local=ROOT/REL;local.mkdir(parents=True,exist_ok=True)
    if (local/'plan.json').exists() and not args.resume:raise RuntimeError('1.1v 기존 계획이 존재함: 덮어쓰기 거절')
    groot=c.GUEST_ROOT+'/'+REL;hroot=c.HOST_ROOT+'/'+REL
    done=validate_resume(local,cells) if args.resume else []
    if args.resume:
        history=local/('resume-history-'+time.strftime('%Y%m%d_%H%M%S'));history.mkdir()
        for name in ['status.json','exit-code.json','completed-cells.json','plan.json']:
            if (local/name).exists():shutil.copy2(local/name,history/name)
    def status(state,**kwargs):c.save(local/'status.json',dict(state=state,completed=len(done),total=len(cells),**kwargs))
    try:
        status('preflight')
        c.active_guest_guard()
        n.check_hash('guest',MODULE,MODULE_SHA)
        runner_source=local/'source-snapshot/runner-src/fio_evaluation.py' if args.resume else ROOT/'src/fio_evaluation.py'
        setup_source=local/'source-snapshot/runner-src/system_setup.py' if args.resume else ROOT/'src/system_setup.py'
        runner_sha=hashlib.sha256(runner_source.read_bytes()).hexdigest()
        setup_sha=hashlib.sha256(setup_source.read_bytes()).hexdigest()
        n.check_hash('guest',RUNNER,runner_sha);n.check_hash('guest',BUILD+'/runner/src/system_setup.py',setup_sha)
        module_source=local/'source-snapshot/module/dm-zns-base-main.c' if args.resume else globals().get('LOCAL_MODULE_SOURCE',ROOT.parent/'dm-zns-base/src/dm-zns-base-main.c')
        source_sha=hashlib.sha256(module_source.read_bytes()).hexdigest()
        n.check_hash('guest',str(Path(MODULE).parent/'dm-zns-base-main.c'),source_sha)
        launcher_sha=hashlib.sha256((ROOT/'scripts/run-dm-zns-512m.sh').read_bytes()).hexdigest()
        config=dict(version='1.1v',purpose='WAL0 고정 EXT4/F2FS 비교',wal_delay_ms=0,
                    filesystems=['ext4','f2fs'],cells=cells,module_path=MODULE,module_sha256=MODULE_SHA,
                    runner_path=RUNNER,runner_sha256=runner_sha,setup_sha256=setup_sha,
                    module_source_sha256=source_sha,
                    launcher_sha256=launcher_sha,femu_sha256=c.EXPECTED_FEMU_SHA,
                    physical_gib=32,logical_gib=24,zone_mib=512,gc_reserve_low_high=[8,16,20],
                    randseed=20261019,fresh_femu_per_cell=True,source_build=BUILD,
                    interpretation={'1-1':'고정 QD32/단일 작업 처리 성능; 절대 최대 성능 아님',
                                    '1-2':'동일54GiB 작업의 완료 시간/GC/WA'})
        if args.resume:
            if json.loads((local/'plan.json').read_text())!=config:raise RuntimeError('기존 계획/소스/바이너리 불일치')
            if (local/'exit-code.json').exists():(local/'exit-code.json').unlink()
        else:
            c.save(local/'plan.json',config)
            c.call('guest','test ! -e '+shlex.quote(groot+'/plan.json')+' && mkdir -p '+shlex.quote(groot))
            snap=local/'source-snapshot';snap.mkdir()
            subprocess.run(['scp','-r',*c.GUEST[:-1],'zns@cns-team4:'+BUILD+'/runner/src',str(snap/'runner-src')],check=True,timeout=90)
            (snap/'module').mkdir()
            for name in ['dm-zns-base-main.c','skiplist.c','skiplist.h','Makefile','dm-zns-base.ko']:
                subprocess.run(['scp',*c.GUEST[:-1],'zns@cns-team4:'+str(Path(MODULE).parent/name),str(snap/'module'/name)],check=True,timeout=45)
            n.guest_scp([local/'plan.json'],groot+'/')
        for cell in cells:
            if cell['index']<=len(done):continue
            index=cell['index'];status('booting',index=index,cell=cell)
            out=groot+'/'+relative_output(cell);host=hroot+'/'+relative_output(cell)
            if args.resume:
                for target,path in [('guest',out),('host',host)]:
                    check=c.call(target,'test ! -e '+shlex.quote(path),check=False)
                    if check.returncode:raise RuntimeError('미집계 경로가 이미 존재함: 자동 덮어쓰기 거절 '+path)
            shutdown_with_grace()
            d.boot(host,f'fio-v11-fs0-{index:02}',launcher_sha)
            initial=list(csv.DictReader(n.read_call('host','head -2 '+shlex.quote(host+'/zns.vZNSSD0.csv')).stdout.splitlines()))
            if len(initial)!=1 or int(initial[0]['host_write_bytes']):raise RuntimeError('FEMU 초기 쓰기 counter가 0이 아님')
            boot=n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if boot in {r['boot_id'] for r in done}:raise RuntimeError('독립 boot_id 중복')
            n.check_hash('guest',MODULE,MODULE_SHA);n.check_hash('guest',RUNNER,runner_sha)
            n.check_hash('guest',BUILD+'/runner/src/system_setup.py',setup_sha)
            c.save(local/f'case-{index:02}.provenance.json',n.provenance())
            cmd=p.command(cell,'dm-zns',cell['filesystem'],groot,MODULE,cell['delay_ms'])
            cmd[cmd.index('src/fio_evaluation.py')]=RUNNER
            cmd[cmd.index('--output-dir')+1]=out
            shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; mkdir -p '+shlex.quote(out)+'; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'/wrapper-exit.json')
            session=f'fio-v11-fs0-cell-{index:02}'
            c.call('guest','mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]))
            status('running',index=index,cell=cell,destination=out,boot_id=boot)
            print(f'[{index}/{len(cells)}] {cell}',flush=True)
            limit=cell['file_gib']*600+(cell['warmup']+cell['measure'] if cell['experiment']=='1-1' else 10800)+900
            deadline=time.monotonic()+limit
            while time.monotonic()<deadline:
                time.sleep(30)
                try:r=c.call('guest','if test -s '+shlex.quote(out+'/wrapper-exit.json')+'; then cat '+shlex.quote(out+'/wrapper-exit.json')+'; elif tmux has-session -t '+session+' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi',timeout=20,check=False)
                except subprocess.TimeoutExpired:continue
                if r.returncode==255:continue
                if r.returncode:raise RuntimeError('회차 상태 조회 실패')
                if r.stdout.strip()=='RUNNING':continue
                if r.stdout.strip()=='STOPPED' or json.loads(r.stdout).get('exit_code')!=0:
                    raise RuntimeError('회차 실패: VM/원본 보존, 후속 실행 중단')
                break
            else:raise RuntimeError('회차 전체 제한 초과: 실행 보존')
            record=collect(local,out,host,cell);record['boot_id']=boot
            done.append(record);c.save(local/'completed-cells.json',done)
            status('cell-completed',index=index,cell=cell)
            n.guest_scp([local/'status.json',local/'completed-cells.json'],groot+'/')
        status('completed');c.save(local/'exit-code.json',dict(exit_code=0))
        n.guest_scp([local/'status.json',local/'exit-code.json'],groot+'/')
    except BaseException as exc:
        status('failed',error=str(exc));c.save(local/'exit-code.json',dict(exit_code=1));raise

if __name__=='__main__':main()

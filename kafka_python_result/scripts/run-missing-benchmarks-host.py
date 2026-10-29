"""team4 미확보 평가 대기열. 실패 원본을 보존하고 다음 독립 VM으로 진행한다."""
import argparse
import csv
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import re
import time

HERE=Path(__file__).resolve().parent
RUN='missing-results-20261028'
ROOT=Path('/home/zns-team4/results/final')/RUN
BUILD='/home/zns/'+RUN
BIN=Path('/home/zns-team4/femu/src/build-femu/qemu-system-x86_64')
IMAGE='/home/zns-team4/zns-team4.qcow2'
MODULES={
 'final':('/home/zns/fio-cns-matched-5c0cdbd-20261022/module-src/dm-zns-base.ko','41044c3ce7f15658c29bddc7c7e0f0e05d3eb852f5e996b2c06d21f6d037d02e'),
 'legacy-test2':('/home/zns/wal-stage-diag-20261020/module-src/dm-zns-base.ko','383f2b860f628c33755c28dac7da8128396e83861806e316332db291c72f5bd6')}

def load(name,path):
    s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

m=load('base',HERE/'controller-base.py');m.CONTROL=ROOT
planmod=load('plan',HERE/'plan-missing-benchmarks.py')
META=json.loads((HERE/'input-manifest.json').read_text())


class IdentityError(RuntimeError):pass
class UserStop(RuntimeError):pass


def qemu():
    found=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try:
            if p.stat().st_uid!=os.getuid():continue
            args=[v.decode(errors='replace') for v in (p/'cmdline').read_bytes().split(b'\0') if v]
            if not args or not Path(args[0]).name.startswith('qemu-system-'):continue
            name=args[args.index('-name')+1] if '-name' in args else ''
            drives=[args[i+1] for i,v in enumerate(args[:-1]) if v=='-drive']
            owned=json.loads((ROOT/'owned-vm.json').read_text()) if (ROOT/'owned-vm.json').exists() else {}
            if (Path(os.readlink(p/'exe')).resolve()!=BIN.resolve() or name not in ['missing-dm-team4','missing-native-team4']
                or not any('file='+IMAGE+',' in v for v in drives) or int(p.name)!=owned.get('pid')):
                raise IdentityError('다른 team4 VM 또는 신원 불일치: 종료하지 않음')
            found.append(int(p.name))
        except (FileNotFoundError,ProcessLookupError):continue
    if len(found)>1:raise IdentityError('복수 VM: 중단')
    return found


def snapshot_kind(c):
    return 'fio-input' if c['kind'].startswith('fio') else ('prefill-input' if c['kind']=='prefill' else 'ko-input')


def cleanup():
    if not qemu():return
    command="""from pathlib import Path
import os,signal
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid():continue
 try:
  a=[x.decode(errors='replace') for x in (p/'cmdline').read_bytes().split(b'\\0') if x]
  if p.stat().st_uid==os.getuid() and a and Path(a[0]).name in ['java','python3','fio'] and any(RUN in x for x in a[1:]):os.kill(int(p.name),signal.SIGTERM)
 except (FileNotFoundError,ProcessLookupError):pass
""".replace('RUN',repr(RUN))
    try:
        m.guest('python3 -c '+shlex.quote(command),timeout=20,check=False)
        m.guest('sudo -n sync',timeout=30,check=False)
        m.guest('sudo -n poweroff',timeout=15,check=False)
    except Exception as e:m.save(ROOT/'cleanup-last-warning.json',dict(error=str(e)))
    for _ in range(15):
        if not qemu():return
        time.sleep(2)
    for pid in qemu():os.kill(pid,signal.SIGTERM)
    for _ in range(30):
        if not qemu():return
        time.sleep(1)
    raise IdentityError('검증된 VM도 종료되지 않음: 후속 초기화 중단')


def boot(c,host):
    cleanup()
    if hashlib.sha256(BIN.read_bytes()).hexdigest()!=META['femu_sha256']:raise IdentityError('FEMU 변경')
    host.mkdir(parents=True,exist_ok=False)
    zns='femu,devsz_mb=32768,namespaces=1,zns_num_ch=8,zns_num_lun=4,zns_num_plane=2,zns_num_blk=128,zns_flash_type=4,femu_mode=3'
    meta='femu,id=native-meta,serial=TEAM4-NATIVE-META,devsz_mb=1024,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=64,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95'
    native=c['backend']=='native'
    cmd=['env','-u','FEMU_MATCHED_ARRAY','-u','FEMU_BIN',
         'FEMU_ZNS_STATS_PREFIX='+str(host/'zns'),'FEMU_STATS_FILE='+str(host/'cns.csv'),str(BIN),
         '-name','missing-native-team4' if native else 'missing-dm-team4','-enable-kvm','-cpu','host','-smp','8','-m','16G',
         '-device','virtio-scsi-pci,id=scsi0','-device','scsi-hd,drive=hd0',
         '-drive',f'file={IMAGE},if=none,aio=native,cache=none,format=qcow2,id=hd0']
    if native:cmd+=['-device',meta]
    cmd+=['-device',zns,'-netdev','user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22',
          '-device','virtio-net-pci,netdev=net0','-display','none','-monitor','none','-serial','file:'+str(host/'serial.log')]
    m.save(host/'command.json',cmd)
    # shell PID가 exec 후 실제 QEMU PID가 된다. 모호한 pgrep 문자열 조회를 사용하지 않는다.
    shell='echo $$ > '+shlex.quote(str(host/'pid'))+'; exec '+shlex.join(cmd)+' > '+shlex.quote(str(host/'qemu.log'))+' 2>&1'
    m.run(['tmux','new-session','-d','-s',RUN+f"-vm-{c['index']:02}",'bash','-lc',shell])
    for _ in range(100):
        if (host/'pid').exists():break
        time.sleep(.1)
    else:raise RuntimeError('VM PID 기록 실패')
    m.save(ROOT/'owned-vm.json',dict(pid=int((host/'pid').read_text()),cell=c))
    for _ in range(80):
        try:
            if m.guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=8,check=False).returncode==0:break
        except Exception:pass
        time.sleep(2)
    else:raise RuntimeError('guest 부팅 실패')
    if not qemu():raise RuntimeError('FEMU 부팅 중 종료')
    expected=('none','2097152','host-managed','67108864') if native else ('host-managed','67108864')
    code="from pathlib import Path; x=[]\nfor d in "+repr(['nvme0n1','nvme1n1'] if native else ['nvme0n1'])+":\n x.extend([Path('/sys/block',d,'queue/zoned').read_text().strip(),Path('/sys/block',d,'size').read_text().strip()])\nassert tuple(x)=="+repr(expected)
    m.guest('python3 -c '+shlex.quote(code),timeout=20)


def deploy(c):
    kind=snapshot_kind(c)
    for folder in [kind,'hooks']:
        dest=BUILD+'/'+folder
        if m.guest('test -d '+dest,check=False).returncode:
            m.guest('mkdir -p '+BUILD)
            m.copy_to(HERE/folder,BUILD+'/')
    expected={BUILD+'/'+name:sha for name,sha in META['files'].items() if name.startswith(kind+'/') or name.startswith('hooks/')}
    if c['backend']=='dm-zns':expected[MODULES[c['module']][0]]=MODULES[c['module']][1]
    code='from pathlib import Path; import hashlib; expected='+repr(expected)+'; assert all(hashlib.sha256(Path(k).read_bytes()).hexdigest()==v for k,v in expected.items()), "input SHA mismatch"'
    m.guest('python3 -c '+shlex.quote(code),timeout=40)
    if c['backend']=='native':
        setup='/home/zns/kafka_python_result/results/native-zns/'+RUN+f"-setup-{c['index']:02}"
        m.guest(shlex.join(['bash',BUILD+'/hooks/prepare-native.sh','--run',setup]),timeout=300)


def command(c,out):
    folder=BUILD+'/'+snapshot_kind(c);module=MODULES[c['module']][0]
    env=['env','DM_ZNS_MODULE_PATH='+module,'BENCH_JAVA_JAR='+folder+'/kafka-benchmark-1.0.jar',
         'BENCH_DM_METRICS='+('1' if c['backend']=='dm-zns' else '0'),
         'BENCH_FILESYSTEMS='+c['filesystem'],'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0',
         'DM_GC_RESERVED_ZONES=8','DM_GC_LOW_WATERMARK=16','DM_GC_HIGH_WATERMARK=20',
         'DM_LOGICAL_CAPACITY_PERCENT=75','BENCH_WAL_DELAY_MS=0',
         'LOG_DIR='+out+'/broker-logs',
         'JAVA_TOOL_OPTIONS=-Dbenchmark.explicitPartitions=8']
    if c['kind'].startswith('fio'):
        native=c['backend']=='native'
        cmd=env+['python3',folder+'/src/fio_evaluation.py','--backend','native-f2fs-zns' if native else 'dm-zns',
            '--filesystem',c['filesystem'],'--workloads',c['workload'],'--bs',c['bs'],'--qds','32',
            '--file-gib',str(c['file_gib']),'--warmup',str(c['warmup']),'--rounds','1','--output-dir',out]
        if native:cmd+=['--mount','/result/native-f2fs','--devices','nvme0n1,nvme1n1']
        else:cmd+=['--prepare','--module-path',module,'--devices','nvme0n1,dm-0']
        if c['kind']=='fio-gc':cmd+=['--evaluation','gc-efficiency','--total-write-gib','54','--safety-timeout','10800']
        else:cmd+=['--measure','120']
        return cmd+['--run']
    if c['kind']=='prefill':
        return env+['python3',folder+'/src/performance_evaluation.py','--single-rate','--backend','dm-zns',
            '--filesystem',c['filesystem'],'--scenario',c['scenario'],'--workload','prefill','--rate',str(c['rate']),
            '--rounds','1','--warmup','60','--measure','180','--occupancy','80','--stall-limit','60','--run-dir',out]
    backlog=c['scenario']=='backlog'
    entry=BUILD+'/hooks/kafka-backlog-entry.py' if backlog else folder+'/src/kafka_only_evaluation.py'
    return env+['PYTHONPATH='+folder+'/src','BENCH_BACKLOG_CLASSES='+BUILD+'/hooks',
        'python3',entry,'--backend',c['backend'],'--filesystem',c['filesystem'],
        '--scenario','write' if backlog else c['scenario'],'--rates',str(c['rate']),'--prepare-rate',str(c['rate']),
        '--prepare-timeout','10800','--prepare-pass-count','3','--minimum-achieved-pct','95',
        '--warmup','0','--measure','1800','--retention-gib','18','--jar',folder+'/kafka-benchmark-1.0.jar',
        '--result-dir',out,'--execute']


def collect(c,out,host):
    dest=ROOT/'collected'/f"cell-{c['index']:02}";dest.mkdir(parents=True,exist_ok=True)
    for remote,target in [(out,dest/'result'),(out+'.launch.log',dest/'launch.log'),(out+'.exit.json',dest/'wrapper-exit.json')]:
        m.copy_from(remote,target)
    for f in host.glob('*.csv'):shutil.copy2(f,dest/f.name)
    folder=dest/'result';row=dict(collected=str(dest),status='completed',reasons=[])
    if json.loads((dest/'wrapper-exit.json').read_text())['exit_code']:
        row['reasons'].append('실행기 오류')
    if c['kind'].startswith('fio'):
        rows=json.loads((folder/'summary.json').read_text())
        if len(rows)!=1:raise RuntimeError('fio summary 불완전')
        s=rows[0];f=load('fio_parse',HERE/'fio-input/src/fio_evaluation.py')
        raw=json.loads((folder/(c['workload']+'-qd32-r1.fio.json')).read_text());f.parse_fio(raw)
        if c['kind']=='fio-gc':f.validate_fixed_write(raw,54*2**30)
        elif raw['jobs'][0]['read']['runtime']<114000:raise RuntimeError('fio 읽기 시간 불완전')
        if not s.get('kernel_log_available') or s.get('kernel_storage_errors'):row['reasons'].append('커널 로그 미확보 또는 저장장치 오류')
        s['femu']={f.name:load('femu_parse',HERE/'fio-input/src/fio_evaluation.py').femu_delta(f,s.get('measurement_start_us_approx',s['start_us']),s['end_us']) for f in dest.glob('*.csv')}
        if c['kind']=='fio-read':
            read=sum(v['read_bytes'] for k,v in s.get('os',{}).get('devices',{}).items() if k.startswith('nvme'))
            row['device_read_ratio_approx']=read/s['read_bytes'] if s.get('read_bytes') else None
            row['cache_warning']=row['device_read_ratio_approx'] is None or row['device_read_ratio_approx']<.5
        row['summary']=s
    elif c['kind']=='prefill':
        pe=load('prefill_parse',HERE/'prefill-input/src/performance_evaluation.py')
        v=pe.classify_run(folder,95,60);row['assessment']=v
        row['metrics']=list(csv.DictReader((folder/'csv/full_results.csv').open()))
        row['reasons'] += [r for r in v['reasons'] if 'achieved_pct=' not in r]
        if v.get('runner_exit_code') or v.get('kernel_storage_errors'):row['reasons'].append('실행기 또는 장치 오류')
        row['saturated']=not v['stable']
    else:
        pe=load('ko_parse',HERE/'ko-input/src/performance_evaluation.py')
        value=json.loads((folder/f"result-{c['rate']}.json").read_text());row['metrics']=value['metrics'];row['assessment']=value['assessment']
        if not value['validity']['valid']:row['reasons']+=value['validity']['invalid_reasons']
        row['reasons'] += [reason for reason in value['assessment'].get('reasons',[]) if '목표 달성률' not in reason]
        for key in ['failed_requests','send_errors','unresolved_after_drain','payload_crc_errors']:
            if value['metrics'].get(key,0):row['reasons'].append(key)
        kernel=folder/'env/performance-kernel-window.log'
        if not kernel.exists() or pe.kernel_errors(kernel):row['reasons'].append('커널 로그 미확보 또는 장치 오류')
        if not (folder/'readiness.json').exists() or not (folder/'completed.json').exists():row['reasons'].append('준비/완료 경계 불완전')
        if c['scenario']=='backlog':
            if not (folder/'backlog-complete.json').exists():row['reasons'].append('과거 로그 읽기 미완료')
            else:row['backlog']=json.loads((folder/'backlog-complete.json').read_text())
        row['saturated']=not value['assessment']['stable']
        first=folder/'retention-start.json';last=folder/'retention-end.json'
        if first.exists() and last.exists():
            a=json.loads(first.read_text())['sample'];b=json.loads(last.read_text())['sample']
            row['cost_boundary_seconds']=b['time']-a['time']
            if 'dm_status' in a and 'dm_status' in b:
                parse=lambda text:{k:int(v) for k,v in re.findall(r'([a-z_]+)=(\d+)',text)}
                x,y=parse(a['dm_status']),parse(b['dm_status'])
                keys=['logical_write_bytes','user_write_bytes','gc_write_bytes','wal_write_bytes','sstable_write_bytes','gc_reclaims','gc_moved_bytes']
                delta={k:y[k]-x[k] for k in keys}
                if any(v<0 for v in delta.values()):raise RuntimeError('DM 비용 counter reset')
                row['dm_delta']=delta
                row['dm_wa']=sum(delta[k] for k in ['user_write_bytes','gc_write_bytes','wal_write_bytes','sstable_write_bytes'])/delta['logical_write_bytes'] if delta['logical_write_bytes'] else None
            elif 'f2fs_status' in a and 'f2fs_status' in b:
                row['f2fs_delta']={k:int(b['f2fs_status'][k])-int(a['f2fs_status'][k]) for k in
                    ['gc_foreground_calls','gc_background_calls','gc_reclaimed_segments','moved_blocks_foreground','moved_blocks_background'] if k in a['f2fs_status'] and k in b['f2fs_status']}
    if row['reasons']:row['status']='failed'
    m.save(dest/'record.json',dict(cell=c,**row));return row


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--resume',action='store_true');args=parser.parse_args()
    if os.uname().nodename!='splab' or os.environ.get('USER')!='zns-team4':raise IdentityError('team4 host 전용')
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    qemu()  # 다른 VM이 있으면 예외. 무조건 종료하지 않는다.
    cells=planmod.cells();plan=dict(cells=cells,inputs=META,modules=MODULES,continue_after_failure=True,notion='인증 설정 없으면 outbox에 대기')
    if (ROOT/'plan.json').exists():
        if not args.resume or json.loads((ROOT/'plan.json').read_text())!=json.loads(json.dumps(plan)):raise IdentityError('기존 계획 보존/재개 불일치')
    else:m.save(ROOT/'plan.json',plan)
    done=json.loads((ROOT/'cell-records.json').read_text()) if (ROOT/'cell-records.json').exists() else []
    if [r['cell'] for r in done]!=cells[:len(done)]:raise IdentityError('완료 prefix 불일치')
    try:
        for c in cells[len(done):]:
            if (ROOT/'STOP').exists():raise UserStop('사용자 중지 표시: 후속 조건 보류')
            host=ROOT/'device'/f"cell-{c['index']:02}";out='/home/zns/kafka_python_result/results/final/'+RUN+f"/cell-{c['index']:02}"
            row=dict(cell=c,status='failed')
            try:
                attached=args.resume and host.exists() and m.guest('test -f '+shlex.quote(out+'.launch.log'),timeout=20,check=False).returncode==0
                if not attached:
                    if host.exists():raise RuntimeError('중단된 준비 단계: 원본 보존 후 다음 조건 진행')
                    m.status('booting',completed=len(done),total=len(cells),cell=c)
                    boot(c,host);deploy(c)
                    provenance="""from pathlib import Path
import subprocess,hashlib,json,os
out={'kernel':os.uname().release,'repositories':{}}
for name in ['dm-zns-base','Kafka-benchmark','kafka_python_result']:
 p=Path.home()/name
 def git(args):return subprocess.run(['git','-C',str(p),*args],capture_output=True,check=False)
 head=git(['rev-parse','HEAD']);status=git(['status','--short']);diff=git(['diff','HEAD','--','src','scripts','tests'])
 out['repositories'][name]={'commit':head.stdout.decode().strip(),'status':status.stdout.decode(),'code_diff_sha256':hashlib.sha256(diff.stdout).hexdigest()}
print(json.dumps(out))"""
                    m.save(host/'stack-provenance.json',json.loads(m.guest('python3 -c '+shlex.quote(provenance),timeout=40).stdout))
                    shell=shlex.join(command(c,out))+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
                    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join(['tmux','new-session','-d','-s',RUN+f"-cell-{c['index']:02}",'bash','-lc',shell]))
                row['boot_id']=m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()
                limit={'fio-read':1800,'prefill':3600,'kafka-only':18000,'fio-gc':16200}[c['kind']]
                markfile=host/'deadline.json'
                if not markfile.exists():m.save(markfile,dict(epoch_deadline=time.time()+limit))
                deadline=time.monotonic()+max(0,json.loads(markfile.read_text())['epoch_deadline']-time.time())
                while True:
                    if (ROOT/'STOP').exists():raise UserStop('사용자 중지 표시: 실행 상태 보존')
                    if not qemu():raise RuntimeError('FEMU 종료')
                    code='from pathlib import Path; p=Path('+repr(out+'.exit.json')+'); print(p.read_text() if p.exists() else "{}")'
                    mark=json.loads(m.guest('python3 -c '+shlex.quote(code),timeout=20,retry=True).stdout)
                    m.status('running',completed=len(done),total=len(cells),cell=c)
                    if mark:break
                    if time.monotonic()>deadline:raise RuntimeError('조건 watchdog 초과')
                    time.sleep(30)
                row.update(collect(c,out,host))
            except (IdentityError,UserStop):raise
            except Exception as error:
                row['error']=str(error)
                dest=ROOT/'collected'/f"cell-{c['index']:02}";dest.mkdir(parents=True,exist_ok=True)
                if not (dest/'result').exists():
                    for remote,target in [(out,dest/'partial-result'),(out+'.launch.log',dest/'partial-launch.log')]:
                        try:m.copy_from(remote,target)
                        except Exception as e:row.setdefault('collection_errors',[]).append(str(e))
                m.save(dest/'failure.json',row)
            done.append(row);m.save(ROOT/'cell-records.json',done)
            m.save(ROOT/'notion-outbox'/f"cell-{c['index']:02}.json",row)
            m.status('cell-finished',completed=len(done),total=len(cells),result=row)
            cleanup()
        m.status('completed',completed=len(done),total=len(cells),failed=sum(r['status']=='failed' for r in done))
    except BaseException as error:
        m.status('stopped',completed=len(done),total=len(cells),error=str(error),error_type=type(error).__name__);raise


if __name__=='__main__':main()

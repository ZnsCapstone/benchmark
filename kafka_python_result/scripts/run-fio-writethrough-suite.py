"""team4 host: 정규화 write-through에서 CNS/DM/Native short 독립 비교."""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import tempfile
import time
import sys

HERE=Path(__file__).resolve().parent
def load(name,path):
    s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
m=load('base',HERE/'controller-base.py')
fio=load('fio',HERE/'input/src/fio_evaluation.py')
RUN='fio-writethrough-20261026'
ROOT=Path('/home/zns-team4/results/final')/RUN
BUILD='/home/zns/'+RUN
BIN=Path('/home/zns-team4/femu/common-nand-20261026/build-align/qemu-system-x86_64')
META=json.loads((HERE/'build-manifest.json').read_text())
MODULE='/home/zns/fio-cns-matched-5c0cdbd-20261022/module-src/dm-zns-base.ko'
m.CONTROL=ROOT

def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cells():
    out=[]
    for repeat in range(1,6):
        for workload,bs in [('seq-write','4k'),('rand-write','4k'),('seq-write','1M')]:
            for backend,fs in [('cns','ext4'),('dm-zns','ext4'),('cns','f2fs'),('dm-zns','f2fs'),('native','f2fs')]:
                out.append(dict(index=len(out)+1,backend=backend,filesystem=fs,workload=workload,
                    bs=bs,repeat=repeat,qd=32,jobs=1,file_gib=1,warmup=20,measure=120))
    return out

def classify_vm(argv, executable, expected_binary, expected_image):
    """None=QEMU 아님, True=이 실행 소유 VM, False=다른/미확인 QEMU."""
    if not argv:return None
    if not (Path(executable).name.startswith('qemu-system-') or
            Path(argv[0]).name.startswith('qemu-system-')):
        return None
    if Path(executable).resolve()!=Path(expected_binary).resolve():return False
    names=[argv[i+1] for i,x in enumerate(argv[:-1]) if x=='-name']
    drives=[argv[i+1] for i,x in enumerate(argv[:-1]) if x=='-drive']
    images=[field[5:] for drive in drives for field in drive.split(',') if field.startswith('file=')]
    return names==['fio-wt-team4'] and images==[str(expected_image)]

def qemu():
    rows=[]
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            if proc.stat().st_uid!=os.getuid():continue
            argv=[a.decode(errors='replace') for a in (proc/'cmdline').read_bytes().split(b'\0') if a]
            if not argv:continue
            if not (Path(argv[0]).name.startswith('qemu-system-') or
                    (proc/'comm').read_text().startswith('qemu-system-')):
                continue
            executable=os.readlink(proc/'exe')
            owned=classify_vm(argv,executable,BIN,'/home/zns-team4/zns-team4.qcow2')
            if owned is False:raise RuntimeError('다른 team4 QEMU: 종료 금지 '+proc.name)
            if owned:rows.append(dict(pid=int(proc.name),argv=argv,executable=executable))
        except (FileNotFoundError,ProcessLookupError):continue
        except PermissionError as error:raise RuntimeError('프로세스 식별 권한 부족: 종료 금지') from error
    if len(rows)>1:raise RuntimeError('team4 QEMU 여러 개: 종료 금지')
    return rows
m.qemu=qemu

def device_args(c):
    cns='femu,devsz_mb=24576,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=1024,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95'
    zns='femu,devsz_mb=32768,namespaces=1,zns_num_ch=8,zns_num_lun=4,zns_num_plane=2,zns_num_blk=128,zns_flash_type=4,femu_mode=3'
    if c['backend']=='cns':return ['-device',cns]
    if c['backend']=='dm-zns':return ['-device',zns]
    meta=cns.replace('devsz_mb=24576','id=native-meta,serial=TEAM4-NATIVE-META,devsz_mb=1024').replace('blks_per_pl=1024','blks_per_pl=64')
    return ['-device',meta,'-device',zns.replace('femu,','femu,id=native-data,serial=TEAM4-NATIVE-DATA,',1)]

def boot(c,dest):
    m.shutdown()
    if digest(BIN)!=META['femu_sha256']:raise RuntimeError('FEMU binary 변경')
    dest.mkdir(parents=True,exist_ok=False)
    cmd=['env','FEMU_MATCHED_ARRAY=4k-lun-wt-v2','FEMU_STATS_FILE='+str(dest/'cns.csv'),
        'FEMU_ZNS_STATS_PREFIX='+str(dest/'zns'),str(BIN),'-name','fio-wt-team4','-enable-kvm',
        '-cpu','host','-smp','8','-m','16G','-device','virtio-scsi-pci,id=scsi0','-device','scsi-hd,drive=hd0',
        '-drive','file=/home/zns-team4/zns-team4.qcow2,if=none,aio=native,cache=none,format=qcow2,id=hd0',
        *device_args(c),'-netdev','user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22','-device','virtio-net-pci,netdev=net0',
        '-display','none','-monitor','none','-serial','file:'+str(dest/'serial.log')]
    m.save(dest/'command.json',cmd)
    shell=shlex.join(cmd)+' > '+shlex.quote(str(dest/'qemu.log'))+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(str(dest/'exit-code.txt'))
    m.run(['tmux','new-session','-d','-s',RUN+f"-vm-{c['index']:02}",'bash','-lc',shell])
    for _ in range(80):
        try:
            r=m.guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=8,check=False)
            if r.returncode==0:break
        except Exception as e:
            if not isinstance(e, __import__('subprocess').TimeoutExpired):raise
        time.sleep(2)
    else:raise RuntimeError('guest 부팅 확인 제한 초과')
    expected= {'cns':[('nvme0n1','none','50331648')], 'dm-zns':[('nvme0n1','host-managed','67108864')],
        'native':[('nvme0n1','none','2097152'),('nvme1n1','host-managed','67108864')]}[c['backend']]
    code="from pathlib import Path; import subprocess,json; expected="+repr(expected)+"; result=[]\nfor name,zoned,size in expected:\n assert Path('/sys/block',name,'size').read_text().strip()==size\n assert Path('/sys/block',name,'queue/zoned').read_text().strip()==zoned\n assert Path('/sys/block',name,'queue/write_cache').read_text().strip()=='write through'\n d=json.loads(subprocess.check_output(['sudo','-n','nvme','id-ctrl','/dev/'+name,'-o','json'],text=True)); assert d['vwc']==0\n result.append(dict(device=name,vwc=d['vwc'],zoned=zoned,size=size))\nprint(json.dumps(result))"
    r=m.guest('python3 -c '+shlex.quote(code),timeout=20)
    m.save(dest/'profile-verified.json',json.loads(r.stdout))
    return m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()

def deploy():
    m.active_guard()
    if m.guest('test -d '+BUILD,check=False).returncode:
        m.guest('mkdir -p '+BUILD+'/scripts')
        m.copy_to(HERE/'input/src',BUILD+'/src')
        for p in (HERE/'input/scripts').iterdir():m.copy_to(p,BUILD+'/scripts/'+p.name)
    hashes={BUILD+'/'+name:sha for name,sha in META['input_hashes'].items()}
    hashes[MODULE]=META['module_sha256']
    code='from pathlib import Path; import hashlib; expected='+repr(hashes)+'; assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in expected.items()), "입력 SHA 불일치"'
    m.guest('python3 -c '+shlex.quote(code),timeout=30)

def stack_provenance():
    code='''from pathlib import Path
import subprocess,hashlib,json
result={}
for name in ['dm-zns-base','Kafka-benchmark','kafka_python_result']:
 p=Path('/home/zns')/name
 def git(*args):
  r=subprocess.run(['git','-C',str(p),*args],capture_output=True)
  return r.stdout if r.returncode==0 else b''
 result[name]=dict(commit=git('rev-parse','HEAD').decode().strip(),status=git('status','--short').decode(),dirty_diff_sha256=hashlib.sha256(git('diff','HEAD','--binary')).hexdigest())
result['jars_not_used_by_fio']={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('/home/zns/Kafka-benchmark/build/libs').glob('*.jar')}
print(json.dumps(result))'''
    return json.loads(m.guest('python3 -c '+shlex.quote(code),timeout=40).stdout)

def launch(c,out):
    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent)))
    if c['backend']=='native':
        setup='/home/zns/kafka_python_result/results/native-zns/'+RUN+f"-setup-{c['index']:02}"
        m.guest(shlex.join(['bash',BUILD+'/scripts/prepare-native-f2fs-user24g.sh','--run',setup]),timeout=600)
        cmd=['python3','-u',BUILD+'/scripts/run-native-fio-cell.py','--output',out,'--cell-json',json.dumps(c),'--run']
    else:
        env=['BENCH_STORAGE_BACKEND='+c['backend'],'BENCH_FILESYSTEMS='+c['filesystem'],
             'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0','DM_GC_RESERVED_ZONES=8',
             'DM_GC_LOW_WATERMARK=16','DM_GC_HIGH_WATERMARK=20','DM_LOGICAL_CAPACITY_PERCENT=75']
        cmd=['env',*env,'python3','-u',BUILD+'/src/fio_profile_entry.py','--backend',c['backend'],
             '--filesystem',c['filesystem'],'--evaluation','performance','--workloads',c['workload'],
             '--bs',c['bs'],'--qds','32','--file-gib','1','--warmup','20','--measure','120',
             '--rounds','1','--output-dir',out,'--devices','nvme0n1','--prepare','--run']
        if c['backend']=='dm-zns':cmd+=['--module-path',MODULE]
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    m.guest(shlex.join(['tmux','new-session','-d','-s',RUN+f"-fio-{c['index']:02}",'bash','-lc',shell]))

def collect(c,out,host,boot_id):
    dest=ROOT/'collected'/c['backend']/c['filesystem']/(c['workload']+'-'+c['bs'])/f"r{c['repeat']}"
    dest.mkdir(parents=True,exist_ok=False)
    m.copy_from(out+'.launch.log',dest/'launch.log');m.copy_from(out+'.exit.json',dest/'wrapper-exit.json')
    exists=m.guest('test -d '+shlex.quote(out),check=False).returncode==0
    if exists:m.copy_from(out,dest/'result')
    if json.loads((dest/'wrapper-exit.json').read_text())['exit_code']:raise RuntimeError('fio 실행 실패: 원본 수집 완료')
    folder=dest/'result';rows=json.loads((folder/'summary.json').read_text())
    if len(rows)!=1:raise RuntimeError('결과 수 불일치')
    r=rows[0];native=c['backend']=='native'
    raw=folder/('measurement.fio.json' if native else c['workload']+'-qd32-r1.fio.json')
    payload=json.loads(raw.read_text());fio.parse_fio(payload)
    fio.parse_fio(json.loads((folder/'prepare.json').read_text()))
    prep=json.loads((folder/'prepare.json').read_text())['jobs'][0]
    if any(prep[k]['io_bytes']!=2**30 for k in ['write','read']):raise RuntimeError('CRC 준비 쓰기/읽기 양 불일치')
    if payload['jobs'][0]['write']['runtime']<114000:raise RuntimeError('측정 시간 미달')
    if not r.get('kernel_log_available') or r.get('kernel_storage_errors'):raise RuntimeError('커널 계측/저장 오류')
    if c['backend']=='dm-zns' and not r.get('dm_available'):raise RuntimeError('DM 계측 없음')
    r['femu']={}
    names=['cns.csv'] if c['backend']=='cns' else ['zns.vZNSSD0.csv']+(['cns.csv'] if native else [])
    for name in names:
        shutil.copy2(host/name,dest/name)
        stats=fio.femu_delta(dest/name,r['measurement_start_us_approx'],r['end_us'])
        if not stats.get('available'):raise RuntimeError('FEMU 통계 범위 부족')
        stats['scope']='정규화 NAND 서비스 모델; 실제 QLC 물리 WA 아님'
        r['femu'][name]=stats
    m.save(dest/'summary-collected.json',r)
    return dict(cell=c,boot_id=boot_id,collected=str(dest),summary=r,raw_sha256=digest(raw))

def main():
    if os.uname().nodename!='splab' or os.environ.get('USER')!='zns-team4':raise RuntimeError('team4 host 전용')
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=dict(cells=cells(),build=META,profile='4k-lun-wt-v2',fresh_femu=True,
        scope='short 성능 비교; GC 고정 쓰기량 실험 미포함; Native buffered fallback/discard 차이 유지')
    resume=sys.argv[1:]==['--resume']
    if sys.argv[1:] and not resume:raise RuntimeError('지원 옵션은 --resume뿐입니다')
    done=[]
    if (ROOT/'plan.json').exists():
        if not resume:raise RuntimeError('기존 실행 보존; 명시적 --resume 필요')
        if json.loads((ROOT/'plan.json').read_text())!=plan:raise RuntimeError('재개 계획/입력 변경')
        done=json.loads((ROOT/'completed-cells.json').read_text())
        if [d['cell'] for d in done]!=plan['cells'][:len(done)]:raise RuntimeError('완료 prefix 불일치')
        if len({d['boot_id'] for d in done})!=len(done):raise RuntimeError('완료 boot_id 중복')
        for d in done:
            folder=Path(d['collected'])/'result';c=d['cell'];native=c['backend']=='native'
            raw=folder/('measurement.fio.json' if native else c['workload']+'-qd32-r1.fio.json')
            if digest(raw)!=d['raw_sha256']:raise RuntimeError('완료 fio raw 변경')
            data=json.loads(raw.read_text());fio.parse_fio(data)
            if data['jobs'][0]['write']['runtime']<114000:raise RuntimeError('완료 runtime 부족')
            prep=json.loads((folder/'prepare.json').read_text());fio.parse_fio(prep)
            if any(prep['jobs'][0][k]['io_bytes']!=2**30 for k in ['read','write']):raise RuntimeError('완료 CRC 양 불일치')
            r=d['summary']
            if not r.get('kernel_log_available') or r.get('kernel_storage_errors'):raise RuntimeError('완료 커널 검증 실패')
            if json.loads((Path(d['collected'])/'summary-collected.json').read_text())!=r:raise RuntimeError('완료 집계 변경')
            profiles=json.loads((ROOT/'device'/f"cell-{c['index']:02}"/'profile-verified.json').read_text())
            if not profiles or any(p['vwc']!=0 for p in profiles):raise RuntimeError('완료 cache profile 불일치')
        history=Path(tempfile.mkdtemp(prefix='resume-history-',dir=ROOT))
        for name in ['status.json','completed-cells.json','plan.json','events.jsonl']:
            if (ROOT/name).exists():shutil.copy2(ROOT/name,history/name)
        m.save(history/'resume-check.json',dict(validated_completed=len(done),controller_sha256=digest(__file__)))
    elif resume:raise RuntimeError('재개할 기존 계획이 없음')
    else:m.save(ROOT/'plan.json',plan)
    try:
        for c in plan['cells'][len(done):]:
            host=ROOT/'device'/f"cell-{c['index']:02}"
            out='/home/zns/kafka_python_result/results/final/'+RUN+f"/cell-{c['index']:02}"
            m.status('booting',completed=len(done),total=len(plan['cells']),cell=c)
            boot_id=boot(c,host);deploy()
            m.save(host/'stack-provenance.json',stack_provenance())
            if boot_id in {d['boot_id'] for d in done}:raise RuntimeError('boot_id 중복')
            launch(c,out);deadline=time.monotonic()+3600
            while True:
                code='from pathlib import Path; p=Path('+repr(out+'.exit.json')+'); print(p.read_text() if p.exists() else "{}")'
                mark=json.loads(m.guest('python3 -c '+shlex.quote(code),timeout=20,retry=True).stdout)
                m.status('running',completed=len(done),total=len(plan['cells']),cell=c,boot_id=boot_id)
                if mark:break
                if time.monotonic()>deadline:raise RuntimeError('조건 1시간 초과: 부하 보존, 자동 초기화 안 함')
                time.sleep(60)
            done.append(collect(c,out,host,boot_id));m.save(ROOT/'completed-cells.json',done)
            m.status('cell-completed',completed=len(done),total=len(plan['cells']),cell=c)
        m.shutdown();m.status('completed',completed=len(done),total=len(plan['cells']))
    except BaseException as error:
        m.status('failed',completed=len(done),total=len(plan['cells']),error=str(error));raise

if __name__=='__main__':main()

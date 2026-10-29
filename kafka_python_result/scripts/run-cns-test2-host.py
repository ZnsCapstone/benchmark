"""team4: 원래 FEMU CNS의 80% prefill+Kafka 장기 압력 실험."""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import time

HERE=Path(__file__).resolve().parent
def load(name,path):
    s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
m=load('base',HERE/'controller-base.py')
pe=load('classify',HERE/'input/src/performance_evaluation.py')
RUN='cns-test2-prefill-long-20261027'
ROOT=Path('/home/zns-team4/results/final')/RUN
BUILD='/home/zns/'+RUN
BIN=Path('/home/zns-team4/femu/src/build-femu/qemu-system-x86_64')
META=json.loads((HERE/'build-manifest.json').read_text())
m.CONTROL=ROOT

def cells():
    return [dict(index=i+1,filesystem=fs,scenario=scenario,rate=rate,backend='cns')
        for i,(fs,scenario,rate) in enumerate((fs,scenario,rate)
        for fs in ['ext4','f2fs'] for scenario in ['write','read-write']
        for rate in [3000,5000,10000,20000])]

def qemu():
    result=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try:
            if p.stat().st_uid!=os.getuid():continue
            args=[a.decode(errors='replace') for a in (p/'cmdline').read_bytes().split(b'\0') if a]
            if not args:continue
            if not (Path(args[0]).name.startswith('qemu-system-') or (p/'comm').read_text().startswith('qemu-system-')):continue
            exe=Path(os.readlink(p/'exe'))
            names=[args[i+1] for i,a in enumerate(args[:-1]) if a=='-name']
            images=[v[5:] for i,a in enumerate(args[:-1]) if a=='-drive' for v in args[i+1].split(',') if v.startswith('file=')]
            if exe.resolve()!=BIN.resolve() or names!=['cns-test2-team4'] or images!=['/home/zns-team4/zns-team4.qcow2']:
                raise RuntimeError('다른 team4 QEMU: 종료 금지')
            result.append(p.name)
        except (FileNotFoundError,ProcessLookupError):continue
    if len(result)>1:raise RuntimeError('VM 복수: 종료 금지')
    return result
m.qemu=qemu

def boot(c,path):
    m.shutdown()
    if hashlib.sha256(BIN.read_bytes()).hexdigest()!=META['femu_sha256']:raise RuntimeError('기존 FEMU SHA 변경')
    path.mkdir(parents=True,exist_ok=False)
    dev='femu,devsz_mb=24576,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=1024,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95'
    cmd=['env','-u','FEMU_MATCHED_ARRAY','-u','FEMU_BIN','FEMU_STATS_FILE='+str(path/'cns.csv'),str(BIN),
         '-name','cns-test2-team4','-enable-kvm','-cpu','host','-smp','8','-m','16G',
         '-device','virtio-scsi-pci,id=scsi0','-device','scsi-hd,drive=hd0',
         '-drive','file=/home/zns-team4/zns-team4.qcow2,if=none,aio=native,cache=none,format=qcow2,id=hd0',
         '-device',dev,'-netdev','user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22','-device','virtio-net-pci,netdev=net0',
         '-display','none','-monitor','none','-serial','file:'+str(path/'serial.log')]
    m.save(path/'command.json',cmd)
    shell=shlex.join(cmd)+' > '+shlex.quote(str(path/'qemu.log'))+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(str(path/'qemu.exit'))
    m.run(['tmux','new-session','-d','-s',RUN+f"-vm-{c['index']:02}",'bash','-lc',shell])
    for _ in range(80):
        try:
            r=m.guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=8,check=False)
            if r.returncode==0:break
        except __import__('subprocess').TimeoutExpired:pass
        time.sleep(2)
    else:raise RuntimeError('부팅 확인 실패')
    code="from pathlib import Path; import subprocess; assert Path('/sys/block/nvme0n1/size').read_text().strip()=='50331648'; assert Path('/sys/block/nvme0n1/queue/zoned').read_text().strip()=='none'; assert 'FEMU' in subprocess.check_output(['lsblk','-dn','-o','MODEL','/dev/nvme0n1'],text=True)"
    m.guest('python3 -c '+shlex.quote(code))

def deploy():
    m.active_guard()
    if m.guest('test -d '+BUILD,check=False).returncode:
        m.guest('mkdir -p '+BUILD)
        m.copy_to(HERE/'input/src',BUILD+'/src')
        m.copy_to(HERE/'input/kafka-benchmark-1.0.jar',BUILD+'/kafka-benchmark-1.0.jar')
    expected={BUILD+'/'+name:sha for name,sha in META['input_hashes'].items()}
    code='from pathlib import Path; import hashlib; expected='+repr(expected)+'; assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==s for p,s in expected.items()), "입력 SHA 불일치"'
    m.guest('python3 -c '+shlex.quote(code),timeout=30)

def launch(c,out):
    cmd=['env','BENCH_STORAGE_BACKEND=cns','BENCH_LONG_GC=1','BENCH_DM_METRICS=0',
        'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0','BENCH_JAVA_JAR='+BUILD+'/kafka-benchmark-1.0.jar',
        'JAVA_TOOL_OPTIONS=-Dbenchmark.explicitPartitions=8','python3','-u',BUILD+'/src/cns_test2_entry.py',
        '--single-rate','--backend','cns','--filesystem',c['filesystem'],'--scenario',c['scenario'],
        '--workload','prefill','--rate',str(c['rate']),'--rounds','1','--warmup','0','--measure','1800',
        '--occupancy','80','--stall-limit','60','--run-dir',out]
    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent)))
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    m.guest(shlex.join(['tmux','new-session','-d','-s',RUN+f"-cell-{c['index']:02}",'bash','-lc',shell]))

def collect(c,out,host,boot_id):
    dest=ROOT/'collected'/c['filesystem']/c['scenario']/str(c['rate']);dest.mkdir(parents=True,exist_ok=False)
    m.copy_from(out,dest/'result');m.copy_from(out+'.launch.log',dest/'launch.log');m.copy_from(out+'.exit.json',dest/'wrapper-exit.json')
    shutil.copy2(host/'cns.csv',dest/'femu-cns.csv')
    folder=dest/'result';result=pe.classify_run(folder,90,60)
    m.save(dest/'classification.json',result)
    if json.loads((dest/'wrapper-exit.json').read_text())['exit_code'] or result['runner_exit_code'] or result.get('kernel_storage_errors'):
        raise RuntimeError('runner/장치 오류: 원본 보존')
    unsafe=[s for s in result['reasons'] if 'achieved_pct=' not in s]
    if unsafe:raise RuntimeError('요청/정합성/안전 오류: '+str(unsafe))
    ready=json.loads((folder/'long-readiness.json').read_text())
    if not ready['retention_all_partitions'] or ready['post_retention_ack_bytes']<10*2**30 or ready['measurement_seconds']!=1800:
        raise RuntimeError('장기 준비/측정 조건 불일치')
    if json.loads((folder/'long-sampler-errors.json').read_text()):raise RuntimeError('계측 오류')
    if not (folder/'long-measurement-start.json').exists() or not (folder/'long-measurement-end.json').exists():raise RuntimeError('경계 통계 누락')
    return dict(cell=c,boot_id=boot_id,result=result,collected=str(dest))

def main():
    if os.uname().nodename!='splab' or os.environ.get('USER')!='zns-team4':raise RuntimeError('team4 host 전용')
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if (ROOT/'plan.json').exists():raise RuntimeError('기존 실행 보존: 자동 재개 안 함')
    plan=dict(cells=cells(),build=META,prefill_percent=80,measure=1800,warmup=0,
        post_retention_ack_bytes=10*2**30,minimum_achieved_pct=90,stop_higher_on_saturation=True,
        scope='기존 FEMU CNS; prefill+Kafka 장기 공간 순환, benchmark4 자체 통과와 구분')
    m.save(ROOT/'plan.json',plan);done=[];skipped=[];saturated=set()
    try:
        for c in plan['cells']:
            key=(c['filesystem'],c['scenario'])
            if key in saturated:
                skipped.append(dict(cell=c,reason='앞선 부하가 90% 달성률 미달'));m.save(ROOT/'skipped-cells.json',skipped);continue
            host=ROOT/'device'/f"cell-{c['index']:02}"
            out='/home/zns/kafka_python_result/results/final/'+RUN+f"/cell-{c['index']:02}"
            m.status('booting',completed=len(done),skipped=len(skipped),total=16,cell=c)
            boot(c,host);deploy();boot_id=m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if boot_id in {d['boot_id'] for d in done}:raise RuntimeError('boot_id 중복')
            launch(c,out);deadline=time.monotonic()+8*3600
            while True:
                if not qemu():raise RuntimeError('FEMU 종료: 장치 통계/콘솔/게스트 디스크 원본 보존')
                code='from pathlib import Path; p=Path('+repr(out+'.exit.json')+'); print(p.read_text() if p.exists() else "{}")'
                mark=json.loads(m.guest('python3 -c '+shlex.quote(code),timeout=20,retry=True).stdout)
                m.status('running',completed=len(done),skipped=len(skipped),total=16,cell=c,boot_id=boot_id)
                if mark:break
                if time.monotonic()>deadline:raise RuntimeError('8시간 조건 한계: 자동 종료/초기화 안 함')
                time.sleep(60)
            row=collect(c,out,host,boot_id);done.append(row);m.save(ROOT/'completed-cells.json',done)
            if not row['result']['stable']:saturated.add(key)
            m.status('cell-completed',completed=len(done),skipped=len(skipped),total=16,cell=c,result=row['result'])
        m.shutdown();m.status('completed',completed=len(done),skipped=len(skipped),total=16)
    except BaseException as error:
        m.status('failed',completed=len(done),skipped=len(skipped),total=16,error=str(error));raise

if __name__=='__main__':main()

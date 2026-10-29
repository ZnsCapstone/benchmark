#!/usr/bin/env python3
"""splab의 zns-team4에서 실행. 외부 포워딩 없이 localhost SSH로 24조건 관리.

기존 게스트 결과의 종료 마커가 있으면 검증 후 수집하고 재실행하지 않는다.
host tmux에서 실행하며 호스트/다른 팀 VM을 재부팅하거나 강제 종료하지 않는다.
"""
import csv
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import shlex
import shutil
import subprocess
import tempfile
import time

PACKAGE=Path(__file__).resolve().parent
CONTROL=Path('/home/zns-team4/results/final/prepatch-5000-30m-control-20261022')
FEMU=Path('/home/zns-team4/femu/src/build-femu/qemu-system-x86_64')
FEMU_SHA='ae27d095f0fe527ae4fc8488590d1ec10279f38cee3db6fc376ae8a02eec0246'
MODULE='/home/zns/wal-stage-diag-20261020/module-src/dm-zns-base.ko'
MODULE_SHA='383f2b860f628c33755c28dac7da8128396e83861806e316332db291c72f5bd6'
SSH=['-i','/home/zns-team4/.ssh/kovalt-long-gc-20261021','-o','IdentitiesOnly=yes',
     '-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',
     '-o','UserKnownHostsFile=/home/zns-team4/.ssh/known_hosts',
     '-o','ConnectTimeout=5','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3']
LAUNCHERS={
    'dm-zns':('/home/zns-team4/run-dm-zns-512m.sh','cabaaa541d2e34c6c200f12655e32a8c349b9482898780f62ed974bcdb9e419a'),
    'native':('/home/zns-team4/run-native-f2fs.sh','a377dc516fcdbecc0d7856e33da91ac3beab2cef268a4a94b3f5c1ceedc605ee')}

def run(command,timeout=60,check=True):
    return subprocess.run(command,capture_output=True,text=True,timeout=timeout,check=check)

def guest(command,timeout=60,check=True,retry=False):
    for attempt in range(5 if retry else 1):
        try:
            result=run(['ssh',*SSH,'-p','2204','zns@127.0.0.1',command],timeout,False)
            if result.returncode!=255 or not retry:break
        except subprocess.TimeoutExpired:
            if not retry or attempt==4:raise
            result=None
        if attempt<4:time.sleep(5)
    if result is None:raise RuntimeError('게스트 SSH 읽기 제한 초과')
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode,command,result.stdout,result.stderr)
    return result

def copy_from(remote,local):
    run(['scp','-r',*SSH,'-P','2204','zns@127.0.0.1:'+remote,str(local)],timeout=300)

def copy_to(local,remote):
    run(['scp','-r',*SSH,'-P','2204',str(local),'zns@127.0.0.1:'+remote],timeout=180)

def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    temporary.replace(path)

def status(state,**values):
    value=dict(at=datetime.datetime.now().astimezone().isoformat(),state=state,**values)
    save(CONTROL/'status.json',value)
    with (CONTROL/'events.jsonl').open('a') as f:f.write(json.dumps(value,ensure_ascii=False)+'\n')
    print(json.dumps(value,ensure_ascii=False),flush=True)

def cells():
    return [dict(index=1,backend='dm-zns',scenario='write',filesystem='ext4',rate=5000)]

def measurement_seconds(cell):
    return 1800

def paths(cell):
    native=cell['backend']=='native'
    rel='results/final/prepatch-5000-30m-'+('native' if native else 'dm')+'-20261022'
    sub=f"{cell['scenario']}/{cell['rate']}ops" if native else f"{cell['scenario']}/{cell['filesystem']}/{cell['rate']}ops"
    return '/home/zns/kafka_python_result/'+rel+'/'+sub,Path('/home/zns-team4')/rel/sub

def source(cell):
    return PACKAGE/('native-input' if cell['backend']=='native' else 'dm-input')

def build_dir(cell):
    return '/home/zns/prepatch-5000-30m-'+('native' if cell['backend']=='native' else 'dm')+'-20261022'

def verify_local():
    if pwd.getpwuid(os.getuid()).pw_name!='zns-team4' or os.uname().nodename!='splab':
        raise RuntimeError('team4 FEMU 호스트 전용 실행기')
    if hashlib.sha256(FEMU.read_bytes()).hexdigest()!=FEMU_SHA:raise RuntimeError('FEMU SHA 변경')
    for path,sha in LAUNCHERS.values():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=sha:raise RuntimeError('실행기 SHA 변경')
    for kind in ['dm','native']:
        folder=PACKAGE/(kind+'-input')
        for name,sha in json.loads((folder/'source-hashes.json').read_text()).items():
            if hashlib.sha256((folder/name).read_bytes()).hexdigest()!=sha:raise RuntimeError('입력 SHA 변경: '+name)

def active_guard():
    guest('test "$(hostname)" = zns-guest && ! pgrep -x fio && ! pgrep -f "kafka[.]Kafka|com[.]hanyang[.]cs[.]KafkaBenchmark"',timeout=20,retry=True)

def qemu():
    r=run(['pgrep','-a','-u','zns-team4','-f','[q]emu-system-x86_64'],check=False)
    if r.returncode==1:return []
    if r.returncode:raise RuntimeError('team4 FEMU 조회 실패')
    lines=r.stdout.strip().splitlines()
    if len(lines)!=1 or not any('-name '+name in lines[0] for name in ['dm-zns-512m-team4','native-f2fs-team4']):
        raise RuntimeError('예상치 못한 team4 VM: 종료하지 않음')
    return lines

def shutdown():
    if not qemu():return
    active_guard()
    guest('set -e; for point in /result/kafka-logs /result/native-f2fs; do if mountpoint -q "$point"; then sudo -n umount "$point"; fi; done; sudo -n sync',timeout=180)
    result=guest('sudo -n poweroff',timeout=30,check=False)
    if result.returncode not in (0,255):raise RuntimeError('정상 poweroff 요청 실패')
    for _ in range(60):
        if not qemu():return
        time.sleep(2)
    raise RuntimeError('team4 FEMU 정상 종료 대기 초과: 강제 종료하지 않음')

def boot(cell,host):
    shutdown()
    if host.exists():raise RuntimeError('미완료 host 결과 경로 존재: 덮어쓰기 거절 '+str(host))
    host.mkdir(parents=True)
    native=cell['backend']=='native'
    args=['env','-u','FEMU_BIN','FEMU_ZNS_STATS_PREFIX='+str(host/'zns')]
    if native:args.append('FEMU_STATS_FILE='+str(host/'cns.csv'))
    args+=['bash',LAUNCHERS[cell['backend']][0]]
    shell=shlex.join(args)+' > '+shlex.quote(str(host/'console.log'))+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(str(host/'exit-code.txt'))
    run(['tmux','new-session','-d','-s',f"prepatch-5k-femu-{cell['index']:02}",'bash','-lc',shell])
    for _ in range(80):
        try:
            r=guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=10,check=False)
            if not r.returncode:break
        except subprocess.TimeoutExpired:pass
        time.sleep(3)
    else:raise RuntimeError('게스트 부팅 확인 제한 초과')
    check="test \"$(cat /sys/block/nvme1n1/size)\" = 67108864 && test \"$(cat /sys/block/nvme0n1/size)\" = 2097152" if native else 'test "$(cat /sys/block/nvme0n1/size)" = 67108864'
    guest(check,timeout=20)
    import csv
    for name in (['cns.csv','zns.vZNSSD0.csv'] if native else ['zns.vZNSSD0.csv']):
        with (host/name).open() as f:row=next(csv.DictReader(f))
        if int(row['host_write_bytes']):raise RuntimeError('새 FEMU 초기 counter가 0이 아님')

def deploy(cell):
    folder=source(cell);build=build_dir(cell);native=cell['backend']=='native'
    exists=guest('test -d '+shlex.quote(build),check=False,retry=True).returncode==0
    if not exists:
        active_guard()
        guest('mkdir -p '+shlex.quote(build if native else build+'/runner'))
        copy_to(folder/'src',build+'/' if native else build+'/runner/')
        copy_to(folder/'kafka-benchmark-1.0.jar',build+'/kafka-benchmark-1.0.jar')
        if native:
            for name in ['run-native-kafka-cell.py','prepare-native-f2fs-user24g.sh']:copy_to(folder/name,build+'/'+name)
    hashes={}
    for name,sha in json.loads((folder/'source-hashes.json').read_text()).items():
        path=build+'/'+name if native or not name.startswith('src/') else build+'/runner/'+name
        hashes[path]=sha
    if not native:hashes[MODULE]=MODULE_SHA
    code='import hashlib,json; from pathlib import Path; expected='+repr(hashes)+'; actual={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in expected}; assert actual==expected, "게스트 입력 SHA 불일치"; print(json.dumps(actual))'
    r=guest('python3 -c '+shlex.quote(code),timeout=40,retry=True)
    save(CONTROL/('verified-'+cell['backend']+'.json'),json.loads(r.stdout))

def launch(cell,out):
    native=cell['backend']=='native';build=build_dir(cell)
    if native:
        setup=f"/home/zns/kafka_python_result/results/native-zns/keper-test2-long-host-setup-20261022/case-{cell['index']:02}"
        guest(shlex.join(['bash',build+'/prepare-native-f2fs-user24g.sh','--run',setup]),timeout=300)
    env=dict(BENCH_LONG_GC='1',BENCH_JAVA_JAR=build+'/kafka-benchmark-1.0.jar',
             JAVA_TOOL_OPTIONS='-Dbenchmark.explicitPartitions=8')
    if native:env['BENCH_NATIVE_LONG']='1'
    else:env.update(DM_ZNS_MODULE_PATH=MODULE,BENCH_DM_METRICS='1',BENCH_WAL_DELAY_MS='0',
        DM_GC_RESERVED_ZONES='8',DM_GC_LOW_WATERMARK='16',DM_GC_HIGH_WATERMARK='20',
        DM_LOGICAL_CAPACITY_PERCENT='75',BENCH_EXT4_DISCARD='0',BENCH_F2FS_DISCARD='0')
    entry=build+'/run-native-kafka-cell.py' if native else build+'/runner/src/performance_evaluation.py'
    cmd=['env',*[f'{k}={value}' for k,value in env.items()],'python3','-u',entry,
         '--single-rate','--backend','dm-zns','--filesystem',cell['filesystem'],'--scenario',cell['scenario'],
         '--workload','prefill','--rate',str(cell['rate']),'--rounds','1','--warmup','0','--measure',str(measurement_seconds(cell)),
         '--occupancy','80','--stall-limit','60','--run-dir',out]
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    guest('mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join(['tmux','new-session','-d','-s',f"prepatch-5k-host-cell-{cell['index']:02}",'bash','-lc',shell]))

def probe(cell,out):
    code='''from pathlib import Path
import json,time
p=Path(OUT)
r={'epoch_us':time.time_ns()//1000,'files':{}}
for name in ['long-phase.json','long-readiness.json','long-measurement-start.json','long-measurement-end.json']:
 f=p/name
 if f.exists():r['files'][name]=json.loads(f.read_text())
f=Path(str(p)+'.exit.json')
if f.exists():r['exit']=json.loads(f.read_text())
print(json.dumps(r))'''.replace('OUT',repr(out))
    return json.loads(guest('python3 -c '+shlex.quote(code),timeout=20,retry=True).stdout)

def validate(folder,cell):
    spec=importlib.util.spec_from_file_location('classify',source(cell)/'src/performance_evaluation.py')
    pe=importlib.util.module_from_spec(spec);spec.loader.exec_module(pe)
    result=pe.classify_run(folder,90,60)
    if result['status']=='infrastructure_error':raise RuntimeError(str(result))
    for reason in result.get('reasons',[]):
        if 'achieved_pct=' not in reason:raise RuntimeError('안전/정합성 실패: '+reason)
    rows=list(csv.DictReader((folder/'csv/full_results.csv').open()))
    if len(rows)!=1:raise RuntimeError('단일 조건 결과 수 불일치')
    row=rows[0]
    expected=dict(filesystem=cell['filesystem'],scenario='scenario_a' if cell['scenario']=='write' else 'scenario_b',
                  target_ops=str(cell['rate']),measure_sec=str(measurement_seconds(cell)),warmup_sec='0',producers='8',dynamic_topics='False')
    if any(row.get(k)!=value for k,value in expected.items()):raise RuntimeError('실측 조건 불일치')
    ready=json.loads((folder/'long-readiness.json').read_text())
    if not ready.get('retention_all_partitions') or ready.get('post_retention_ack_bytes',0)<10*1024**3:
        raise RuntimeError('retention 준비량 미달')
    if json.loads((folder/'long-sampler-errors.json').read_text()):raise RuntimeError('GC 계측 오류')
    for name in ['long-measurement-start.json','long-measurement-end.json']:
        snap=json.loads((folder/name).read_text())
        if not snap.get('f2fs_status' if cell['backend']=='native' else 'dm_status'):raise RuntimeError('GC 경계 계측 누락')
    return result

def collect(cell,out,host,boot_id):
    parent=CONTROL/'collected'/cell['backend']/cell['scenario']/cell['filesystem']
    parent.mkdir(parents=True,exist_ok=True);final=parent/f"{cell['rate']}ops"
    if not final.exists():
        temporary=Path(tempfile.mkdtemp(prefix='.collect-',dir=parent))
        copy_from(out,temporary/'result')
        copy_from(out+'.launch.log',temporary/'result/launch.log')
        copy_from(out+'.exit.json',temporary/'result/wrapper-exit.json')
        if cell['backend']=='native':
            setup=f"/home/zns/kafka_python_result/results/native-zns/keper-test2-long-host-setup-20261022/case-{cell['index']:02}"
            copy_from(setup,temporary/'result/native-setup')
        for name in ['zns.vZNSSD0.csv']+(['cns.csv'] if cell['backend']=='native' else []):
            shutil.copy2(host/name,temporary/'result'/name)
        validate(temporary/'result',cell)
        (temporary/'result').rename(final)
    if json.loads((final/'wrapper-exit.json').read_text()).get('exit_code')!=0:raise RuntimeError('wrapper 실패')
    result=validate(final,cell)
    return dict(cell=cell,boot_id=boot_id,result=result,collected=str(final),guest_output=out,
        csv_sha256=hashlib.sha256((final/'csv/full_results.csv').read_bytes()).hexdigest())

def main():
    verify_local();CONTROL.mkdir(parents=True,exist_ok=True)
    lock=(CONTROL/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=dict(cells=cells(),threshold=90,measure=1800,post_retention_ack_bytes=10*1024**3,
              module_sha256=MODULE_SHA,femu_sha256=FEMU_SHA,external_ssh_required=False)
    if (CONTROL/'plan.json').exists() and json.loads((CONTROL/'plan.json').read_text())!=plan:
        raise RuntimeError('원격 재개 계획 불일치')
    save(CONTROL/'plan.json',plan)
    done=json.loads((CONTROL/'completed-cells.json').read_text()) if (CONTROL/'completed-cells.json').exists() else []
    if [r['cell'] for r in done]!=plan['cells'][:len(done)]:raise RuntimeError('완료 prefix 불일치')
    for record in done:
        validate(Path(record['collected']),record['cell'])
        if hashlib.sha256((Path(record['collected'])/'csv/full_results.csv').read_bytes()).hexdigest()!=record['csv_sha256']:
            raise RuntimeError('완료 결과 SHA 변경')
    try:
        for cell in plan['cells'][len(done):]:
            out,host=paths(cell)
            status('checking-existing',completed=len(done),cell=cell)
            p=probe(cell,out)
            if 'exit' not in p:
                exists=guest('test -e '+shlex.quote(out),check=False,retry=True).returncode==0
                if exists:
                    session=guest('tmux has-session -t '+f"prepatch-5k-host-cell-{cell['index']:02}",check=False).returncode==0
                    if not session:raise RuntimeError('기존 불완전 결과: 자동 덮어쓰기/재실행 거절')
                else:
                    if shutil.disk_usage('/home/zns-team4').free<2*1024**3:raise RuntimeError('호스트 결과 공간 부족')
                    guest("python3 -c \"import shutil; assert shutil.disk_usage('/home/zns').free>=2*1024**3\"",retry=True)
                    status('booting',completed=len(done),cell=cell)
                    boot(cell,host);deploy(cell);launch(cell,out)
            else:
                deploy(cell)
            boot_id=guest('cat /proc/sys/kernel/random/boot_id',retry=True).stdout.strip()
            deadline=time.monotonic()+8*3600
            while 'exit' not in p:
                p=probe(cell,out)
                status('running',completed=len(done),cell=cell,guest=p)
                if 'exit' in p:break
                if time.monotonic()>deadline:raise RuntimeError('조건 전체8시간 제한: 원격 실행 보존')
                time.sleep(60)
            if p['exit'].get('exit_code')!=0:raise RuntimeError('게스트 부하기 실패: 원본 보존')
            record=collect(cell,out,host,boot_id)
            done.append(record);save(CONTROL/'completed-cells.json',done)
            status('cell-completed',completed=len(done),cell=cell,result=record['result'])
        status('completed',completed=len(done));save(CONTROL/'exit-code.json',dict(exit_code=0))
    except BaseException as exc:
        status('failed',completed=len(done),error=str(exc));raise

if __name__=='__main__':main()

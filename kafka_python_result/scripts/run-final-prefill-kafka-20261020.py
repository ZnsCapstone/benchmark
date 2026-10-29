#!/usr/bin/env python3
"""2번: 80% prefill+Kafka, 각 부하 독립 FEMU, 포화 뒤 높은 부하 생략."""
import datetime
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
REL='results/final/test2-prefill-kafka-20261020'
INPUT=ROOT/'results/final/test2-input-20261020'
BUILD='/home/zns/test2-prefill-kafka-20261020'
MODULE='/home/zns/wal-stage-diag-20261020/module-src/dm-zns-base.ko'
MODULE_SHA='383f2b860f628c33755c28dac7da8128396e83861806e316332db291c72f5bd6'

def main():
    long_run=globals().get('LONG_RUN',False)
    warmup,measure,threshold=(0,3600,90) if long_run else (60,180,95)
    spec=importlib.util.spec_from_file_location('v11',HERE/'run-fio-v11-controller.py')
    v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
    local=ROOT/REL
    local.mkdir(exist_ok=False)
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    groot=v.c.GUEST_ROOT+'/'+REL;hroot=v.c.HOST_ROOT+'/'+REL
    done=[]
    def status(state,**extra):
        v.c.save(local/'status.json',dict(at=datetime.datetime.now().astimezone().isoformat(),
                   state=state,completed=len(done),**extra))
    try:
        status('preflight')
        v.c.active_guest_guard()
        v.n.check_hash('guest',MODULE,MODULE_SHA)
        hashes=json.loads((INPUT/'source-hashes.json').read_text())
        for name,sha in hashes.items():
            if hashlib.sha256((INPUT/name).read_bytes()).hexdigest()!=sha:
                raise RuntimeError('보존 입력 변경: '+name)
        v.c.call('guest','test ! -e '+shlex.quote(BUILD)+' && mkdir -p '+shlex.quote(BUILD+'/runner'))
        subprocess.run(['scp','-r',*v.c.GUEST[:-1],str(INPUT/'src'),'zns@cns-team4:'+BUILD+'/runner/'],check=True,timeout=120)
        v.n.guest_scp([INPUT/'kafka-benchmark-1.0.jar'],BUILD+'/kafka-benchmark-1.0.jar')
        for name,sha in hashes.items():
            path=BUILD+'/runner/'+name if name.startswith('src/') else BUILD+'/'+name
            v.n.check_hash('guest',path,sha)
        v.n.guest_scp([ROOT/'results/final/TEST1-PRESERVED-20261020.md'],'/home/zns/kafka_python_result/results/final/')
        launcher_sha=hashlib.sha256((HERE/'run-dm-zns-512m.sh').read_bytes()).hexdigest()
        plan=dict(module_sha256=MODULE_SHA,module_source_sha256='743c05e66f9213a4f371ef8f6ab7e1ed657617f0d2c44d1fffba67675d653653',
            filesystems=['ext4','f2fs'],scenarios=['write','read-write'],rates=[3000,5000,10000,20000],
            warmup=warmup,measure=measure,rounds=1,occupancy=80,wal_delay_ms=0,explicit_partitions=8,
            logical_gib=24,physical_gib=32,zone_mib=512,gc_reserve_low_high=[8,16,20],
            minimum_achieved_pct=threshold,stop_higher_rates_on_saturation=not long_run,fresh_femu_per_rate=True,
            source_hashes=hashes,femu_sha256=v.c.EXPECTED_FEMU_SHA)
        v.c.save(local/'plan.json',plan)
        if long_run:
            plan.update(post_retention_ack_bytes=10*1024**3,retention_prepare_timeout_seconds=21600,
                        experiment='장기 지속 공간 순환/GC 압력')
            v.c.save(local/'plan.json',plan)
        subprocess.run(['cp','-a',str(INPUT/'provenance.json'),str(local/'provenance.json')],check=True)
        index=0;boots=set()
        for scenario in plan['scenarios']:
            for fs in plan['filesystems']:
                for rate in plan['rates']:
                    index+=1;cell=dict(index=index,filesystem=fs,scenario=scenario,rate=rate)
                    sub=f'{scenario}/{fs}/{rate}ops';out=groot+'/'+sub;host=hroot+'/'+sub
                    status('booting',cell=cell)
                    v.c.call('guest',"python3 -c \"import shutil; assert shutil.disk_usage('/home/zns').free >= 2*1024**3, '결과 디스크 여유 2GiB 미만: 원본 보존 후 중단'\"")
                    v.shutdown_with_grace()
                    v.d.boot(host,f'prefill-team4-{index:02}',launcher_sha)
                    boot=v.n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
                    if boot in boots:raise RuntimeError('boot_id 중복')
                    boots.add(boot);v.n.check_hash('guest',MODULE,MODULE_SHA)
                    env=dict(DM_ZNS_MODULE_PATH=MODULE,BENCH_DM_METRICS='1',BENCH_WAL_DELAY_MS='0',
                        DM_GC_RESERVED_ZONES='8',DM_GC_LOW_WATERMARK='16',DM_GC_HIGH_WATERMARK='20',
                        DM_LOGICAL_CAPACITY_PERCENT='75',BENCH_EXT4_DISCARD='0',BENCH_F2FS_DISCARD='0',
                        JAVA_TOOL_OPTIONS='-Dbenchmark.explicitPartitions=8',
                        BENCH_JAVA_JAR=BUILD+'/kafka-benchmark-1.0.jar')
                    if long_run:env['BENCH_LONG_GC']='1'
                    cmd=['env',*[f'{k}={val}' for k,val in env.items()],'python3',BUILD+'/runner/src/performance_evaluation.py',
                         '--single-rate','--backend','dm-zns','--filesystem',fs,'--scenario',scenario,
                         '--workload','prefill','--rate',str(rate),'--rounds','1','--warmup',str(warmup),
                         '--measure',str(measure),'--occupancy','80','--stall-limit','60','--run-dir',out]
                    wrapper=out+'.exit.json'
                    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(wrapper)
                    v.c.call('guest','mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join(['tmux','new-session','-d','-s',f'prefill-team4-{index:02}','bash','-lc',shell]))
                    status('running',cell=cell,boot_id=boot,destination=out)
                    verified=False;deadline=time.monotonic()+(28800 if long_run else 10800)
                    while time.monotonic()<deadline:
                        time.sleep(60)
                        r=v.c.call('guest','if test -s '+shlex.quote(wrapper)+'; then cat '+shlex.quote(wrapper)+'; else echo RUNNING; fi',timeout=20,check=False)
                        if r.returncode:raise RuntimeError('SSH 상태 조회 실패: 원격 실행 보존')
                        if not verified and r.stdout.strip()=='RUNNING':
                            try:
                                actual=v.environment(0,fs);verified=True
                                v.c.save(local/f'case-{index:02}.environment.json',actual)
                            except subprocess.CalledProcessError:pass
                        if r.stdout.strip()=='RUNNING':continue
                        result=json.loads(r.stdout);break
                    else:raise RuntimeError('3시간 제한: 원격 실행 보존, 후속 중단')
                    destination=local/sub;destination.parent.mkdir(parents=True,exist_ok=True)
                    subprocess.run(['scp','-r',*v.c.GUEST[:-1],'zns@cns-team4:'+out,str(destination)],check=True,timeout=180)
                    subprocess.run(['scp',*v.c.GUEST[:-1],'zns@cns-team4:'+out+'.launch.log',str(destination/'launch.log')],check=True,timeout=90)
                    subprocess.run(['scp',*v.c.RELAY[:-3],'-P',v.c.RELAY[-2],
                        'zns-team4@192.168.67.1:'+host+'/zns.vZNSSD0.csv',str(destination/'femu-zns.csv')],check=True,timeout=90)
                    import sys
                    sys.path.insert(0,str(INPUT/'src'))
                    import performance_evaluation as pe
                    classified=pe.classify_run(destination,threshold,60)
                    record=dict(cell=cell,result=classified,exit=result,boot_id=boot)
                    done.append(record);v.c.save(local/'completed-cells.json',done)
                    status('cell-completed',cell=cell,result=classified)
                    unsafe=long_run and any('achieved_pct=' not in reason for reason in classified.get('reasons',[]))
                    if result['exit_code']!=0 or classified['status']=='infrastructure_error' or classified.get('kernel_storage_errors') or unsafe:
                        raise RuntimeError('실행/저장 오류: 다음 조건 중단, 원본 보존')
                    if not classified['stable'] and not long_run:break
        status('completed');v.c.save(local/'exit-code.json',dict(exit_code=0))
    except BaseException as exc:
        status('failed',error=str(exc));raise

if __name__=='__main__':main()

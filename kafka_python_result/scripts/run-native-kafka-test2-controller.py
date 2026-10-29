#!/usr/bin/env python3
"""Native F2FS의 2번 평가. 각 rate fresh FEMU, 포화 후 상위 rate 생략."""
import datetime
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
REL='results/final/test2-native-f2fs-20261020'
INPUT=ROOT/'results/final/native-test2-input-20261020'
BUILD='/home/zns/native-kafka-test2-20261020'

def main():
    long_run=globals().get('LONG_RUN',False)
    warmup,measure,threshold=(0,3600,90) if long_run else (60,180,95)
    spec=importlib.util.spec_from_file_location('v11',HERE/'run-fio-v11-controller.py')
    v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
    c,n=v.c,v.n
    local=ROOT/REL;local.mkdir(exist_ok=False)
    lock=open('/tmp/kovalt-cns-fio-controller.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    done=[]
    def status(state,**kw):c.save(local/'status.json',dict(at=datetime.datetime.now().astimezone().isoformat(),state=state,completed=len(done),**kw))
    try:
        status('preflight');c.active_guest_guard()
        hashes=json.loads((INPUT/'source-hashes.json').read_text())
        for name,sha in hashes.items():
            if hashlib.sha256((INPUT/name).read_bytes()).hexdigest()!=sha:raise RuntimeError('입력 SHA 변경')
        c.call('guest','test ! -e '+shlex.quote(BUILD)+' && mkdir -p '+shlex.quote(BUILD))
        subprocess.run(['scp','-r',*c.GUEST[:-1],str(INPUT/'src'),'zns@cns-team4:'+BUILD+'/'],check=True,timeout=120)
        n.guest_scp([INPUT/'run-native-kafka-cell.py',INPUT/'prepare-native-f2fs-user24g.sh',INPUT/'kafka-benchmark-1.0.jar'],BUILD+'/')
        for name,sha in hashes.items():n.check_hash('guest',BUILD+'/'+name,sha)
        plan=dict(backend='native-f2fs-zns',rates=[3000,5000,10000,20000],scenarios=['write','read-write'],
            warmup=warmup,measure=measure,rounds=1,occupancy=80,user_gib=24,physical_zns_gib=32,
            auxiliary_cns_gib=1,zone_mib=512,explicit_partitions=8,retention_gib=2,discard=True,
            occupancy_denominator='24GiB CP user capacity',minimum_achieved_pct=threshold,
            fresh_femu_per_rate=True,source_hashes=hashes,femu_sha256=c.EXPECTED_FEMU_SHA)
        c.save(local/'plan.json',plan)
        if long_run:
            plan.update(post_retention_ack_bytes=10*1024**3,retention_prepare_timeout_seconds=21600,
                        experiment='장기 지속 공간 순환/GC 압력',stop_higher_rates_on_saturation=False)
            c.save(local/'plan.json',plan)
        index=0;boots=set()
        for scenario in plan['scenarios']:
            for rate in plan['rates']:
                index+=1;cell=dict(index=index,filesystem='f2fs',scenario=scenario,rate=rate)
                sub=f'{scenario}/{rate}ops';out=c.GUEST_ROOT+'/'+REL+'/'+sub;host=c.HOST_ROOT+'/'+REL+'/'+sub
                status('booting',cell=cell)
                c.call('guest',"python3 -c \"import shutil; assert shutil.disk_usage('/home/zns').free>=2*1024**3\"")
                v.shutdown_with_grace()
                n.boot(host,'native-kafka-team4',index);n.fresh_stats(host)
                boot=n.read_call('guest','cat /proc/sys/kernel/random/boot_id').stdout.strip()
                if boot in boots:raise RuntimeError('독립 boot_id 중복')
                boots.add(boot)
                setup=c.GUEST_ROOT+f'/results/native-zns/test2-setup-20261020/case-{index:02}'
                n.check_hash('guest',BUILD+'/prepare-native-f2fs-user24g.sh',hashes['prepare-native-f2fs-user24g.sh'])
                status('formatting-native',cell=cell,boot_id=boot)
                c.call('guest',shlex.join(['bash',BUILD+'/prepare-native-f2fs-user24g.sh','--run',setup]),timeout=300)
                env=dict(BENCH_JAVA_JAR=BUILD+'/kafka-benchmark-1.0.jar',JAVA_TOOL_OPTIONS='-Dbenchmark.explicitPartitions=8')
                if long_run:env.update(BENCH_LONG_GC='1',BENCH_NATIVE_LONG='1')
                cmd=['env',*[f'{k}={value}' for k,value in env.items()],'python3','-u',BUILD+'/run-native-kafka-cell.py',
                    '--single-rate','--backend','dm-zns','--filesystem','f2fs','--scenario',scenario,
                    '--workload','prefill','--rate',str(rate),'--rounds','1','--warmup',str(warmup),'--measure',str(measure),
                    '--occupancy','80','--stall-limit','60','--run-dir',out]
                wrapper=out+'.exit.json';session=f'native-kafka-cell-{index:02}'
                shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(wrapper)
                c.call('guest','mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join(['tmux','new-session','-d','-s',session,'bash','-lc',shell]))
                status('running',cell=cell,boot_id=boot,destination=out)
                deadline=time.monotonic()+(28800 if long_run else 10800)
                while time.monotonic()<deadline:
                    time.sleep(60)
                    r=c.call('guest','if test -s '+shlex.quote(wrapper)+'; then cat '+shlex.quote(wrapper)+'; elif tmux has-session -t '+session+' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi',timeout=20,check=False)
                    if r.returncode:raise RuntimeError('SSH 확인 실패: 원격 실행 보존')
                    if r.stdout.strip()=='RUNNING':continue
                    if r.stdout.strip()=='STOPPED':raise RuntimeError('wrapper 없이 종료')
                    result=json.loads(r.stdout);break
                else:raise RuntimeError('3시간 제한: 원격 실행 보존')
                destination=local/sub;destination.parent.mkdir(parents=True,exist_ok=True)
                subprocess.run(['scp','-r',*c.GUEST[:-1],'zns@cns-team4:'+out,str(destination)],check=True,timeout=180)
                for src,name in [(out+'.launch.log','launch.log'),(setup,'native-setup')]:
                    subprocess.run(['scp','-r',*c.GUEST[:-1],'zns@cns-team4:'+src,str(destination/name)],check=True,timeout=90)
                for name in ['cns.csv','zns.vZNSSD0.csv']:
                    subprocess.run(['scp',*c.RELAY[:-3],'-P',c.RELAY[-2],'zns-team4@192.168.67.1:'+host+'/'+name,str(destination/name)],check=True,timeout=90)
                sys.path.insert(0,str(INPUT/'src'));import performance_evaluation as pe
                classified=pe.classify_run(destination,threshold,60)
                done.append(dict(cell=cell,result=classified,exit=result,boot_id=boot));c.save(local/'completed-cells.json',done)
                status('cell-completed',cell=cell,result=classified)
                unsafe=long_run and any('achieved_pct=' not in reason for reason in classified.get('reasons',[]))
                if result['exit_code'] or classified['status']=='infrastructure_error' or classified.get('kernel_storage_errors') or unsafe:
                    raise RuntimeError('실행/저장 오류: 후속 중단')
                if not classified['stable'] and not long_run:break
        status('completed');c.save(local/'exit-code.json',dict(exit_code=0))
    except BaseException as exc:status('failed',error=str(exc));raise

if __name__=='__main__':main()

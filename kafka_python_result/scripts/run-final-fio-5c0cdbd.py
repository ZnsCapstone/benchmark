#!/usr/bin/env python3
"""team4 host: 기존 CNS와 같은 short 30조건, 기본 정책 DM 측정."""
import csv
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
import shutil
import tempfile
import time

HERE=Path(__file__).resolve().parent
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod

m=load('base',HERE/'controller-base.py')
p=load('plan',HERE/'plan-fio-final.py')
fio=load('fio',HERE/'dm-input/src/fio_evaluation.py')
RUN='fio-cns-matched-5c0cdbd-20261022'
m.PACKAGE=HERE
m.CONTROL=Path('/home/zns-team4/results/final')/RUN
META=json.loads((HERE/'build-manifest.json').read_text())
m.MODULE='/home/zns/'+RUN+'/module-src/dm-zns-base.ko'
m.MODULE_SHA=META['module_sha256']
launcher=HERE/'run-femu-headless.sh'
m.LAUNCHERS['dm-zns']=(str(launcher),hashlib.sha256(launcher.read_bytes()).hexdigest())
BUILD='/home/zns/'+RUN+'/runner-build'

def cells():
    reference=json.loads((HERE/'cns-reference-plan.json').read_text())
    short=[c for c in reference['cells'] if c['group']=='short']
    if len(short)!=30:raise RuntimeError('CNS short 계획 수 불일치')
    result=[dict(c,index=i+1,backend='dm-zns',experiment='1-1',jobs=1,
                 total_write_gib=None,randseed=None) for i,c in enumerate(short)]
    return result

def paths(c):
    sub=f"{c['experiment']}/{c['filesystem']}/{c['workload']}-{c['bs']}/r{c['repeat']}"
    return '/home/zns/kafka_python_result/results/final/'+RUN+'/'+sub,m.CONTROL/'device'/sub

m.build_dir=lambda c:BUILD

def environment():
    code='''import pathlib,subprocess,json
p=pathlib.Path('/sys/module/dm_zns_base/parameters')
expected={'wal_batch_delay_ms':'0','data_append_max_blocks':'64','wal_group_pages':'8',
          'gc_reserved_zones':'8','gc_low_watermark':'16','gc_high_watermark':'20'}
actual={k:(p/k).read_text().strip() for k in expected}
assert actual==expected,actual
assert not any((p/k).exists() for k in ['wal_writeback','adaptive_gc','sstable_probe_cache'])
actual['logical_sectors']=subprocess.check_output(['sudo','-n','blockdev','--getsz','/dev/mapper/kafka-zns'],text=True).strip()
assert actual['logical_sectors']=='50331648'
print(json.dumps(actual))'''
    return json.loads(m.guest('python3 -c '+shlex.quote(code),timeout=20,retry=True).stdout)

def validate(folder,c):
    rows=json.loads((folder/'summary.json').read_text())
    if len(rows)!=1:raise RuntimeError('summary 수 불일치')
    r=rows[0];manifest=json.loads((folder/'manifest.json').read_text())
    for k,v in dict(backend='dm-zns',filesystem=c['filesystem'],experiment_id=c['experiment'],
                   file_gib=c['file_gib'],workloads=c['workload'],bs=c['bs'],qds='32',rounds=1,
                   warmup=c['warmup'],randseed=c['randseed']).items():
        if manifest.get(k)!=v:raise RuntimeError('manifest 불일치: '+k)
    tag=c['workload']+'-qd32-r1'
    raw=json.loads((folder/(tag+'.fio.json')).read_text());fio.parse_fio(raw)
    prep=json.loads((folder/'prepare.json').read_text());fio.parse_fio(prep)
    if any(prep['jobs'][0][k]['io_bytes']!=c['file_gib']*2**30 for k in ['read','write']):
        raise RuntimeError('준비 CRC 검증 바이트 불일치')
    if not r.get('kernel_log_available') or r.get('kernel_storage_errors') or not r.get('dm_available'):
        raise RuntimeError('커널/DM 계측 실패')
    kernel=(folder/(tag+'.kernel.log')).read_text(errors='replace')
    if re.search(r'insufficient relocation workspace|foreground allocation exhaust|metadata.*fail.closed|WAL append failed',kernel,re.I):
        raise RuntimeError('DM 진행성 오류')
    if manifest.get('measure')!=120 or raw['jobs'][0]['write']['runtime']<114000:
        raise RuntimeError('short runtime 미달')
    return r

def launch(c,out):
    cmd=p.command(c,'dm-zns',c['filesystem'],'/unused',m.MODULE,None)
    # CNS도 명시적 randseed 없이 fio 기본 randrepeat를 사용했다.
    pos=cmd.index('--randseed');del cmd[pos:pos+2]
    cmd[cmd.index('src/fio_evaluation.py')]=BUILD+'/runner/src/fio_evaluation.py'
    cmd[cmd.index('--output-dir')+1]=out
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join([
        'tmux','new-session','-d','-s',f"final-fio-{c['index']:02}",'bash','-lc',shell]))

def collect(c,out,host,boot):
    dest=m.CONTROL/'collected'/f"cell-{c['index']:02}"
    if not dest.exists():
        dest.parent.mkdir(parents=True,exist_ok=True)
        temp=Path(tempfile.mkdtemp(prefix='.collect-',dir=dest.parent))
        m.copy_from(out,temp/'result');m.copy_from(out+'.launch.log',temp/'result/launch.log')
        m.copy_from(out+'.exit.json',temp/'result/wrapper-exit.json')
        shutil.copy2(host/'zns.vZNSSD0.csv',temp/'result/femu-zns.csv')
        shutil.copy2(HERE/'build-manifest.json',temp/'result/build-manifest.json')
        (temp/'result').rename(dest)
    if json.loads((dest/'wrapper-exit.json').read_text())['exit_code']!=0:raise RuntimeError('fio wrapper 실패')
    r=validate(dest,c)
    m.save(dest/'environment-verified.json',environment())
    r['femu']={'zns':fio.femu_delta(dest/'femu-zns.csv',r['measurement_start_us_approx'],r['end_us'])}
    if not r['femu']['zns']['available']:raise RuntimeError('FEMU 구간 계측 부족')
    m.save(dest/'summary-collected.json',[r])
    return dict(cell=c,boot_id=boot,collected=str(dest),summary=r,
                module_sha256=m.MODULE_SHA,raw_sha256=hashlib.sha256((dest/(c['workload']+'-qd32-r1.fio.json')).read_bytes()).hexdigest())

def main():
    m.verify_local();m.CONTROL.mkdir(parents=True,exist_ok=True)
    lock=(m.CONTROL/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=dict(cells=cells(),build=META,physical_gib=32,logical_gib=24,zone_mib=512,
              gc_reserve_low_high=[8,16,20],fresh_femu_per_cell=True,policies='module defaults',
              scope='CNS short와 같은 1GiB/20초준비/120초측정/QD32/독립5회; NAND 모델 동등성은 별도',
              reference_plan_sha256=hashlib.sha256((HERE/'cns-reference-plan.json').read_bytes()).hexdigest())
    if (m.CONTROL/'plan.json').exists() and json.loads((m.CONTROL/'plan.json').read_text())!=plan:
        raise RuntimeError('재개 계획 불일치')
    m.save(m.CONTROL/'plan.json',plan)
    done=json.loads((m.CONTROL/'completed-cells.json').read_text()) if (m.CONTROL/'completed-cells.json').exists() else []
    if [d['cell'] for d in done]!=plan['cells'][:len(done)]:raise RuntimeError('완료 prefix 불일치')
    for d in done:
        folder=Path(d['collected']);validate(folder,d['cell'])
        if hashlib.sha256((folder/(d['cell']['workload']+'-qd32-r1.fio.json')).read_bytes()).hexdigest()!=d['raw_sha256']:
            raise RuntimeError('기존 결과 SHA 변경')
    try:
        for c in plan['cells'][len(done):]:
            out,host=paths(c)
            exists=m.guest('test -e '+shlex.quote(out),check=False,retry=True)
            if exists.returncode not in (0,1):raise RuntimeError('게스트 경로 조회 실패')
            if exists.returncode==1:
                m.status('booting',completed=len(done),total=30,cell=c)
                m.boot(c,host);m.deploy(c);launch(c,out)
            boot=m.guest('cat /proc/sys/kernel/random/boot_id',retry=True).stdout.strip()
            if boot in {d['boot_id'] for d in done}:raise RuntimeError('독립 boot_id 중복')
            deadline=time.monotonic()+c['file_gib']*600+140+900
            while True:
                code="from pathlib import Path; import json; p=Path("+repr(out+'.exit.json')+"); print(p.read_text() if p.exists() else '{}')"
                mark=json.loads(m.guest('python3 -c '+shlex.quote(code),retry=True).stdout)
                m.status('running',completed=len(done),total=30,cell=c,boot_id=boot)
                if mark:
                    if mark['exit_code']!=0:raise RuntimeError('fio 실패: 원본 보존')
                    break
                if time.monotonic()>deadline:raise RuntimeError('조건 제한 초과: 자동 종료/재초기화 안 함')
                time.sleep(60)
            record=collect(c,out,host,boot)
            done.append(record);m.save(m.CONTROL/'completed-cells.json',done)
            m.status('cell-completed',completed=len(done),total=30,cell=c)
        # 마지막 회차도 정상 정리. 호스트/타 팀은 건드리지 않는다.
        m.shutdown()
        m.status('completed',completed=len(done),total=30)
    except BaseException as exc:
        m.status('failed',completed=len(done),total=30,error=str(exc));raise

if __name__=='__main__':main()

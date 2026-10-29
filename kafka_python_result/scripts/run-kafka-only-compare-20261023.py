#!/usr/bin/env python3
"""team4 host: Kafka-only DM/CNS x EXT4/F2FS x 1만/2만, 8조건."""
import csv
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import shlex
import shutil
import tempfile
import time
import subprocess

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('base',HERE/'controller-base.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
RUN=os.environ.get('KAFKA_ONLY_RUN','kafka-only-compare-5c0cdbd-20261023-r3-diagnostic')
ROOT=Path('/home/zns-team4/results/final')/RUN
BUILD='/home/zns/'+RUN
META=json.loads((HERE/'build-manifest.json').read_text())
MODULE='/home/zns/fio-cns-matched-5c0cdbd-20261022/module-src/dm-zns-base.ko'
m.CONTROL=ROOT
m.MODULE=MODULE
m.MODULE_SHA=META['module_sha256']
m.PACKAGE=HERE
m.build_dir=lambda c:BUILD
m.source=lambda c:HERE/'dm-input'

def cells():
    if os.environ.get('KAFKA_ONLY_DIAGNOSTIC')=='1':
        return [dict(index=1,backend='dm-zns',filesystem='ext4',rate=10000)]
    plan = [dict(index=i+1,backend=backend,filesystem=fs,rate=rate)
            for i,(fs,rate,backend) in enumerate((fs,rate,backend)
            for fs in ['ext4','f2fs'] for rate in [10000,20000] for backend in ['dm-zns','cns'])]
    start=int(os.environ.get('KAFKA_ONLY_START_CELL','1'))
    if not 1 <= start <= 8:raise ValueError('시작 조건 범위 오류')
    return plan[start-1:]

def qemu():
    r=m.run(['pgrep','-a','-u','zns-team4','-f','[q]emu-system-x86_64'],check=False)
    if r.returncode==1:return []
    if r.returncode:raise RuntimeError('QEMU 조회 실패')
    lines=r.stdout.strip().splitlines()
    if len(lines)!=1 or not any('-name '+v+' ' in lines[0] for v in ['dm-zns-512m-team4','cns-team4']):
        raise RuntimeError('알 수 없는 team4 VM: 중단/재부팅 금지')
    return lines
m.qemu=qemu

def boot(c,host):
    m.shutdown()
    if host.exists():raise RuntimeError('기존 장치 결과 경로 보존: '+str(host))
    host.mkdir(parents=True)
    launcher=HERE/('run-dm-headless.sh' if c['backend']=='dm-zns' else 'run-cns-headless.sh')
    if hashlib.sha256(launcher.read_bytes()).hexdigest()!=META['launchers'][launcher.name]:
        raise RuntimeError('launcher SHA 변경')
    env=['env','FEMU_ZNS_STATS_PREFIX='+str(host/'zns'),'FEMU_STATS_FILE='+str(host/'cns.csv')]
    command=shlex.join([*env,'bash',str(launcher)])+' > '+shlex.quote(str(host/'console.log'))+' 2>&1; rc=$?; printf "%s\\n" "$rc" > '+shlex.quote(str(host/'exit-code.txt'))
    m.run(['tmux','new-session','-d','-s',f"{RUN}-femu-{c['index']:02}",'bash','-lc',command])
    for _ in range(80):
        try:
            r=m.guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=10,check=False)
            if r.returncode==0:break
        except subprocess.TimeoutExpired:
            pass
        time.sleep(3)
    else:raise RuntimeError('guest 부팅 실패; 강제 종료 안 함')
    expected=('host-managed','67108864') if c['backend']=='dm-zns' else ('none','50331648')
    code="from pathlib import Path; assert (Path('/sys/block/nvme0n1/queue/zoned').read_text().strip(),Path('/sys/block/nvme0n1/size').read_text().strip())=="+repr(expected)
    m.guest('python3 -c '+shlex.quote(code))

def launch(c,out):
    env=dict(DM_ZNS_MODULE_PATH=MODULE,BENCH_DM_METRICS='1' if c['backend']=='dm-zns' else '0',
             BENCH_FILESYSTEMS=c['filesystem'],BENCH_EXT4_DISCARD='0',BENCH_F2FS_DISCARD='0',
             DM_GC_RESERVED_ZONES='8',DM_GC_LOW_WATERMARK='16',DM_GC_HIGH_WATERMARK='20',
             DM_LOGICAL_CAPACITY_PERCENT='75')
    cmd=['env',*[f'{k}={v}' for k,v in env.items()], 'python3','-u',BUILD+'/runner/src/kafka_only_evaluation.py',
         '--backend',c['backend'],'--filesystem',c['filesystem'],'--scenario','write',
         '--rates',str(c['rate']),'--prepare-rate',str(c['rate']),'--prepare-timeout','10800',
         '--warmup','0','--measure','1800','--retention-gib','18',
         '--jar',BUILD+'/kafka-benchmark-1.0.jar','--module-provenance',BUILD+'/build-manifest.json',
         '--result-dir',out,'--execute']
    if os.environ.get('KAFKA_ONLY_DIAGNOSTIC')=='1':cmd += ['--diagnostic-rounds','5']
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join([
        'tmux','new-session','-d','-s',f"{RUN}-cell-{c['index']:02}",'bash','-lc',shell]))

def validate(folder,c):
    if os.environ.get('KAFKA_ONLY_DIAGNOSTIC')=='1':
        value=json.loads((folder/'diagnostic-completed.json').read_text())
        if not value.get('valid') or value.get('rounds')!=5:raise RuntimeError('진단 미완료')
        return value
    manifest=json.loads((folder/'manifest.json').read_text());args=manifest['arguments']
    expected=dict(backend=c['backend'],filesystem=c['filesystem'],scenario='write',rates=str(c['rate']),
                  prepare_rate=c['rate'],warmup=0,measure=1800,retention_gib=18)
    if any(args.get(k)!=v for k,v in expected.items()):raise RuntimeError('실측 설정 불일치')
    if not json.loads((folder/'readiness.json').read_text()).get('ready'):raise RuntimeError('준비 조건 미완료')
    result=json.loads((folder/f"result-{c['rate']}.json").read_text())
    if not result['validity']['valid']:raise RuntimeError('부하기 정합성 오류: '+str(result['validity']))
    for key in ['failed_requests','unresolved_after_drain','send_errors','payload_crc_errors']:
        if result['metrics'].get(key,0):raise RuntimeError('요청/정합성 오류 '+key)
    # 달성률 부족은 삭제/중단 사유가 아닌 이 구성의 포화 결과다.
    if any('목표 달성률' not in v for v in result['assessment']['reasons']):
        raise RuntimeError('안전/환경 오류 '+str(result['assessment']))
    return dict(metrics=result['metrics'],assessment=result['assessment'])

def collect(c,out,host,boot_id):
    dest=ROOT/'collected'/c['backend']/c['filesystem']/str(c['rate'])
    dest.parent.mkdir(parents=True,exist_ok=True)
    temp=Path(tempfile.mkdtemp(prefix='.collect-',dir=dest.parent))
    m.copy_from(out,temp/'result');m.copy_from(out+'.launch.log',temp/'result/launch.log')
    m.copy_from(out+'.exit.json',temp/'result/wrapper-exit.json')
    stat=host/('zns.vZNSSD0.csv' if c['backend']=='dm-zns' else 'cns.csv')
    shutil.copy2(stat,temp/'result/femu.csv');shutil.copy2(HERE/'build-manifest.json',temp/'result/build-manifest.json')
    (temp/'result').rename(dest)
    if json.loads((dest/'wrapper-exit.json').read_text())['exit_code']!=0:
        raise RuntimeError('실험 오류: 원본 수집/보존 완료')
    value=validate(dest,c)
    return dict(cell=c,boot_id=boot_id,collected=str(dest),result=value)

def main():
    if pwd.getpwuid(os.getuid()).pw_name!='zns-team4' or os.uname().nodename!='splab':
        raise RuntimeError('team4 host 전용')
    if hashlib.sha256(m.FEMU.read_bytes()).hexdigest()!=m.FEMU_SHA:raise RuntimeError('FEMU SHA 변경')
    for name,sha in META['input_hashes'].items():
        if hashlib.sha256((HERE/'dm-input'/name).read_bytes()).hexdigest()!=sha:raise RuntimeError('입력 SHA 변경')
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=dict(cells=cells(),build=META,measure=1800,retention_gib=18,ready_window=120,
              occupancy_band=[75,85],max_swing_pct=3,all_partitions=8,minimum_achieved_pct=90,
              scope='같은 Kafka 부하; CNS/ZNS 매체 모델은 서로 다름')
    if (ROOT/'plan.json').exists() and json.loads((ROOT/'plan.json').read_text())!=plan:raise RuntimeError('계획 변경')
    m.save(ROOT/'plan.json',plan)
    done=json.loads((ROOT/'completed-cells.json').read_text()) if (ROOT/'completed-cells.json').exists() else []
    if [d['cell'] for d in done]!=plan['cells'][:len(done)]:raise RuntimeError('완료 prefix 불일치')
    for d in done:validate(Path(d['collected']),d['cell'])
    try:
        for c in plan['cells'][len(done):]:
            sub=f"{c['backend']}/{c['filesystem']}/{c['rate']}"
            out='/home/zns/kafka_python_result/results/final/'+RUN+'/'+sub;host=ROOT/'device'/sub
            m.status('booting',completed=len(done),total=len(plan['cells']),cell=c)
            boot(c,host);m.deploy(c)
            m.copy_to(HERE/'build-manifest.json',BUILD+'/build-manifest.json')
            boot_id=m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if boot_id in {d['boot_id'] for d in done}:raise RuntimeError('독립 boot 중복')
            launch(c,out);deadline=time.monotonic()+5*3600
            while True:
                code="from pathlib import Path; p=Path("+repr(out+'.exit.json')+"); print(p.read_text() if p.exists() else '{}')"
                mark=json.loads(m.guest('python3 -c '+shlex.quote(code),retry=True).stdout)
                m.status('running',completed=len(done),total=len(plan['cells']),cell=c,boot_id=boot_id)
                if mark:break
                if time.monotonic()>deadline:raise RuntimeError('조건 제한 초과; 원격 부하 보존')
                time.sleep(60)
            record=collect(c,out,host,boot_id);done.append(record)
            m.save(ROOT/'completed-cells.json',done)
            m.status('cell-completed',completed=len(done),total=len(plan['cells']),cell=c)
        m.shutdown();m.status('completed',completed=len(done),total=len(plan['cells']))
    except BaseException as exc:
        m.status('failed',completed=len(done),total=len(plan['cells']),error=str(exc));raise

if __name__=='__main__':main()

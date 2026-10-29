#!/usr/bin/env python3
"""team4 host 전용: 최종 커밋 EXT4/F2FS 원래 benchmark4 조건 검증."""
import importlib.util
import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import shutil
import os

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('controller', HERE/'controller-base.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
m.PACKAGE = HERE
RUN_ID = os.environ.get('BENCH4_RUN_ID', 'benchmark4-final-20261022')
if RUN_ID not in ('benchmark4-final-20261022','benchmark4-final-20261022-r2'):
    raise RuntimeError('허용되지 않은 실행 경로')
m.CONTROL = Path('/home/zns-team4/results/final')/RUN_ID
m.MODULE = '/home/zns/benchmark4-final-20261022/module-src/dm-zns-base.ko'
m.MODULE_SHA = json.loads((HERE/'build-manifest.json').read_text())['module_sha256']
if RUN_ID.endswith('-r2'):
    safe_launcher=HERE/'run-femu-headless.sh'
    m.LAUNCHERS['dm-zns']=(str(safe_launcher),hashlib.sha256(safe_launcher.read_bytes()).hexdigest())


def cells():
    return [dict(index=i+1,backend='dm-zns',scenario='read-write',filesystem=fs,rate=20000)
            for i,fs in enumerate(['ext4','f2fs'])]


def paths(cell):
    sub=cell['filesystem']
    return ('/home/zns/kafka_python_result/results/final/'+RUN_ID+'/'+sub,
            Path('/home/zns-team4/results/final')/(RUN_ID+'-device')/sub)


def build_dir(cell):
    return '/home/zns/benchmark4-final-runner-20261022'


def launch(cell,out):
    build=build_dir(cell)
    env=dict(BENCH_JAVA_JAR=build+'/kafka-benchmark-1.0.jar',
             JAVA_TOOL_OPTIONS='-Dbenchmark.explicitPartitions=8',
             DM_ZNS_MODULE_PATH=m.MODULE,BENCH_DM_METRICS='1',BENCH_WAL_DELAY_MS='0',
             DM_GC_RESERVED_ZONES='2',DM_GC_LOW_WATERMARK='4',DM_GC_HIGH_WATERMARK='5',
             DM_LOGICAL_CAPACITY_PERCENT='75',BENCH_EXT4_DISCARD='0',BENCH_F2FS_DISCARD='0',
             BENCH_ENDURANCE_RETENTION_TOTAL_BYTES=str(2*1024**3),
             BENCH_RETENTION_SEGMENT_BYTES=str(128*1024**2),
             BENCH_RETENTION_SEGMENT_MS='60000',BENCH_INTEGRITY_DIAG='1')
    cmd=['env',*[f'{k}={v}' for k,v in env.items()], 'python3','-u',
         build+'/runner/src/performance_evaluation.py','--single-rate',
         '--backend','dm-zns','--filesystem',cell['filesystem'],'--scenario','read-write',
         '--workload','prefill','--rate','20000','--rounds','1','--warmup','300',
         '--measure','3600','--occupancy','80','--stall-limit','60','--run-dir',out]
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+" 2>&1; rc=$?; printf '{\"exit_code\":%s}\\n' \"$rc\" > "+shlex.quote(out+'.exit.json')
    m.guest('mkdir -p '+shlex.quote(str(Path(out).parent))+' && '+shlex.join([
        'tmux','new-session','-d','-s',f"keper-gc-host-cell-{cell['index']:02}",'bash','-lc',shell]))


def validate(folder,cell):
    import csv
    rows=list(csv.DictReader((folder/'csv/full_results.csv').open()))
    if len(rows)!=1:raise RuntimeError('결과 행 수 불일치')
    r=rows[0]
    expected=dict(filesystem=cell['filesystem'],scenario='scenario_b',target_ops='20000',
                  measure_sec='3600',warmup_sec='300',record_size='1024',producers='8',
                  dynamic_topics='False',occupancy_target_pct='80')
    if any(r.get(k)!=v for k,v in expected.items()):raise RuntimeError('benchmark4 조건 불일치')
    if r.get('valid')!='True':raise RuntimeError('benchmark4 validity 실패: '+r.get('invalid_reasons',''))
    for k in ['failed_requests','unresolved_after_drain','send_errors','consumer_record_delta',
              'malformed_headers','payload_crc_errors','sequence_gaps','duplicate_records','out_of_order_records']:
        if int(float(r.get(k) or 0))!=0:raise RuntimeError('정합성/요청 오류: '+k)
    if r.get('drain_completed')!='True' or r.get('consumer_drain_completed')!='True':
        raise RuntimeError('drain 미완료')
    for p in folder.rglob('*kernel*.log'):
        import re
        if re.search(r'I/O error|journal.*abort|read-only filesystem|BUG:|Oops:|insufficient relocation workspace',p.read_text(errors='replace'),re.I):
            raise RuntimeError('커널 오류: '+str(p))
    return dict(status='passed',achieved_ops=r.get('achieved_ops'),achieved_pct=r.get('achieved_pct'),
                p99_ms=r.get('app_p99_ms'),scope='benchmark4 한 회; 독립 반복 미완료')


def collect(cell,out,host,boot_id):
    parent=m.CONTROL/'collected';parent.mkdir(parents=True,exist_ok=True)
    final=parent/cell['filesystem']
    if not final.exists():
        temp=Path(tempfile.mkdtemp(prefix='.collect-',dir=parent))
        m.copy_from(out,temp/'result')
        m.copy_from(out+'.launch.log',temp/'result/launch.log')
        m.copy_from(out+'.exit.json',temp/'result/wrapper-exit.json')
        shutil.copy2(host/'zns.vZNSSD0.csv',temp/'result/zns.vZNSSD0.csv')
        shutil.copy2(HERE/'build-manifest.json',temp/'result/build-manifest.json')
        # 실패 결과도 호스트에 먼저 보존한다.
        (temp/'result').rename(final)
    if json.loads((final/'wrapper-exit.json').read_text())['exit_code']!=0:
        raise RuntimeError('wrapper 실패; 원본 보존')
    result=validate(final,cell)
    return dict(cell=cell,boot_id=boot_id,result=result,collected=str(final),guest_output=out,
                csv_sha256=hashlib.sha256((final/'csv/full_results.csv').read_bytes()).hexdigest())


m.cells=cells
m.paths=paths
m.build_dir=build_dir
m.measurement_seconds=lambda cell:3600
m.launch=launch
m.validate=validate
m.collect=collect
# FEMU stdin을 tmux에서 분리한다. 게스트 SSH를 통한 정상 poweroff는 유지한다.
original_run=m.run
def isolated_run(command,*args,**kwargs):
    if command[:2]==['tmux','new-session'] and any(str(x).startswith('keper-gc-femu-') for x in command):
        command=list(command)
        command[-1]='exec 0</dev/null; '+command[-1]
    return original_run(command,*args,**kwargs)
m.run=isolated_run
if __name__=='__main__':
    m.main()

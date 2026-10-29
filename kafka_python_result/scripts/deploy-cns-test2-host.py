"""기존 FEMU/2번 JAR를 고정해 CNS 2번 장기 압력을 team4 tmux에서 실행."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess

BASE=Path(__file__).resolve().parents[1]
s=importlib.util.spec_from_file_location('connection',BASE/'scripts/run-fio-cns-controller.py')
c=importlib.util.module_from_spec(s);s.loader.exec_module(c)
LOCAL=BASE/'results/final/cns-test2-input-20261027'
REMOTE='/home/zns-team4/benchmark-control/cns-test2-prefill-long-20261027'
FEMU='/home/zns-team4/femu/src/build-femu/qemu-system-x86_64'
JAR='/home/zns-team4/benchmark-control/keper-long-gc-20261022/dm-input/kafka-benchmark-1.0.jar'
def host(cmd):return c.call('host',cmd,timeout=30).stdout
def upload(src,dest):
    opts=c.RELAY[:-1].copy();opts[opts.index('-p')]='-P'
    subprocess.run(['scp','-r',*opts,str(src),c.RELAY[-1]+':'+dest],check=True,timeout=60)
if __name__=='__main__':
    assert host('sha256sum '+FEMU).split()[0]=='ae27d095f0fe527ae4fc8488590d1ec10279f38cee3db6fc376ae8a02eec0246'
    jar_sha=host('sha256sum '+JAR).split()[0]
    assert jar_sha=='e6e2f68fd90219e6bf5ded609c9e8a15343965e70af161acf1082fdd32fdbea1'
    hashes={str(p.relative_to(LOCAL)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((LOCAL/'src').glob('*.py'))}
    hashes['kafka-benchmark-1.0.jar']=jar_sha
    meta=dict(femu_sha256='ae27d095f0fe527ae4fc8488590d1ec10279f38cee3db6fc376ae8a02eec0246',
        jar_sha256=jar_sha,input_hashes=hashes,reference='keper test2 long-GC 30min / 기존 JVM retry=0 정책 유지',
        scope='CNS 24GiB/EXT4/F2FS; 원래 FEMU, 정규화 프로파일 미사용')
    (LOCAL/'build-manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2)+'\n')
    host('test ! -e '+REMOTE+' && mkdir -p '+REMOTE+'/input && cp '+JAR+' '+REMOTE+'/input/kafka-benchmark-1.0.jar')
    upload(LOCAL/'src',REMOTE+'/input/')
    upload(LOCAL/'build-manifest.json',REMOTE+'/build-manifest.json')
    upload(BASE/'scripts/run-long-gc-on-femu-host.py',REMOTE+'/controller-base.py')
    upload(BASE/'scripts/run-cns-test2-host.py',REMOTE+'/controller.py')
    host('python3 -m py_compile '+REMOTE+'/controller.py '+REMOTE+'/input/src/cns_test2_entry.py')
    host('tmux new-session -d -s cns-test2-prefill-long-20261027 '+shlex.quote('python3 -u '+REMOTE+'/controller.py > '+REMOTE+'/controller.log 2>&1'))
    print('시작 요청 완료:',REMOTE)

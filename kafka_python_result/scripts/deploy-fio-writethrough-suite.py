"""고정 입력/해시와 함께 75 short 비교를 team4 host tmux에 배포한다."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess

BASE=Path(__file__).resolve().parents[1]
s=importlib.util.spec_from_file_location('connection',BASE/'scripts/run-fio-cns-controller.py')
c=importlib.util.module_from_spec(s);s.loader.exec_module(c)
LOCAL=BASE/'results/final/fio-writethrough-input-20261026'
REMOTE='/home/zns-team4/benchmark-control/fio-writethrough-20261026'
SOURCE='/home/zns-team4/femu/common-nand-20261026'
FEMU_SHA='92a0e7905920a0c4c24ee11334f0b0ebc3edf0b52cf807950601fe133f51c84e'
MODULE_SHA='41044c3ce7f15658c29bddc7c7e0f0e05d3eb852f5e996b2c06d21f6d037d02e'

def host(cmd):return c.call('host',cmd,timeout=30).stdout
def upload(src,dest):
    opts=c.RELAY[:-1].copy();opts[opts.index('-p')]='-P'
    subprocess.run(['scp','-r',*opts,str(src),c.RELAY[-1]+':'+dest],check=True,timeout=60)

if __name__=='__main__':
    actual=host('sha256sum '+SOURCE+'/build-align/qemu-system-x86_64').split()[0]
    if actual!=FEMU_SHA:raise RuntimeError('검증한 FEMU 바이너리와 다름')
    hashes={str(p.relative_to(LOCAL)):hashlib.sha256(p.read_bytes()).hexdigest()
            for folder,glob in [('src','*.py'),('scripts','*')]
            for p in sorted((LOCAL/folder).glob(glob)) if p.is_file()}
    manifest=dict(femu_sha256=actual,module_sha256=MODULE_SHA,dm_commit='5c0cdbd',
        module_source_sha256='8d4745d4dd5fd89111ff486f79f5b1934995a95e0caad3a9868dfcc88dde3f63',
        input_hashes=hashes,profile='4k-lun-wt-v2',scope='short75 독립 반복; 정규화 cacheless 기준선')
    code="from pathlib import Path; import subprocess,hashlib,json; p=Path("+repr(SOURCE)+"); print(json.dumps(dict(commit=subprocess.check_output(['git','-C',str(p),'rev-parse','HEAD'],text=True).strip(),dirty_diff_sha256=hashlib.sha256(subprocess.check_output(['git','-C',str(p),'diff','HEAD','--binary'])).hexdigest(),files={str(f.relative_to(p)):hashlib.sha256(f.read_bytes()).hexdigest() for f in [p/n for n in ['hw/femu/femu.c','hw/femu/nvme-admin.c','hw/femu/nand/nand-media.c','hw/femu/nand/nand-media.h','hw/femu/bbssd/ftl-media.c','hw/femu/zns/zftl.c','hw/femu/zns/zns.h','hw/femu/zns/normalized-map.h']]})))"
    manifest['femu_source']=json.loads(host('python3 -c '+shlex.quote(code)))
    (LOCAL/'build-manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    host('test ! -e '+REMOTE+' && mkdir -p '+REMOTE+'/input')
    for name in ['src','scripts']:upload(LOCAL/name,REMOTE+'/input/')
    upload(LOCAL/'build-manifest.json',REMOTE+'/build-manifest.json')
    upload(BASE/'scripts/run-long-gc-on-femu-host.py',REMOTE+'/controller-base.py')
    upload(BASE/'scripts/run-fio-writethrough-suite.py',REMOTE+'/controller.py')
    host('python3 -m py_compile '+REMOTE+'/controller.py '+REMOTE+'/input/src/fio_profile_entry.py')
    host('tmux new-session -d -s fio-writethrough-20261026 '+shlex.quote('python3 -u '+REMOTE+'/controller.py > '+REMOTE+'/controller.log 2>&1'))
    print('실행 요청 완료:',REMOTE)

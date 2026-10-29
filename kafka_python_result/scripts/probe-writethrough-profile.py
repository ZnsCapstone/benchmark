"""게스트의 새 FEMU 장치에서 cache/FLUSH/FUA/Append/Reset 가시성 진단."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

def run(args, success=True):
    r=subprocess.run(args,capture_output=True,text=True,timeout=60)
    if success and r.returncode:raise RuntimeError(str(args)+': '+r.stdout+r.stderr)
    return r

def main():
    if os.uname().nodename!='zns-guest':raise RuntimeError('guest 전용')
    dev='/dev/nvme0n1'
    assert Path('/sys/block/nvme0n1/size').read_text().strip()=='67108864'
    assert 'FEMU' in run(['lsblk','-dn','-o','MODEL',dev]).stdout
    assert run(['findmnt','-rn','-S',dev],False).returncode==1
    assert not list(Path('/sys/block/nvme0n1/holders').iterdir())
    zoned=Path('/sys/block/nvme0n1/queue/zoned').read_text().strip()=='host-managed'
    ctrl=json.loads(run(['sudo','-n','nvme','id-ctrl',dev,'-o','json']).stdout)
    assert ctrl['vwc']==0,ctrl['vwc']
    cache=Path('/sys/block/nvme0n1/queue/write_cache').read_text().strip()
    assert cache=='write through',cache
    enable=run(['sudo','-n','nvme','set-feature',dev,'-f','6','-v','1'],False)
    assert enable.returncode!=0,'write-back cache를 재활성화하면 안 됨'
    folder=Path(tempfile.mkdtemp(prefix='array-wt-probe-'))
    first=folder/'first';second=folder/'second';read=folder/'read'
    first.write_bytes(b'A'*4096);second.write_bytes(b'B'*4096)
    if zoned:run(['sudo','-n','blkzone','reset','-o','0','-l','1048576',dev])
    def write(path,lba,fua=False):
        cmd=['sudo','-n','nvme','write',dev,'--start-block='+str(lba),'--block-count=7','--data-size=4096','--data='+str(path)]
        if fua:cmd+=['--force-unit-access']
        return run(cmd)
    write(first,0)
    # Verify the ordinary write immediately after its completion, before Flush.
    run(['sudo','-n','nvme','read',dev,'--start-block=0','--block-count=7','--data-size=4096','--data='+str(read)])
    assert read.read_bytes()==first.read_bytes()
    append_result=None
    if zoned:
        # Zone Append + FUA (NLB=7, control.FUA=1) uses the real ZNS command path.
        append=run(['sudo','-n','nvme','io-passthru',dev,'--opcode=0x7d','--namespace-id=1',
            '--data-len=4096','--write','--input-file='+str(second),'--cdw10=0','--cdw11=0','--cdw12='+str(7|(1<<30))])
        append_result=append.stdout+append.stderr
        rejected=run(['sudo','-n','nvme','write',dev,'--start-block=0','--block-count=7','--data-size=4096','--data='+str(first)],False)
        assert rejected.returncode!=0,'ZNS 순차 쓰기 위반이 거절되지 않음'
    else:write(second,8,True)
    run(['sudo','-n','nvme','flush',dev])
    read=folder/'read-after-flush'
    run(['sudo','-n','nvme','read',dev,'--start-block=0','--block-count=15','--data-size=8192','--data='+str(read)])
    assert read.read_bytes()==first.read_bytes()+second.read_bytes(),'FUA/Flush 뒤 데이터 불일치'
    if zoned:
        run(['sudo','-n','blkzone','reset','-o','0','-l','1048576',dev])
        write(second,0,True)
        read=folder/'read-after-reset'
        run(['sudo','-n','nvme','read',dev,'--start-block=0','--block-count=7','--data-size=4096','--data='+str(read)])
        assert read.read_bytes()==second.read_bytes(),'reset 뒤 재쓰기 불일치'
        run(['sudo','-n','blkzone','reset','-o','0','-l','1048576',dev])
    result=dict(passed=True,zoned=zoned,vwc=ctrl['vwc'],write_cache=cache,
        ordinary_write_readback=True,fua_readback=True,flush=True,
        sequential_write_rejection=zoned,append_fua=zoned,reset_rewrite=zoned,
        append_result=append_result,scope='RAM backend의 명령/가시성 검증; 실제 정전 내구성 검증 아님')
    print(json.dumps(result,ensure_ascii=False))

if __name__=='__main__':main()

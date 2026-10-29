"""team4 host 전용, 새 raw FEMU 장치의 1MiB 순차 쓰기 256MiB 진단."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('base',HERE/'controller-base.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
ROOT=Path('/home/zns-team4/results/final/writethrough-profile-smoke-20261026-r2')
BIN=Path('/home/zns-team4/femu/common-nand-20261026/build-align/qemu-system-x86_64')
m.CONTROL=ROOT

def qemu():
    r=m.run(['pgrep','-a','-u','zns-team4','-f','[q]emu-system-x86_64'],check=False)
    if r.returncode==1:return []
    if r.returncode:raise RuntimeError('VM 조회 실패')
    lines=r.stdout.strip().splitlines()
    if len(lines)!=1 or not any('-name '+v+' ' in lines[0] for v in ['native-f2fs-team4','array-smoke-team4']):
        raise RuntimeError('미확인 VM: 종료 금지')
    return lines
m.qemu=qemu

def main():
    if os.uname().nodename!='splab' or os.environ.get('USER')!='zns-team4':raise RuntimeError('team4 host 전용')
    if ROOT.exists():raise RuntimeError('기존 결과 보존')
    ROOT.mkdir(parents=True)
    binary_sha=hashlib.sha256(BIN.read_bytes()).hexdigest()
    source=BIN.parents[1]
    source_hashes={name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in
        ['hw/femu/femu.c','hw/femu/nvme-admin.c','hw/femu/bbssd/ftl-media.c',
         'hw/femu/nand/nand-media.c','hw/femu/nand/nand-media.h',
         'hw/femu/zns/zftl.c','hw/femu/zns/zns.h','hw/femu/zns/normalized-map.h']}
    m.save(ROOT/'method.json',dict(binary=str(BIN),binary_sha256=binary_sha,
        source_hashes=source_hashes,source_commit=m.run(['git','-C',str(source),'rev-parse','HEAD']).stdout.strip(),
        scope='raw-device cacheless write-through 기준선; FS/DM 및 실제 정전 내구성 비교 아님',
        bs='1MiB',write_bytes=256*1024**2,qd=32,jobs=1,warmup=0,
        termination='고정 쓰기량; fio runtime 120초 안전 한계',
        cases=['cns-wt','zns-wt','cns-4k-wt','zns-4k-wt']))
    rows=[]
    try:
        for label in ['cns-wt','zns-wt','cns-4k-wt','zns-4k-wt']:
            m.status('booting',case=label,completed=len(rows),total=4)
            m.shutdown()
            dest=ROOT/label;dest.mkdir()
            cns=label.startswith('cns')
            dev=('femu,devsz_mb=32768,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=1024,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95' if cns else
                 'femu,devsz_mb=32768,namespaces=1,zns_num_ch=8,zns_num_lun=4,zns_num_plane=2,zns_num_blk=128,zns_flash_type=4,femu_mode=3')
            env=dict(os.environ,FEMU_STATS_FILE=str(dest/'cns.csv'),FEMU_ZNS_STATS_PREFIX=str(dest/'zns'))
            env.pop('FEMU_MATCHED_ARRAY',None)
            env['FEMU_MATCHED_ARRAY']='4k-lun-wt-v2'
            command=[str(BIN),'-name','array-smoke-team4','-enable-kvm','-cpu','host','-smp','8','-m','16G',
                '-device','virtio-scsi-pci,id=scsi0','-device','scsi-hd,drive=hd0',
                '-drive','file=/home/zns-team4/zns-team4.qcow2,if=none,aio=native,cache=none,format=qcow2,id=hd0',
                '-device',dev,'-netdev','user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22',
                '-device','virtio-net-pci,netdev=net0','-display','none','-monitor','none','-serial','file:'+str(dest/'serial.log')]
            output=(dest/'qemu.log').open('w')
            process=subprocess.Popen(command,env=env,stdout=output,stderr=subprocess.STDOUT)
            for _ in range(70):
                if process.poll() is not None:raise RuntimeError('FEMU 조기 종료: '+str(process.returncode))
                try:
                    r=m.guest('test "$(hostname)" = zns-guest && sudo -n true',timeout=8,check=False)
                    if r.returncode==0:break
                except subprocess.TimeoutExpired:pass
                time.sleep(2)
            else:raise RuntimeError('게스트 부팅 제한 초과')
            m.active_guard()
            # 새로운 RAM-backed FEMU NVMe만 허용. OS 디스크와 기존 mounted device 제외.
            code="from pathlib import Path; import subprocess; assert Path('/sys/block/nvme0n1/size').read_text().strip()=='67108864'; assert Path('/sys/block/nvme0n1/queue/zoned').read_text().strip()=="+repr('none' if cns else 'host-managed')+"; assert 'FEMU' in subprocess.check_output(['lsblk','-dn','-o','MODEL','/dev/nvme0n1'],text=True); assert not list(Path('/sys/block/nvme0n1/holders').iterdir()); assert subprocess.run(['findmnt','-rn','-S','/dev/nvme0n1'],capture_output=True).returncode==1"
            m.guest('python3 -c '+shlex.quote(code))
            m.copy_to(HERE/'probe-writethrough-profile.py','/home/zns/array-wt-probe-20261026.py')
            probe=m.guest('python3 /home/zns/array-wt-probe-20261026.py',timeout=180,check=False)
            (dest/'probe.stdout').write_text(probe.stdout);(dest/'probe.stderr').write_text(probe.stderr)
            if probe.returncode:raise RuntimeError('write-through/Zone 명령 검증 실패')
            m.save(dest/'probe.json',json.loads(probe.stdout))
            fio=['sudo','-n','fio','--name=array-smoke','--filename=/dev/nvme0n1','--rw=write',
                '--bs=1M','--ioengine=libaio','--direct=1','--iodepth=32','--numjobs=1',
                '--size=1G','--io_size=256M','--runtime=120','--group_reporting=1','--output-format=json']
            if not cns:fio+=['--zonemode=zbd']
            expected_bytes=256*1024**2
            if '-4k-' in label:
                fio=[s.replace('--bs=1M','--bs=4k').replace('--io_size=256M','--io_size=8M') for s in fio]
                expected_bytes=8*1024**2
            m.save(dest/'command.json',dict(argv=fio,profile=env.get('FEMU_MATCHED_ARRAY'),boot_id=m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()))
            m.status('fio-running',case=label,completed=len(rows),total=4)
            r=m.guest(shlex.join(fio),timeout=180,check=False)
            (dest/'fio.stdout').write_text(r.stdout);(dest/'fio.stderr').write_text(r.stderr)
            if r.returncode:raise RuntimeError('fio 실패 '+str(r.returncode))
            data=json.loads(r.stdout);job=data['jobs'][0]
            if job['error'] or job['write']['io_bytes']!=expected_bytes:raise RuntimeError('fio 오류/쓰여진 양 불일치')
            wr=job['write'];pct=wr['clat_ns']['percentile']
            row=dict(case=label,iops=wr['iops'],mib_s=wr['bw_bytes']/1024**2,
                p95_ms=pct['95.000000']/1e6,p99_ms=pct['99.000000']/1e6,write_bytes=wr['io_bytes'])
            kernel=m.guest('sudo -n dmesg').stdout
            (dest/'kernel.log').write_text(kernel)
            errors=[line for line in kernel.splitlines() if 'fd0' not in line and any(s in line.lower() for s in
                ['i/o error','timeout, aborting','resetting controller','kernel panic'])]
            row['kernel_errors']=errors
            rows.append(row);m.save(ROOT/'results.json',rows)
            if errors:raise RuntimeError('커널 저장장치 오류: 결과 비교 금지')
            m.shutdown();process.wait(timeout=15);output.close()
        m.status('completed',completed=4,total=4)
    except BaseException as error:
        m.status('failed',completed=len(rows),total=4,error=str(error))
        raise

if __name__=='__main__':main()

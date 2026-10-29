#!/usr/bin/env python3
"""준비된 native mount에서 기존 2번 Kafka 부하기만 재사용. DM 초기화 금지."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent/'src'))
import performance_evaluation as pe

USER_BYTES=24*1024**3
MOUNT='/result/native-f2fs'

def logical_usage(stats):
    available=stats.f_bavail*stats.f_frsize
    if not 0<=available<=USER_BYTES:raise RuntimeError('Native 사용자 가용 공간 불일치')
    used=USER_BYTES-available
    return dict(total_bytes=USER_BYTES,usable_bytes=USER_BYTES,used_bytes=used,
                available_bytes=available,used_percent=100*used/USER_BYTES,
                statfs_total_bytes=stats.f_blocks*stats.f_frsize,
                statfs_free_bytes=stats.f_bfree*stats.f_frsize)

def assert_native():
    if os.uname().nodename!='zns-guest' or Path('/sys/module/dm_zns_base').exists():
        raise RuntimeError('게스트/DM 부재 검사 실패')
    mount=subprocess.check_output(['findmnt','-n','-o','SOURCE,FSTYPE,TARGET,OPTIONS',MOUNT],text=True).split()
    if mount[:3]!=['/dev/nvme0n1','f2fs',MOUNT] or not {'mode=lfs','discard'}.issubset(set(mount[3].split(','))):
        raise RuntimeError('Native mount 불일치')
    for path,value in [('nvme0n1/size','2097152'),('nvme1n1/size','67108864'),
                       ('nvme0n1/queue/zoned','none'),('nvme1n1/queue/zoned','host-managed'),
                       ('nvme1n1/queue/chunk_sectors','1048576')]:
        if Path('/sys/block',path).read_text().strip()!=value:raise RuntimeError('장치 geometry 불일치')
    return mount

def install_native_hooks(cfg,run_dir):
    import bench_runner as runner
    import system_setup as setup
    cfg.MOUNT_POINT=MOUNT;cfg.FS_DEVICE='/dev/nvme0n1'
    cfg.RAW_ZNS_DEVICE='/dev/nvme1n1';cfg.RAW_DEVICE_BASENAME='nvme1n1'
    cfg.PREFILL_FILE=MOUNT+'/.benchmark-prefill'
    cfg.DM_IMPLEMENTATION_LABELS['dynamic']='native F2FS multi-device (DM 없음)'
    def prepared(fs):
        if fs!='f2fs':raise RuntimeError('Native는 F2FS만 지원')
        assert_native()
    def no_dm(strict=True):
        if Path('/sys/module/dm_zns_base').exists():raise RuntimeError('예상치 못한 DM 적재')
    def usage():return logical_usage(os.statvfs(MOUNT))
    def capture():
        manifest=dict(backend='native-f2fs-zns',mount=assert_native(),user_bytes=USER_BYTES,
            initial_usage=usage(),occupancy_denominator='CP user_block_count 24GiB; reserved 제외',
            io_labels=dict(raw='ZNS nvme1n1',mapper='보조 CNS nvme0n1; DM 아님'),
            jar_sha256=hashlib.sha256(Path(cfg.JAVA_BENCH_JAR).read_bytes()).hexdigest(),
            auxiliary_cns_gib=1,discard=True,kernel=os.uname().release)
        (Path(cfg.ENV_DIR)/'native-environment.json').write_text(json.dumps(manifest,indent=2))
    runner.setup_filesystem=prepared
    runner.remove_dm_stack=no_dm
    setup.remove_dm_stack=no_dm
    setup.filesystem_usage=usage;runner.filesystem_usage=usage
    runner.capture_environment=capture

def main():
    original=pe.configure_result_paths
    def configure(cfg,run_dir):
        original(cfg,run_dir);install_native_hooks(cfg,run_dir)
    pe.configure_result_paths=configure
    return pe.main()

if __name__=='__main__':sys.exit(main())

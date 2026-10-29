#!/usr/bin/env bash
# team4 CNS: 기존 run-cns.sh의 장치 모델 그대로, 입력 콘솔만 파일로 분리.
set -euo pipefail
[[ $(id -un) == zns-team4 ]]
if pgrep -u zns-team4 -f '[q]emu-system-x86_64' >/dev/null || ss -ltn 'sport = :2204' | grep -q ':2204'; then
  echo 'team4 VM 또는 2204 사용 중' >&2; exit 1
fi
exec /home/zns-team4/femu/src/build-femu/qemu-system-x86_64 \
 -name cns-team4 -enable-kvm -cpu host -smp 8 -m 16G \
 -device virtio-scsi-pci,id=scsi0 -device scsi-hd,drive=hd0 \
 -drive file=/home/zns-team4/zns-team4.qcow2,if=none,aio=native,cache=none,format=qcow2,id=hd0 \
 -device femu,devsz_mb=24576,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=1024,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95 \
 -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22 -device virtio-net-pci,netdev=net0 \
 -display none -monitor none -serial "file:${FEMU_STATS_FILE}.serial.log"

#!/usr/bin/env bash
# team4 전용: F2FS 메타데이터 CNS 1GiB + 512MiB-zone ZNS 32GiB.
# 호스트에서 실행. 기존 실행기/소스는 변경하지 않는다.
set -euo pipefail
FEMU_BIN=${FEMU_BIN:-/home/zns-team4/femu/src/build-femu/qemu-system-x86_64}
IMG=/home/zns-team4/zns-team4.qcow2
# 시작 정렬 512MiB + 메타데이터 512MiB를 1GiB CNS에 수용한다.
# main_blkaddr가 CNS의 끝과 일치하는지는 게스트에서 검증한다.
CNS='femu,id=native-meta,serial=TEAM4-NATIVE-META,devsz_mb=1024,namespaces=1,femu_mode=1,secsz=512,secs_per_pg=8,pgs_per_blk=256,blks_per_pl=64,pls_per_lun=1,luns_per_ch=4,nchs=8,pg_rd_lat=85000,pg_wr_lat=12196000,blk_er_lat=3000000,ch_xfer_lat=0,gc_thres_pcent=75,gc_thres_pcent_high=95'
# 현재 FEMU: zone_bytes = namespace_bytes * num_plane / num_blk.
# 32GiB * 2 / 128 = 512MiB, 총 64 zones. 실제 값은 guest에서 재검증한다.
ZNS='femu,id=native-data,serial=TEAM4-NATIVE-DATA,devsz_mb=32768,namespaces=1,zns_num_ch=8,zns_num_lun=4,zns_num_plane=2,zns_num_blk=128,zns_flash_type=4,femu_mode=3'
CMD=("$FEMU_BIN" -name native-f2fs-team4 -enable-kvm -cpu host -smp 8 -m 16G
  -device virtio-scsi-pci,id=scsi0 -device scsi-hd,drive=hd0
  -drive "file=$IMG,if=none,aio=native,cache=none,format=qcow2,id=hd0"
  -device "$CNS" -device "$ZNS"
  -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22
  -device virtio-net-pci,netdev=net0 -display none -monitor none
  -serial "file:${FEMU_STATS_FILE}.serial.log")
if [[ ${DRY_RUN:-0} == 1 ]]; then
  printf '%q ' "${CMD[@]}"
  printf '\n'
  exit 0
fi
[[ $(id -un) == zns-team4 && -x $FEMU_BIN && -f $IMG ]]
if pgrep -u zns-team4 -f '[q]emu-system-x86_64' >/dev/null; then
  echo 'team4 FEMU가 이미 실행 중입니다. 정상 종료 후 실행하세요.' >&2
  exit 1
fi
if ss -ltn 'sport = :2204' | grep -q ':2204'; then
  echo '2204 포트 사용 중' >&2
  exit 1
fi
exec "${CMD[@]}"

#!/usr/bin/env bash
# team4 DM-ZNS 비교용. 기존 run-zns.sh/native 실행기는 수정하지 않는다.
set -euo pipefail
BIN=/home/zns-team4/femu/src/build-femu/qemu-system-x86_64
IMG=/home/zns-team4/zns-team4.qcow2
CMD=("$BIN" -name dm-zns-512m-team4 -enable-kvm -cpu host -smp 8 -m 16G
  -device virtio-scsi-pci,id=scsi0 -device scsi-hd,drive=hd0
  -drive "file=$IMG,if=none,aio=native,cache=none,format=qcow2,id=hd0"
  -device femu,devsz_mb=32768,namespaces=1,zns_num_ch=8,zns_num_lun=4,zns_num_plane=2,zns_num_blk=128,zns_flash_type=4,femu_mode=3
  -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2204-:22
  -device virtio-net-pci,netdev=net0 -nographic)
if [[ ${DRY_RUN:-0} == 1 ]]; then printf '%q ' "${CMD[@]}"; printf '\n'; exit 0; fi
[[ $(id -un) == zns-team4 && -x $BIN && -f $IMG ]]
if pgrep -u zns-team4 -f '[q]emu-system-x86_64' >/dev/null || ss -ltn 'sport = :2204' | grep -q ':2204'; then
  echo 'team4 VM 또는 2204 포트 사용 중: 실행 거절' >&2
  exit 1
fi
exec "${CMD[@]}"

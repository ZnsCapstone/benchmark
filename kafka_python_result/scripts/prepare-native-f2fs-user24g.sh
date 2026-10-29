#!/usr/bin/env bash
# 게스트 전용. 검증한 두 폐기용 FEMU 장치만 초기화한다.
set -euo pipefail
if [[ ${1:-} != --run ]]; then
  echo '계획: CNS 1GiB + ZNS 32GiB/512MiB, mkfs -Z 8 -o 14.286; --run으로 초기화'
  exit 0
fi
OUT=${2:?신규 결과 경로 필요}
[[ $OUT == /home/zns/kafka_python_result/results/native-zns/* && ! -e $OUT ]]
[[ $(hostname) == zns-guest ]]
FORMAT=/home/zns/tools/f2fs-native-reserve-03aa49e/mkfs.f2fs
DUMP=/home/zns/tools/f2fs-native-reserve-03aa49e/dump.f2fs
[[ $(sha256sum "$FORMAT" | cut -d ' ' -f 1) == 28008104b1137467e2768c0c3ada39e7529c76a4f9ede2e8c09e0daa4ad1af34 ]]
[[ $(cat /sys/block/nvme0n1/size) == 2097152 ]]
[[ $(cat /sys/block/nvme1n1/size) == 67108864 ]]
[[ $(cat /sys/block/nvme0n1/queue/zoned) == none ]]
[[ $(cat /sys/block/nvme1n1/queue/zoned) == host-managed ]]
[[ $(cat /sys/block/nvme1n1/queue/chunk_sectors) == 1048576 ]]
[[ $(lsblk -dn -o MODEL /dev/nvme0n1) == 'FEMU BlackBox-SSD Controller' ]]
[[ $(lsblk -dn -o MODEL /dev/nvme1n1) == 'FEMU ZMS-SSD Controller _by Misao_' ]]
if pgrep -x fio || pgrep -f 'kafka[.]Kafka|com[.]hanyang[.]cs[.]KafkaBenchmark'; then
  echo '다른 부하기 실행 중: 초기화 거절' >&2
  exit 1
fi
if findmnt -rn -S /dev/nvme0n1 || findmnt -rn -S /dev/nvme1n1; then
  echo '실험 장치가 마운트됨: 초기화 거절' >&2
  exit 1
fi
[[ -z $(ls /sys/block/nvme0n1/holders) && -z $(ls /sys/block/nvme1n1/holders) ]]
exec 9>/tmp/kovalt-fio-evaluation.lock
flock -n 9
sudo -n true
mkdir -p "$OUT"
lsblk -b -o NAME,SIZE,MODEL,MOUNTPOINT > "$OUT/devices.txt"
uname -a > "$OUT/kernel.txt"
sha256sum "$FORMAT" "$DUMP" > "$OUT/formatter-sha256.txt"
sudo -n sh -c 'echo mq-deadline > /sys/block/nvme1n1/queue/scheduler'
sudo -n "$FORMAT" -f -m -d 1 -Z 8 -o 14.286 -c /dev/nvme1n1 /dev/nvme0n1 > "$OUT/mkfs.log" 2>&1
sudo -n "$DUMP" -d 1 /dev/nvme0n1 > "$OUT/layout.log" 2>&1
# 32GiB main / 24GiB user, section=256 segments, main은 CNS 끝에서 시작.
grep -Eq 'main_blkaddr.*262144' "$OUT/layout.log"
grep -Eq 'user_block_count.*6291456' "$OUT/layout.log"
grep -Eq 'rsvd_segment_count.*2048' "$OUT/layout.log"
grep -Eq 'overprov_segment_count.*4096' "$OUT/layout.log"
sudo -n mkdir -p /result/native-f2fs
sudo -n mount -t f2fs -o mode=lfs,discard /dev/nvme0n1 /result/native-f2fs
sudo -n chown zns:zns /result/native-f2fs
findmnt /result/native-f2fs > "$OUT/mount.txt"
df -B1 /result/native-f2fs > "$OUT/df.txt"
cat "$OUT/df.txt"

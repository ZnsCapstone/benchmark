# ZNS 기반 Kafka 저장장치 벤치마크

Apache Kafka의 저장 경로를 Conventional SSD(CNS), DM-ZNS, Native ZNS 환경에서 비교하기 위한 벤치마크 소스입니다.

Python 실행기가 파일시스템·Device Mapper·Kafka 환경을 구성하고 실험을 제어하며, Java 부하 생성기가 Kafka 처리량, ACK 지연, stall 및 consumer 정합성을 측정합니다.

## 실험 구성

| 실험 | 목적 | 준비 방식 | 주요 지표 |
| --- | --- | --- | --- |
| fio | 파일시스템·저장장치 경로 성능 | 고정 크기 파일에 실제 데이터 기록 | IOPS, MiB/s, p95/p99 지연, GC, WA |
| prefill + Kafka | 높은 점유율에서 Kafka 성능과 장기 진행성 | fio로 약 80%를 채운 뒤 파일 유지 | ACK ops/s, 지연, stall, 정합성 |
| Kafka-only | Kafka의 데이터 배치와 retention 기반 공간 순환 | Kafka로 채우고 segment 삭제·공간 회수 확인 | 준비 시간, 점유율, retention 진행, Kafka 성능 |

Kafka 실험은 다음 시나리오를 제공합니다.

- `write`: producer write
- `read-write`: producer + consumer
- `read-write-topics`: producer + consumer + topic 생성·삭제

주요 비교 대상은 다음과 같습니다.

- CNS + EXT4/F2FS
- ZNS + DM-ZNS + EXT4/F2FS
- Native ZNS F2FS multi-device

fio 실행기는 `cns`, `dm-zns`, `native-f2fs-zns`를 지원합니다. prefill + Kafka는 `cns`와 `dm-zns`를 지원하며, Native는 준비된 mount에 hook을 적용하는 별도 진입점을 사용합니다. Kafka-only 기본 실행기는 DM-ZNS 전용입니다. Native·CNS의 과거 Kafka-only 결과에는 별도 보존 실행기를 사용했으므로 backend 이름만 바꿔 같은 실험을 실행할 수는 없습니다.


## 코드 구조

전체 실행 흐름은 다음과 같습니다.

```text
CLI entrypoint
  → 실험 인자·장치·중복 실행 검사
  → system_setup.py: DM/mkfs/mount/Kafka 준비
  → fio 또는 Java KafkaBenchmark 실행
  → 실행 중 OS·장치·DM·점유율 표본 수집
  → 결과 유효성 판정
  → reporting.py: JSON/CSV/요약 보고서 저장
```

### Python 실행기

Python 코드는 환경 구성과 반복 실행을 담당합니다.

- `settings.py`는 device path, Kafka 경로, DM 옵션과 공통 실험 기본값을 관리합니다.
- `system_setup.py`는 backend에 따라 ZNS reset, DM 생성, mkfs, mount와 Kafka KRaft 환경 구성을 수행합니다.
- `bench_runner.py`는 파일시스템·record size·scenario·round 조합을 순회하고 Java 명령을 실행합니다.
- `bench_logging.py`는 `iostat`, `vmstat`, kernel log와 실행 환경을 수집합니다.
- `performance.py`는 Java 출력에서 처리량, latency, stall, backpressure와 정합성 지표를 추출합니다.
- `reporting.py`는 회차별 원본 결과를 JSON과 CSV로 정규화합니다.

`fio_evaluation.py`, `performance_evaluation.py`, `kafka_only_evaluation.py`는 서로 다른 평가 목적을 위한 상위 entrypoint입니다. 각 entrypoint는 공통 실행기와 환경 구성 코드를 재사용하므로 backend와 파일시스템이 달라도 같은 결과 형식과 판정 기준을 적용할 수 있습니다.

### Java Kafka 부하 생성기

`KafkaBenchmark.java`는 목표 OP/s에 맞춰 producer 요청을 생성하고 callback에서 broker ACK 완료 시각을 기록합니다. warmup과 측정 구간을 분리하며, 측정 종료 후 outstanding 요청을 drain한 뒤 다음 값을 출력합니다.

- 전송·ACK 처리량과 목표 달성률
- 평균 및 p50/p95/p99/p99.9 ACK latency
- outstanding, backpressure와 schedule lag
- ACK stall 횟수·누적 시간·최대 시간
- send failure와 drain 후 unresolved 요청

읽기 시나리오에서는 consumer가 record header와 sequence를 검사하여 누락, 중복, 순서 역전과 payload CRC 오류를 집계합니다. `ConsumerProgress.java`는 consumer 진행 상태를 추적하고, `RebalanceProbe.java`는 partition rebalance 동작을 별도로 확인합니다.

### 실험별 제어 흐름

- **fio:** 미리 생성하고 검증한 bounded file 안에서 direct I/O를 수행합니다. 원본 fio JSON과 초별 IOPS·대역폭·지연 로그를 함께 저장합니다.
- **prefill + Kafka:** 매 조건마다 파일시스템을 초기화하고 fio 파일로 목표 점유율을 만든 뒤, prefill 파일을 유지한 상태에서 Kafka 부하를 실행합니다.
- **Kafka-only:** 같은 Kafka topic에 계속 쓰면서 점유율과 partition별 segment offset을 관측합니다. 점유율이 안정되고 모든 partition에서 retention 진행이 확인된 뒤 측정 구간으로 넘어갑니다.

준비 실패, kernel storage error와 외부 간섭은 성능 포화와 구분합니다. 실행기가 결과 파일과 종료 표식을 모두 확인한 경우에만 해당 회차를 유효한 측정 후보로 취급합니다.

## 실행 환경

- Linux guest와 별도 실험용 block device
- Python 3
- Java 17
- Kafka 4.2.0
- fio
- EXT4/F2FS 도구

구체적인 device path, Kafka 경로와 DM 설정은 `kafka_python_result/src/settings.py`에서 확인합니다.


## 실험 재현 방법

아래 명령은 **FEMU 장치가 준비된 Linux 게스트 안에서** 저장소를 clone하고 한 조건씩 실행하는 절차입니다. 파일시스템 생성으로 시험 장치의 데이터가 초기화되므로 전용 FEMU 장치를 사용합니다. 진행 중인 실험이 있으면 종료 후 시작합니다.

예시는 ZNS 32GiB·zone 512MiB, DM 논리 24GiB입니다. Native는 보조 CNS 1GiB와 ZNS 32GiB를 사용합니다. CNS 단독 namespace는 24GiB입니다. 실험 데이터는 시험 장치에, 결과와 broker 진단 로그는 별도 시스템 디스크에 저장합니다.

### 1. 공통 환경 준비

| 구분 | 용도 | 준비 항목 |
| --- | --- | --- |
| 실행 도구 | 모듈 빌드와 fio/Kafka 실행 | Git, Python 3.10 이상, JDK 17, fio, sysstat, e2fsprogs, f2fs-tools, util-linux, kmod, make·C 컴파일러·실행 커널 헤더 |
| Kafka 설치 | 단일 KRaft broker | Kafka 4.2.0 설치본과 실행 가능한 bin 스크립트 |
| 시험 장치 | 선택한 backend의 I/O 대상 | DM용 ZNS, CNS 단독 또는 Native용 CNS+ZNS 장치 |

FEMU·Kafka 본체는 이 저장소에 포함하지 않습니다. 사용할 FEMU 장치와 Kafka 설치를 준비한 상태에서 시작합니다. Kafka 경로는 실제 broker 스크립트가 실행되는 설치 경로로 지정합니다.

```bash
git clone https://github.com/ZnsCapstone/benchmark.git "$HOME/benchmark"
git clone --branch zns_dm/Kovalt03 --single-branch \
    https://github.com/ZnsCapstone/dm-zns-base.git "$HOME/dm-zns-base"
cd "$HOME/benchmark/Kafka-benchmark"
./gradlew test jar
cd "$HOME/dm-zns-base"
make -C src W=1
```

기존 checkout이 있으면 새로 복제하지 말고 작업 트리를 확인한 뒤 사용할 버전을 선택합니다. 모듈은 **게스트에서 해당 커널용으로** 빌드합니다. Gradle 의존성 캐시가 있으면 `./gradlew test jar --offline`도 사용할 수 있습니다.

**직접 수정할 설정**

`kafka_python_result/src/settings.py`에서 다음 값을 확인합니다. 이 항목은 모두 환경변수로 변경할 수 있는 설정이 아닙니다.

| 코드 설정 | 예시 | 확인할 내용 |
| --- | --- | --- |
| KAFKA_PATH | ~/kafka-4.2.0-src | bin/kafka-server-start.sh 등이 실제로 실행되는 경로 |
| RAW_ZNS_DEVICE | /dev/nvme0n1 | DM/CNS 시험 namespace. Native에서는 이 번호가 보조 CNS임 |
| MOUNT_POINT | /result/kafka-logs | 시험 파일시스템 전용 mount |
| METADATA_DIR | /var/lib/kafka-meta | 시험 장치와 별도인 시스템 디스크 |

다음 환경변수는 실험을 실행하는 게스트 터미널에서 설정합니다. 경로에는 공백을 사용하지 않습니다.

```bash
export BENCH_ROOT="$HOME/benchmark"
export PYTHONPATH="$BENCH_ROOT/kafka_python_result/src"
export BENCH_JAVA_JAR="$BENCH_ROOT/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar"
export DM_ZNS_MODULE_PATH="$HOME/dm-zns-base/src/dm-zns-base.ko"
export BENCH_DM_METRICS=1
export DM_LOGICAL_CAPACITY_PERCENT=75
export DM_GC_RESERVED_ZONES=8
export DM_GC_LOW_WATERMARK=16
export DM_GC_HIGH_WATERMARK=20
export BENCH_EXT4_DISCARD=0
export BENCH_F2FS_DISCARD=0
export JAVA_TOOL_OPTIONS='-Dbenchmark.explicitPartitions=8'
export RESULT_ROOT="$HOME/benchmark-results"
mkdir -p "$RESULT_ROOT"
```

아래 상위 실행기는 동적 DM을 선택합니다. 설정 파일 초기값만 보지 말고 실행 시 출력된 DM implementation도 확인합니다.

### 2. 실행 전 점검

```bash
uname -r
lsblk -b -o NAME,SIZE,MODEL,FSTYPE,MOUNTPOINTS
findmnt -T "$RESULT_ROOT"
df -h "$RESULT_ROOT"
pgrep -af 'fio|kafka[.]Kafka|com[.]hanyang[.]cs[.]KafkaBenchmark'
sudo -v
sudo -n true
modinfo -F vermagic "$DM_ZNS_MODULE_PATH"

# DM용 ZNS 기대값: host-managed / 67108864 sectors / 1048576 sectors
cat /sys/block/nvme0n1/queue/zoned
cat /sys/block/nvme0n1/size
cat /sys/block/nvme0n1/queue/chunk_sectors
sudo blkzone report /dev/nvme0n1
```

장치 모델이 FEMU인지, 사용 중인 mount·DM holder·다른 부하기가 없는지 확인합니다. 이 점검에서 기존 프로세스를 종료하지 않습니다. 모듈 vermagic의 첫 값은 `uname -r`와 같아야 합니다. `sudo -n true`가 실패하면 비대화형 실행에 필요한 게스트 권한부터 준비합니다.

CNS 단독 구성에서는 `zoned=none`, `size=50331648`을 확인하고 `blkzone`은 사용하지 않습니다. Native의 장치 번호는 6절과 같습니다. 결과 디렉터리는 시험 mount와 다른 파일시스템에 둡니다.

```bash
EVIDENCE="$RESULT_ROOT/environment-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$EVIDENCE"
git -C "$BENCH_ROOT" rev-parse HEAD > "$EVIDENCE/benchmark-commit.txt"
git -C "$BENCH_ROOT" status --short > "$EVIDENCE/benchmark-status.txt"
git -C "$BENCH_ROOT" diff HEAD > "$EVIDENCE/benchmark.diff"
git -C "$HOME/dm-zns-base" rev-parse HEAD > "$EVIDENCE/dm-commit.txt"
git -C "$HOME/dm-zns-base" status --short > "$EVIDENCE/dm-status.txt"
git -C "$HOME/dm-zns-base" diff HEAD > "$EVIDENCE/dm.diff"
sha256sum "$BENCH_JAVA_JAR" "$DM_ZNS_MODULE_PATH" > "$EVIDENCE/binaries.sha256"
uname -a > "$EVIDENCE/kernel.txt"
fio --version > "$EVIDENCE/fio-version.txt"
```

사용한 FEMU 버전과 장치 설정도 결과에 기록합니다. 각 조건은 초기 상태가 같은 FEMU·파일시스템에서 시작합니다. 독립 5회는 초기화부터 다섯 번 진행하는 것이며 `--rounds 5`만 지정하는 것과 다릅니다.

### 3. fio 재현

**실험 1-1: 고정 시간 처리 성능**

다음은 DM+EXT4 순차 4KiB 쓰기 한 조건의 계획 확인 명령입니다.

```bash
OUT="$RESULT_ROOT/fio-dm-ext4-seq4k-$(date +%Y%m%d-%H%M%S)"
python3 "$BENCH_ROOT/kafka_python_result/src/fio_evaluation.py" \
    --backend dm-zns --filesystem ext4 --prepare \
    --module-path "$DM_ZNS_MODULE_PATH" \
    --evaluation performance --workloads seq-write --bs 4k --qds 32 \
    --file-gib 1 --warmup 20 --measure 120 --rounds 1 \
    --devices nvme0n1,dm-0 --output-dir "$OUT"
```

계획 확인 후 **같은 명령 끝에 `--run`을 추가해 실행**합니다. `--prepare`는 실행 시 reset·DM 생성·mkfs·mount까지 수행합니다. DM을 미리 올릴 필요가 없습니다. 계획 출력만으로 실제 장치 검사가 모두 끝난 것은 아닙니다.

| 변경할 조건 | 인자 |
| --- | --- |
| F2FS | --filesystem f2fs |
| 랜덤 4KiB 쓰기 | --workloads rand-write --bs 4k |
| 순차 1MiB 쓰기 | --workloads seq-write --bs 1M |
| 순차 4KiB 읽기 | --workloads seq-read --bs 4k |
| 랜덤 4KiB 읽기 | --workloads rand-read --bs 4k |
| 순차 1MiB 읽기 | --workloads seq-read --bs 1M |
| CNS 단독 VM | --backend cns --devices nvme0n1, module-path 인자는 제거 |

각 조건은 새로운 OUT을 사용합니다. 실행기는 파일 전체를 실제 데이터로 준비하고 CRC32C를 검증한 뒤 측정합니다. 시간 내 같은 1GiB 범위를 반복하며 파일이 무한히 커지는 시험은 아닙니다. `direct=1`은 요청값으로, Native의 실제 장치 읽기는 OS 카운터로 확인해야 합니다.

**실험 1-2: 고정 쓰기량 GC 효율**

```bash
OUT="$RESULT_ROOT/fio-gc-dm-ext4-rand4k-$(date +%Y%m%d-%H%M%S)"
python3 "$BENCH_ROOT/kafka_python_result/src/fio_evaluation.py" \
    --backend dm-zns --filesystem ext4 --prepare \
    --module-path "$DM_ZNS_MODULE_PATH" \
    --evaluation gc-efficiency --workloads rand-write --bs 4k --qds 32 \
    --file-gib 18 --total-write-gib 54 --randseed 20261029 \
    --warmup 0 --rounds 1 --safety-timeout 10800 \
    --devices nvme0n1,dm-0 --output-dir "$OUT"
```

확인 후 `--run`을 추가합니다. 파일 준비·검증 후 본 측정에서 총 54GiB를 덮어씁니다. 준비 쓰기는 54GiB에 포함하지 않습니다. watchdog 도달은 성공 종료가 아닙니다. 고정 seed·단일 workload·단일 QD·1회·warmup 0을 사용하며 순차 4KiB/1MiB도 새 초기 상태에서 별도로 실행합니다.

### 4. prefill + Kafka 재현

다음은 fio 80% prefill 이후 목표 1만 읽기·쓰기, warmup 60초·측정 180초의 계획입니다. 실행기가 장치·DM·파일시스템·prefill·broker·topic을 준비합니다.

```bash
OUT="$RESULT_ROOT/prefill-dm-ext4-rw10k-$(date +%Y%m%d-%H%M%S)"
export LOG_DIR="$OUT/broker-logs"
python3 "$BENCH_ROOT/kafka_python_result/src/performance_evaluation.py" \
    --backend dm-zns --filesystem ext4 --scenario read-write \
    --workload prefill --rates 10000 --rounds 1 \
    --warmup 60 --measure 180 --occupancy 80 \
    --minimum-achieved-pct 95 --stall-limit 60 --no-refine \
    --output-dir "$OUT" --dry-run
```

**같은 명령에서 `--dry-run`을 제거하면 실행**합니다. 이 실행기에는 `--run`을 붙이지 않습니다.

| 선택 | 바꿀 인자 |
| --- | --- |
| 쓰기만 | --scenario write |
| F2FS | --filesystem f2fs |
| 목표 2만 | --rates 20000 |
| CNS 참고 | CNS 단독 VM에서 --backend cns |
| 1시간 기능 검증 | --warmup 300 --measure 3600. 단기와 별도 결과로 보존 |

1KiB payload·producer 8개·명시적 partition 8개를 사용합니다. prefill retention은 기본 총 2GiB이며 필요하면 `BENCH_ENDURANCE_RETENTION_TOTAL_BYTES=2147483648`로 명시합니다. segment 조건은 settings.py와 생성된 broker·topic 설정에서 확인합니다. `--kafka-retention-gib`는 별도 Kafka-only 경로용이므로 prefill 설정에 사용하지 않습니다.

1만·2만을 모두 독립 실행하려면 한 rate씩 지정하고 VM 초기화부터 반복합니다. `--rates 10000,20000` sweep은 포화 시 상위 rate를 생략할 수 있습니다. `--no-refine`은 중간 rate 탐색을 끄는 옵션이지 실패 후 전체 조건을 강제 실행하는 옵션이 아닙니다.

**실험 2의 GC·WA 추가 수집**

이 실행기는 DM 비용의 시작·종료 경계를 자동 저장하지 않습니다. `BENCH_DM_METRICS=1`만으로 GC·WA 결과 파일이 만들어지지 않습니다. 별도 게스트 터미널에서 다음과 같이 시계열을 수집할 수 있습니다.

```bash
# 실행 중인 OUT의 실제 절대 경로로 수정한다.
COST_OUT="$HOME/benchmark-results/실행중인-결과폴더"
mkdir -p "$COST_OUT"
while true; do
    date -Ins
    sudo -n dmsetup status kafka-zns
    sleep 1
done > "$COST_OUT/dm-status-timeline.log" 2>&1
```

실험 후 Ctrl-C로 수집을 끝냅니다. 비용 차분에 사용할 시작·종료 시각을 기록하고, prefill·warmup까지 포함한 전체 차분을 본 측정 180초의 비용으로 사용하지 않습니다. 이 수집은 1초 간격 표본이며 엄밀한 동기 경계는 아닙니다. 대응 경계가 없는 과거 GC·WA는 미확보로 유지합니다.

### 5. Kafka-only 재현

다음 기본 실행기는 **DM-ZNS 전용**입니다. 공간을 Kafka로 채우고 같은 topic의 retention을 이어갑니다. 장치는 32GiB host-managed FEMU, DM 논리 24GiB로 검사합니다.

```bash
OUT="$RESULT_ROOT/kafka-only-dm-f2fs-rw10k-$(date +%Y%m%d-%H%M%S)"
export LOG_DIR="$OUT/broker-logs"
python3 "$BENCH_ROOT/kafka_python_result/src/kafka_only_evaluation.py" \
    --filesystem f2fs --scenario read-write \
    --rates 10000 --prepare-rate 10000 --prepare-timeout 10800 \
    --retention-gib 18 --warmup 0 --measure 1800 \
    --jar "$BENCH_JAVA_JAR" --result-dir "$OUT"
```

확인 후 **`--execute`를 추가하면 실행**합니다. 쓰기만은 `--scenario write`, EXT4는 `--filesystem ext4`, 2만은 `--rates 20000 --prepare-rate 20000`으로 바꿉니다. 각 조건은 새 VM에서 시작합니다.

| 단계 | 이 checkout 기본 실행기의 동작 |
| --- | --- |
| 초기화 | DM·파일시스템·broker·topic 구성 |
| Kafka 채우기 | prepare-rate의 쓰기를 120초 단위로 반복 |
| 준비 완료 | 최근 120초 점유율 75~85%, 변동 폭 3%p 이내, 8개 partition 모두 retention 진행 |
| 제한 | 준비 3시간, 사용률 90% 또는 결과 디스크 여유 2GiB 미만 시 중단 |
| 본 측정 | 준비 후 지정 부하로 1800초 |
| 판정 | 95% 달성, ACK stall 60초 미만, 요청·정합성·장치 오류 확인 |

**보존된 실험과의 차이:** 진행 중인 추가 평가는 별도 `ko-input`에 준비 부하 95% 달성 3회 연속 확인과 retention 비용 경계를 추가했습니다. 이 기본 CLI에는 `--backend`, `--prepare-pass-count`, `--minimum-achieved-pct`가 없습니다. 기존 결과의 정확한 재현에는 해당 입력 snapshot과 manifest가 필요합니다. SOURCE-SNAPSHOT.json의 코드가 모든 과거 실험에서 동일하게 실행됐다는 뜻은 아닙니다.

**과거 로그 읽기**는 아래 wrapper로 실행할 수 있습니다. 위 기본 실행기의 준비 조건을 그대로 사용합니다.

```bash
export BENCH_BACKLOG_CLASSES="$RESULT_ROOT/backlog-classes"
mkdir -p "$BENCH_BACKLOG_CLASSES"
javac -cp "$BENCH_JAVA_JAR" -d "$BENCH_BACKLOG_CLASSES" \
    "$BENCH_ROOT/kafka_python_result/scripts/BacklogReadProbe.java"

OUT="$RESULT_ROOT/kafka-only-dm-f2fs-backlog10k-$(date +%Y%m%d-%H%M%S)"
export LOG_DIR="$OUT/broker-logs"
python3 "$BENCH_ROOT/kafka_python_result/scripts/kafka-backlog-entry.py" \
    --filesystem f2fs --scenario write \
    --rates 10000 --prepare-rate 10000 --prepare-timeout 10800 \
    --retention-gib 18 --warmup 0 --measure 1800 \
    --jar "$BENCH_JAVA_JAR" --result-dir "$OUT"
```

확인 후 `--execute`를 추가합니다. 여기서는 `--scenario write`에도 wrapper가 별도 consumer를 붙입니다. sync 후 파일 캐시를 한 번 제거하고 고정 offset 범위를 읽습니다. `backlog-complete.json`이 없으면 성공으로 보지 않습니다. 읽는 동안 retention이 해당 범위를 삭제하면 실패할 수 있습니다.

### 6. Native F2FS에서 실행

Native 실험은 보조 CNS와 ZNS가 함께 준비된 FEMU 환경에서 수행합니다.

| 장치 | 예상 구성 |
| --- | --- |
| /dev/nvme0n1 | 보조 CNS 1GiB, 2097152 sectors, zoned=none |
| /dev/nvme1n1 | ZNS 32GiB, 67108864 sectors, zone 1048576 sectors |
| mount | /result/native-f2fs, mode=lfs,discard |

장치 번호·모델·크기·기존 mount를 먼저 확인합니다. 이 예시와 다르면 설정과 실행기의 장치 검사도 함께 검토해야 합니다. 아래 포맷은 기존 데이터를 지우므로 다른 작업이 없는 두 시험 장치에만 수행합니다.

사용자 영역 24GiB를 맞추는 데 사용한 formatter 기준은 f2fs-tools `03aa49e4959fc986d87cfa3cef9a8ccd4aabdb74`입니다. 해당 버전의 mkfs·dump를 준비하고 경로를 직접 지정합니다.

```bash
# 준비한 도구의 실제 절대 경로로 수정한다.
MKFS_F2FS=/path/to/mkfs.f2fs
DUMP_F2FS=/path/to/dump.f2fs
NATIVE_SETUP="$RESULT_ROOT/native-setup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$NATIVE_SETUP"

sudo -n "$MKFS_F2FS" -f -m -d 1 -Z 8 -o 14.286 \
    -c /dev/nvme1n1 /dev/nvme0n1 > "$NATIVE_SETUP/mkfs.log" 2>&1
sudo -n "$DUMP_F2FS" -d 1 /dev/nvme0n1 > "$NATIVE_SETUP/layout.log" 2>&1
sha256sum "$MKFS_F2FS" "$DUMP_F2FS" > "$NATIVE_SETUP/tools.sha256"
```

mount 전에 layout.log에서 `main_blkaddr=262144`, `user_block_count=6291456`, `rsvd_segment_count=2048`, `overprov_segment_count=4096`을 확인합니다. 예상과 다르면 다음 단계로 진행하지 않습니다. 이 설정의 사용자 용량은 24GiB이며 df에 표시되는 전체 영역과 구분합니다.

```bash
sudo -n mkdir -p /result/native-f2fs
sudo -n mount -t f2fs -o mode=lfs,discard /dev/nvme0n1 /result/native-f2fs
sudo -n chown "$(id -un):$(id -gn)" /result/native-f2fs
findmnt /result/native-f2fs
```

준비한 Native에서 fio를 실행할 때는 `--prepare`와 `--dm-name`을 사용하지 않습니다.

```bash
OUT="$RESULT_ROOT/fio-native-seq4k-$(date +%Y%m%d-%H%M%S)"
python3 "$BENCH_ROOT/kafka_python_result/src/fio_evaluation.py" \
    --backend native-f2fs-zns --filesystem f2fs --mount /result/native-f2fs \
    --devices nvme0n1,nvme1n1 --workloads seq-write --bs 4k --qds 32 \
    --file-gib 1 --warmup 20 --measure 120 --rounds 1 --output-dir "$OUT"
```

확인 후 `--run`을 추가합니다. 읽기는 3절의 workload·bs를 선택합니다. Native의 `direct=1`은 요청값이므로 실제 장치 읽기를 함께 확인합니다.

Native 18GiB 파일·54GiB 고정량 시험은 예약/OP를 제외한 사용자 용량 기준의 공간 검사가 필요합니다. 공개 기본 실행기는 statfs 기반 80% 검사로 해당 조건을 거절할 수 있으며, 보고서의 완료 결과에는 이 검사를 보완한 별도 입력을 사용했습니다. 기본 명령만으로 그 결과가 그대로 재현된다고 간주하지 않습니다.

Native prefill+Kafka는 준비한 mount에서 별도 wrapper를 사용합니다. 기존 broker·부하기·DM 모듈이 없어야 하며 PYTHONPATH를 설정한 상태로 실행합니다. wrapper의 hostname·장치·mount 검사값도 자신의 환경과 일치하는지 먼저 확인합니다. 아래는 계획 확인이 아닌 **실제 실행 명령**입니다.

```bash
OUT="$RESULT_ROOT/prefill-native-rw10k-$(date +%Y%m%d-%H%M%S)"
export LOG_DIR="$OUT/broker-logs"
python3 "$BENCH_ROOT/kafka_python_result/scripts/run-native-kafka-cell.py" \
    --single-rate --backend dm-zns --filesystem f2fs --scenario read-write \
    --workload prefill --rate 10000 --rounds 1 \
    --warmup 60 --measure 180 --occupancy 80 --stall-limit 60 --run-dir "$OUT"
```

backend 이름은 공통 parser용이며 wrapper가 Native mount 재사용과 DM 금지 처리를 적용합니다. 사용자 용량 분모도 24GiB로 맞춥니다. 새 독립 조건은 초기화부터 다시 수행합니다. Native/CNS Kafka-only는 별도 보존 입력을 사용했으므로 기본 Kafka-only CLI에 backend만 추가하는 방식으로 실행할 수 없습니다.

### 7. 완료와 결과 검증

| 실험 | 확인할 파일과 조건 |
| --- | --- |
| fio | manifest.json, summary.json, 각 *.fio.json. job error=0, runtime·I/O량이 계획과 일치 |
| fio 1-2 | 준비와 구분한 본 측정 write bytes가 정확히 54GiB. watchdog·중도 종료는 실패 |
| prefill+Kafka | sweep_summary.json, 하위 runner_exit_code.txt·csv/full_results.csv·kernel log |
| Kafka-only | readiness.json, result-10000.json 등, summary.json, completed.json, failure.json 유무 |
| 과거 로그 | backlog-ready.json·backlog-complete.json과 고정 offset 범위 완독 |

```bash
find "$OUT" -maxdepth 3 -type f -name '*.json'
find "$OUT" -type f -name 'performance-kernel-window.log'
```

실행 직후 종료 코드도 기록합니다. exit=0이나 completed.json만으로 성능 통과라고 판단하지 않습니다. 목표 미달·준비 실패·요청/장치 오류·미측정을 구분합니다. consumer를 사용했다면 ACK와의 차이·drain·CRC·gap·duplicate·order를 함께 확인합니다.

FEMU 내부 통계는 계측 지원 빌드가 생성한 CSV를 별도로 준비합니다. fio의 `--femu-csv`에는 현재 환경에서 읽을 수 있는 CSV 경로를 지정합니다. 실행 후 대응 측정 시각으로 집계할 때도 준비 구간을 제외하고 사용한 경계를 기록합니다. Native wrapper의 single-rate 결과에는 sweep_summary가 없으므로 CSV·종료 표식·kernel log를 직접 확인합니다.

### 8. 중단·실패 후 처리

자신이 실행한 전경 runner만 Ctrl-C로 중단하고 cleanup을 기다립니다.

실패 시 초기화 전에 failure.json·launch log·Java raw·kernel log·DM 상태·장치 목록을 보존합니다.

```bash
sudo journalctl -k --no-pager > "$EVIDENCE/kernel-after.txt"
sudo dmsetup status kafka-zns > "$EVIDENCE/dm-status-after.txt" 2>&1
lsblk -b -o NAME,SIZE,MODEL,FSTYPE,MOUNTPOINTS > "$EVIDENCE/devices-after.txt"
pgrep -af 'fio|kafka[.]Kafka|com[.]hanyang[.]cs[.]KafkaBenchmark'
```

자신의 runner·broker가 끝난 것을 확인한 뒤 시험 mount를 unmount하고 DM을 제거합니다. sync·unmount가 D-state로 정체되면 reset·mkfs를 추가로 수행하지 않습니다. 증거를 보존하고 초기 상태를 복원한 FEMU 환경에서 다음 조건을 실행합니다. 다른 실험의 프로세스는 종료하지 않습니다.

## 결과와 측정 지표

실행기에 따라 다음 자료가 결과 디렉터리에 저장됩니다.

- 원본 fio JSON과 초별 IOPS·대역폭·지연 로그
- Kafka raw output
- `results_snapshot.json`
- `csv/full_results.csv`
- `summary_report.txt`
- 점유율·OS·DM 표본과 kernel log
- 실행 인자, commit 및 SHA-256이 포함된 manifest

주요 판정 지표는 다음과 같습니다.

- fio: IOPS, MiB/s, p50/p95/p99/p99.9 latency
- Kafka: target/achieved ops/s, ACK latency, stall, outstanding, backpressure
- 정합성: send error, unresolved request, sequence gap, CRC error
- 저장 경로: GC, compaction, foreground wait, write amplification(WA)

WA는 상위 계층이 요청한 논리 쓰기보다 DM·장치 내부에서 실제로 얼마나 더 기록했는지를 나타냅니다. 비교 시 DM WA와 FEMU NAND WA의 분모가 다를 수 있으므로 같은 정의끼리 비교합니다.

Kafka 실행의 기본 안정성 판정은 다음과 같습니다.

- 목표 처리량 달성률 95% 이상
- ACK stall 60초 미만
- send/failed/unresolved request 0
- consumer 사용 시 정합성 오류 0
- kernel storage error 0

실행 완료와 성능 재현성은 별개입니다. 최종 비교에는 동일한 초기 상태에서 독립 반복이 필요합니다.

## 재현성 정보

`SOURCE-SNAPSHOT.json`에는 이 저장소를 구성할 때 사용한 Python 실행기와 Java 부하 생성기의 기준 commit, archive checksum 및 파일별 SHA-256이 기록되어 있습니다.

최종 결과에는 다음 정보를 함께 보존합니다.

- benchmark commit과 dirty diff
- DM-ZNS commit, source와 module SHA-256
- Kafka benchmark JAR SHA-256
- FEMU commit, binary SHA-256 및 실행 옵션
- kernel, fio, Kafka와 파일시스템 버전

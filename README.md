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

이 저장소의 Python 실행기는 `cns`와 `dm-zns` backend를 직접 지원합니다. Native ZNS 및 VM 단위 전체 matrix 실행에는 별도의 host-side controller와 환경 구성이 필요합니다.


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

> 이 절은 환경별 최종 실행 명령과 확인 절차를 정리하기 위한 항목입니다. 실제 장치명, module/JAR 경로와 결과 디렉터리를 검증한 뒤 작성합니다.

### 1. 공통 환경 준비

<!-- TODO: guest/host 접속, 저장소 checkout, 의존성 설치, Java JAR 및 DM module 빌드 명령 -->

### 2. 실행 전 점검

<!-- TODO: commit·dirty diff·SHA-256, 장치 geometry, mount, 실행 중인 fio/Kafka, 결과 디스크 여유 공간 확인 명령 -->

### 3. fio 재현

<!-- TODO: CNS/DM-ZNS, EXT4/F2FS별 dry-run과 실제 실행 명령, workload·QD·반복 설정 -->

### 4. prefill + Kafka 재현

<!-- TODO: backend·파일시스템·시나리오·목표 OP/s별 실행 명령과 80% prefill 확인 방법 -->

### 5. Kafka-only 재현

<!-- TODO: 준비 부하, retention, 점유율 안정 조건, 측정 부하와 timeout을 포함한 실행 명령 -->

### 6. Native ZNS 및 host controller 실행

<!-- TODO: native F2FS multi-device 구성, FEMU VM 시작·종료, host-side 통계 수집과 backend matrix 실행 명령 -->

### 7. 완료 및 결과 검증

<!-- TODO: exit code, 결과 파일 존재, kernel storage error, 정합성, 목표 달성률과 반복 완료 여부 확인 명령 -->

### 8. 중단·실패 후 처리

<!-- TODO: 실행 중단 방법, Kafka/DM/mount 정리, 증거 보존 및 재실행 조건 -->

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

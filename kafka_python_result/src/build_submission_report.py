#!/usr/bin/env python3
"""제출용 본문과 채택한 Kafka 관측 CSV를 생성한다."""
import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/submission-20261015'


def main():
    groups = json.loads((OUT / 'fio-groups.json').read_text())
    lookup = {(g['fs'], g['state'], g['workload'], g['bs']): g for g in groups}
    kafka = json.loads((ROOT / 'results/analysis-fixed-10k-20k-20261015/summary.json').read_text())
    def fio_table(state):
        lines = ['| 부하 | EXT4 처리량 중앙값(범위) | F2FS 처리량 중앙값(범위) | p99 중앙값 EXT4 / F2FS(ms) | DM WA 중앙값 EXT4 / F2FS |',
                 '|---|---:|---:|---:|---:|']
        for workload, bs, label in [('seq-write', '4k', '순차 쓰기 4KiB'), ('rand-write', '4k', '랜덤 쓰기 4KiB'), ('seq-write', '1M', '순차 쓰기 1MiB')]:
            a, b = [lookup[fs, state, workload, bs] for fs in ('ext4', 'f2fs')]
            key, unit = ('write_iops', 'IOPS') if bs == '4k' else ('write_mib_s', 'MiB/s')
            def val(g):
                digits = 0 if unit == 'IOPS' else 2
                return f"{g[key + '_median']:,.{digits}f} ({g[key + '_min']:,.{digits}f}~{g[key + '_max']:,.{digits}f}) {unit}"
            lines.append(f"| {label} | {val(a)} | {val(b)} | {a['write_p99_ms_median']:.2f} / {b['write_p99_ms_median']:.2f} | {a['dm_wa_issued_median']:.2f} / {b['dm_wa_issued_median']:.2f} |")
        return '\n'.join(lines)
    rows = []
    for r in kafka:
        m, d = r['cycle_metrics'], r['cycle_dm_approx']
        rows.append({'target_ops': r['target'], 'achieved_ops': m['achieved_ops'],
                     'achieved_pct': m['achieved_pct'], 'p95_ms': m['p95_ms'], 'p99_ms': m['p99_ms'],
                     'ack_stall_max_sec': m['ack_stall_max_sec'], 'failed_requests': m['failed_requests'],
                     'send_errors': m['send_errors'], 'unresolved': m['unresolved_after_drain'],
                     'preparation_seconds': r['prepare_seconds_wall'],
                     'preparation_gc_reclaims': r['prepare_dm']['gc_reclaims'],
                     'observed_gc_seconds': d['seconds'], 'gc_reclaims': d['gc_reclaims'],
                     'gc_moved_gib': d['gc_moved_bytes'] / 2**30, 'dm_wa': d['dm_wa'],
                     'scope': 'EXT4 only; independent single observations; unequal preconditioning'})
    with (OUT / 'Kafka-only-최종관측.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    text = '''# DM-ZNS 기반 저장 경로 및 Kafka 성능 평가

## 1. 평가 목적과 범위

본 평가는 FEMU ZNS SSD 위에서 DM-ZNS가 제공하는 일반 블록 장치를 사용해 EXT4와 F2FS의 저장 경로 성능을 비교하고, Kafka의 공간 순환 과정에서 발생하는 GC 및 쓰기 증폭 비용을 분석한다. F2FS는 DM 위에서 동작하며 native ZNS F2FS가 아니다.

제출 결과는 동일 조건의 최신 fio 48회(12조건 × 4회)와 균등 파티션 배정 Kafka-only의 완료된 EXT4 관측 2개를 사용한다. 실행 오류·중단 결과와 조건이 다른 탐색 결과는 정량 비교표에서 제외했다. 정상 완료한 저속 회차는 삭제하지 않고 반복 범위에 포함했다.

| 평가 | 방법 | 본문에서 사용하는 결과 |
|---|---|---|
| fio | 고정 파일 범위에서 직접 I/O | EXT4·F2FS 동일 조건 4회 반복 비교 |
| 80% prefill + Kafka | fio로 80% 채운 파일을 유지하고 Kafka 실행 | 최종 균등 파티션 조건의 FS 비교 결과 미확보로 정량표 미제시 |
| Kafka-only | Kafka로 채우고 retention 순환 상태에서 같은 목표 부하 유지 | EXT4의 GC·WA 부담 사례 분석; FS 비교 아님 |

## 2. 공통 실험 환경과 지표

| 항목 | 설정 |
|---|---|
| 실행 위치 | 원격 splab의 team4 FEMU 게스트 |
| 게스트 | vCPU 8개, RAM 16GiB, Linux 5.15.0-186-generic |
| 장치와 경로 | FEMU ZNS 32GiB → DM-ZNS 24GiB → EXT4 또는 F2FS |
| 공간 정책 | GC reserve / low / high = 2 / 4 / 5 zones |
| 파일시스템 | EXT4 discard OFF / F2FS nodiscard |
| 데이터·결과 분리 | 실험 데이터는 전용 DM 장치, 결과는 게스트 루트 디스크 |
| 독립 실행 | 조건마다 zone reset, DM 재생성, mkfs, 파일 실제 기록·검증 |

IOPS와 MiB/s를 함께 해석한다. p95/p99는 요청 지연의 백분위이며, fio는 완료 지연, Kafka는 producer 전송부터 ACK까지의 앱 지연이다. fio는 direct I/O인 반면 Kafka는 page cache와 요청 결합의 영향을 받으며 레코드 크기도 다르다. 두 지연이나 fio IOPS와 Kafka 레코드 ops/s를 같은 지표로 직접 비교하지 않는다.

DM WA는 구간 USER_DATA·GC_DATA·WAL·SSTable 쓰기 합계 / 구간 DM 논리 쓰기로 정의한다. GC는 실제 data zone 회수 횟수와 이주량을 구분하며 worker 기동 횟수를 회수 횟수로 쓰지 않는다. GC worker 시간은 탐색·대기까지 포함하므로 NAND busy 시간으로 해석하지 않는다.

## 3. 파일시스템 비교: fio

### 실험 방법

| 항목 | 저점유율 조건 | 고점유율 GC 부하 조건 |
|---|---|---|
| 실제 기록한 고정 파일 | 1GiB | 18GiB |
| 부하 | 순차 4KiB / 랜덤 4KiB / 순차 1MiB 쓰기 | 동일 3개 쓰기 부하 |
| I/O 방식 | fio 3.28, libaio, direct=1, numjobs=1, QD32 | 동일 |
| warmup / measurement | 20초 / 120초 | 30초 / 600초 |
| 반복 | 조건별 독립 초기화 4회 | 조건별 독립 초기화 4회 |

쓰기 범위는 고정 파일 안으로 제한한다. 18GiB는 GC를 유도하기 위한 고점유율 파일 크기이며 “정확히 80%”라는 의미는 아니다. 저점유율도 누적 쓰기량이 많으면 GC가 발생할 수 있다. 4KiB 순차·랜덤 비교는 크기를 고정하고, 순차 4KiB·1MiB 비교는 접근 순서를 고정한다.

총 48회 모두 fio job 오류 0, 수집된 커널 저장장치 오류 0, 커널 로그 미수집 0이다. 3회 반복 후 추가한 마지막 1회도 같은 모듈 SHA와 fio 실행기 소스 SHA·설정임을 확인해 합산했다. 표는 4회의 중앙값과 전체 범위다. p99는 회차별 p99의 중앙값이지 전체 요청을 합친 p99가 아니다. 상세 CSV에는 p95와 각 회차도 포함한다.

### 저점유율 결과

''' + fio_table('low') + '''

### 18GiB GC 부하 결과

''' + fio_table('gc') + '''

### 파일시스템 차이와 원인 분석

**관측 1 — 저점유율 4KiB에서는 F2FS가 소폭 높은 처리량을 보였다.** 순차 쓰기 중앙값은 EXT4 대비 약 2.1%, 랜덤 쓰기는 약 10.2% 높았다. p99도 비슷하거나 낮았다. 다만 전체 I/O는 파일시스템뿐 아니라 DM 매핑·WAL·FEMU를 거치므로 이 차이를 파일시스템의 일반적 우위로 확대하지 않는다. FS 내부 작업과 WAL 대기 시간을 따로 분해한 결과는 없다.

**관측 2 — 고점유율 랜덤 쓰기의 비용은 두 FS에서 공통으로 컸다.** 저점유율 대비 처리량 중앙값은 EXT4 약 77.8%, F2FS 약 78.8% 감소했고 DM WA는 각각 18.46배, 18.18배였다. 측정 중 GC 이주량 중앙값은 약 32.17GiB, 34.04GiB였다. 작은 새 쓰기보다 유효 데이터 이주가 훨씬 많은 상태였다는 계측 근거가 있다.

해석: DM은 랜덤 논리 쓰기도 물리 zone에 순차 배치한다. 그러나 갱신이 여러 zone에 퍼지면 회수 victim에 살아 있는 데이터가 많이 남아 이주 비용이 커질 수 있다. 이번 높은 이주량·WA는 이 설명과 일치한다. “ZNS는 랜덤 쓰기를 지원하지 못해서 느리다”는 결론은 아니다.

**관측 3 — F2FS 순차 4KiB GC의 낮은 WA는 장점이지만 우위는 확정할 수 없다.** 중앙값은 3,192 대 2,746 IOPS이고 DM WA는 1.89 대 3.30배였다. GC 이주량 중앙값도 F2FS 약 6.01GiB, EXT4 약 14.00GiB로 달랐다. 파일시스템별 쓰기 배치·유효 데이터 분포 차이가 영향을 줬을 가능성이 있다. 다만 F2FS는 정상 완료한 1,183 IOPS 회차가 있어 범위가 크고, 평균은 F2FS 2,733 대 EXT4 2,760 IOPS로 순서가 바뀐다. 내부 GC/checkpoint 및 victim 분포 추적 없이 원인을 확정하지 않는다.

**관측 4 — 1MiB 쓰기는 높은 대역폭과 큰 요청 지연을 함께 보였다.** 저점유율 중앙값은 EXT4 156.89MiB/s, F2FS 142.18MiB/s였지만 범위가 겹친다. GC 조건은 109.11 대 107.38MiB/s로 비슷했고 p99는 초 단위였다. 1MiB는 4KiB의 256배이므로 IOPS가 낮아도 대역폭은 높을 수 있다. 같은 QD32라도 outstanding 바이트는 다르므로 요청 크기 차이를 단순 FS 성능 차이로 해석하지 않는다.

## 4. 애플리케이션 공간 순환 사례: Kafka-only

### 실험 방법과 비교 범위

EXT4에서 fio prefill 없이 Kafka만으로 채운다. 1KiB 레코드, producer 8개, 명시적 파티션 8개, 단일 KRaft broker, replication=1, acks=1, 압축 없음이다. 총 retention 18GiB, segment 128MiB/60초를 사용한다. producer별 고정 파티션으로 8개 모두에 데이터가 기록되는 것을 확인했다.

1만·2만 조건마다 장치를 초기화하며, 채우기와 순환에서 동일 목표 부하를 유지한다. 준비는 120초 부하기 실행을 반복한다. 점유율 75~85%, 최근 120초 변동 3%p 이하, 8개 파티션의 최초 segment 삭제를 확인한 뒤 같은 broker·토픽으로 warmup 60초와 measurement 600초를 실행한다.

두 조건은 각 1회 관측이다. 특히 2만 조건은 준비 안정 판정을 오래 기다려 **측정 전 zone 회수가 197회였고 1만 조건은 1회**였다. 따라서 아래 값은 각 실행의 관측 사례이며 목표 부하 하나만 바꾼 통제된 인과 비교가 아니다. Kafka-only의 F2FS 비교값은 아직 없다.

### 채우기·준비

| 항목 | 1만 목표 | 2만 목표 |
|---|---:|---:|
| 최초 retention 관측까지 | 30분 15초 | 16분 38초 |
| 최초 retention 전 완료 구간 평균 | 9,782 ops/s | 18,180 ops/s |
| 준비 안정 판정까지 총 경과 | 31분 21초 | 70분 16초 |
| 준비 전체 실제 평균 | 9,731 ops/s | 10,244 ops/s |

초기 평균은 최초 retention 이전에 완료된 120초 구간만 포함한다. 준비 전체 평균에는 최초 retention 이후 순환·안정 대기도 포함하며 producer 실행 사이 간격은 제외한다. 따라서 2만 조건이 채우는 데 70분 걸린 것은 아니다.

### Retention 순환 측정

| 항목 | 1만 목표 | 2만 목표 |
|---|---:|---:|
| 실제 ACK 처리량 / 달성률 | 7,369 ops/s / 73.7% | 6,593 ops/s / 33.0% |
| p95 / p99 | 2.94 / 82.59ms | 353.72 / 830.92ms |
| ACK 정지 총 시간 / 최대 | 128초 / 12초 | 314초 / 14초 |
| 전송 실패 / 미처리 요청 | 0 / 0 | 0 / 0 |
| 실제 data zone 회수 | 17회 | 39회 |
| GC 이주량 | 29.49GiB | 74.30GiB |
| DM 논리 쓰기 / ZNS 쓰기 | 4.91 / 34.84GiB | 4.48 / 78.90GiB |
| DM WA | 7.10배 | 17.61배 |

Java 통계는 600초이며 GC/WA는 10초 표본에 따른 약 602.3초·593.5초 근사 구간 차분이다. raw 장치 쓰기량으로 교차 확인한 WA도 각각 약 7.10·17.61배였다. ACK는 디스크 영구 기록을 보장하지 않으며 쓰기 전용이므로 consumer 정합성 검증 결과는 아니다.

### 분석

관측된 GC 이주량과 DM WA는 공간 순환 중 새 데이터 외의 쓰기 부담이 컸음을 보여준다. 1만 실행은 준비 평균 약 9,731 ops/s에서 순환 측정 약 7,369 ops/s로 낮아졌다. 높은 점유율에서 유효 데이터를 옮기며 공간을 회수하는 비용이 ACK 지연과 처리량 저하에 기여한 것으로 해석할 수 있다.

그러나 2만 실행은 더 긴 사전 순환과 다른 물리 상태를 거쳤으므로 지연 악화를 부하 증가만의 결과라고 단정하지 않는다. 두 조건 모두 오류 없이 완료했지만 목표 달성률 95% 기준에는 미달했다. 6천~7천 ops/s를 장치 최대 성능으로 제시하거나 “1만·2만을 안정적으로 처리한다”고 결론 내리지 않는다.

## 5. 결론

1. 동일 환경의 4회 fio 관측에서 저점유율 4KiB 쓰기는 F2FS가 소폭 높은 처리량을 보였다. 고점유율 순차 쓰기는 반복 범위가 커 전체적인 FS 우열을 확정할 수 없다.
2. 고점유율 랜덤 쓰기는 두 FS 모두 약 78% 처리량 감소와 약 18배 DM WA를 보였다. 개선 우선순위는 단순 IOPS 증가뿐 아니라 GC 이주량과 쓰기 증폭을 줄이는 데 있다.
3. Kafka-only EXT4 관측에서도 공간 순환 중 GC·WA·ACK 정지 부담이 확인됐다. 파일시스템 경로의 처리량과 Kafka ACK 처리량은 다른 관점의 결과로 해석해야 한다.

## 6. 적용 한계와 재현 자료

본 결과는 현재 FEMU 모델 안에서 관측된 저장 경로 성능이다. FEMU ZNS의 캐시 소유 zone 처리 문제를 소스에서 확인했으며 NAND 배치·지연·WA 해석을 제한한다. 따라서 모델 NAND WA 숫자는 제출 결과에서 제외했고, 실제 SSD 성능이나 다른 환경에서의 일반적 FS 우위로 확대하지 않는다. 문제를 발견한 것과 이번 개별 저속 회차의 원인을 확정한 것은 다르다.

조건마다 zone·DM·FS를 초기화했지만 FEMU 프로세스 재시작과 전용 CPU 확보까지 통제하지 않았다. 정상 완료와 성능 재현성·장기 정합성은 별개다. CNS 및 native ZNS F2FS와의 정량 비교, 균등 파티션 조건의 prefill+Kafka FS 비교, Kafka-only F2FS 및 독립 반복은 후속 검증 범위다. 이 제출 결과는 원래 benchmark 4의 3,600초 장기 통과를 대신하지 않는다.

재현 근거:

- fio 원본: 게스트 results/final/fio-compact-20261013 및 fio-compact-extra-20261014. 동일 모듈 SHA-256 dc48b944450eaecbe8e615f1068898360ac8a552a28a0574bfcfb84598cf5404.
- Kafka-only 원본: results/final/kafka-only-fixed-10k-20k-20261015/10000ops 및 20000ops. 큐 분리+계측 모듈 SHA-256 a76daf6ba1c7b07debeda25ec54ea44d582da972fa6c0415202cd92f886f10d5.
- DM 소스는 Kovalt03 기반 큐 분리와 계측 코드다. 큐 분리는 후속 커밋 12ce4fa에 반영됐고 두 빌드의 세 소스 파일 해시는 같지만 바이너리 해시는 다르다.
- 첨부 fio CSV는 48회 전체와 조건별 집계다. Kafka CSV는 위 두 관측과 준비 이력 차이를 함께 기록한다. 원본 manifest·summary·커널 로그·실행기 출처는 상위 검증 보고서에 연결한다.
'''
    # 세 평가의 방법을 먼저 모으고 결과를 뒤에 배치한다.
    prefill = json.loads((OUT / 'prefill-kafka-input.json').read_text())
    assert len(prefill) == 36
    error_keys = ('failed_requests', 'send_errors', 'unresolved_after_drain',
                  'consumer_record_delta', 'payload_crc_errors', 'sequence_gaps',
                  'duplicate_records', 'out_of_order_records', 'ack_stall_max_sec')
    assert all(r['valid'] == 'True' and all(float(r[k]) == 0 for k in error_keys) for r in prefill)
    prefill_lines = ['| 시나리오 | 목표 ops/s | EXT4 평균 ACK ops/s (달성률) | F2FS 평균 ACK ops/s (달성률) |',
                     '|---|---:|---:|---:|']
    latency_lines = ['| 파일시스템 | 시나리오 | p95 중앙값(ms) | p99 중앙값(ms) | 평균 raw util(%) |',
                     '|---|---|---:|---:|---:|']
    aggregates = []
    for scenario, label in [('scenario_a', '쓰기 전용'), ('scenario_b', '쓰기+읽기')]:
        for target in (1000, 2000, 3000):
            values = []
            for fs in ('ext4', 'f2fs'):
                subset = [r for r in prefill if r['filesystem'] == fs and r['scenario'] == scenario and int(r['target_ops']) == target]
                assert len(subset) == 3
                avg = statistics.mean(float(r['achieved_ops']) for r in subset)
                p95 = statistics.median(float(r['app_p95_ms']) for r in subset)
                p99 = statistics.median(float(r['app_p99_ms']) for r in subset)
                util = statistics.mean(float(r['raw_util_avg']) for r in subset)
                aggregates.append(dict(filesystem=fs, scenario=scenario, target_ops=target,
                                       repetitions=3, achieved_ops_mean=avg,
                                       p95_ms_median=p95, p99_ms_median=p99, raw_util_mean=util))
                values.append(f'{avg:,.2f} ({avg / target * 100:.2f}%)')
                if target == 3000:
                    latency_lines.append(f'| {fs.upper()} | {label} | {p95:.2f} | {p99:.2f} | {util:.2f} |')
            prefill_lines.append(f'| {label} | {target:,} | {values[0]} | {values[1]} |')
    for name, records in [('prefill-Kafka-원자료.csv', prefill), ('prefill-Kafka-조건별집계.csv', aggregates)]:
        with (OUT / name).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)

    common = text.split('## 2. 공통 실험 환경과 지표\n\n')[1].split('## 3. 파일시스템 비교: fio')[0]
    common = common.replace('| 독립 실행 | 조건마다 zone reset, DM 재생성, mkfs, 파일 실제 기록·검증 |',
                            '| 초기화 | 각 평가의 방법에 명시한 단위로 zone reset, DM 재생성 및 mkfs |')
    fio_method = text.split('### 실험 방법\n\n')[1].split('### 저점유율 결과')[0]
    fio_results = '### 저점유율 결과' + text.split('### 저점유율 결과')[1].split('## 4. 애플리케이션 공간 순환 사례: Kafka-only')[0]
    kafka_method = text.split('### 실험 방법과 비교 범위\n\n')[1].split('### 채우기·준비')[0]
    kafka_results = '### 채우기·준비' + text.split('### 채우기·준비')[1].split('## 5. 결론')[0]
    conclusion = text.split('## 5. 결론\n\n')[1].split('## 6. 적용 한계와 재현 자료')[0]
    limits = text.split('## 6. 적용 한계와 재현 자료\n\n')[1]
    limits = limits.replace('균등 파티션 조건의 prefill+Kafka FS 비교,', 'prefill+Kafka의 균등 8개 파티션 재검증,')
    intro = '''# DM-ZNS 기반 저장 경로 및 Kafka 성능 평가

## 1. 평가 목적과 채택 범위

FEMU ZNS SSD 위 DM-ZNS 일반 블록 장치에서 EXT4와 F2FS를 비교하고, 저장 경로와 Kafka 애플리케이션의 부하·공간 순환 비용을 구분해 평가한다. 여기서 F2FS는 DM 위의 파일시스템이며 native ZNS F2FS가 아니다.

| 평가 | 목적 | 채택 결과 |
|---|---|---|
| ① fio 독립 실험 | 저장 경로의 처리량·지연·GC·WA 비교 | 12조건 × 독립 초기화 4회 = 48회 |
| ② fio 80% prefill + Kafka | 높은 초기 점유율에서 쓰기 및 쓰기+읽기 성능 비교 | 2FS × 2시나리오 × 3목표 × 3회 = 36회 |
| ③ Kafka-only | Kafka로 채운 뒤 retention 순환 비용 관측 | EXT4 1만·2만 목표 각 1회 |

오류·중단·조건이 다른 fio 탐색 실험은 제외했다. 독립 실험의 정상 저속 회차는 포함했다. 토픽 생성·삭제 시나리오는 제출 비교에서 제외했다. ②는 실제 활성 파티션 6개, ③은 균등 8개 조건이므로 두 결과를 같은 조건으로 직접 비교하지 않는다.

## 2. 실험 방법

### 2.1 공통 환경과 지표 정의

'''
    prefill_method = '''### 2.3 fio 80% prefill + Kafka 부하 실험

| 항목 | 설정 |
|---|---|
| 순서 | 장치·DM·FS 초기화 → fio로 FS 80% 실제 채움 → 고정 파일 유지 → Kafka 실행 → 결과 수집 |
| 파일시스템 | EXT4 discard OFF / F2FS nodiscard |
| 시나리오 | scenario_a: producer 쓰기만 / scenario_b: producer 쓰기 + 단일 consumer 읽기 |
| 부하 | 1KiB 레코드, producer 8개, 목표 1,000 / 2,000 / 3,000 ops/s |
| 시간·반복 | warmup 60초 + measurement 180초, FS·시나리오·목표별 3회 |
| Kafka | 단일 broker, acks=1, 압축 없음, retention 총 2GiB, segment 128MiB / 60초 |
| 파티션 | 설정 8개이나 기존 producer 문자열 key의 해시 배정으로 실제 6개 활성(2·5번 비어 있음) |
| 확인 지표 | ACK 처리량·달성률, p95/p99, 실패·미처리, consumer 정합성, OS 장치 util |

fio와 Kafka를 동시에 실행하는 실험이 아니다. fio가 만든 고정 파일로 높은 초기 점유율을 만들고, 이후 Kafka만 부하를 준다. 초기 점유율은 EXT4 약 80.00%, F2FS 약 80.07%이며 각 회차 종료 점유율은 약 81.1~83.4%였다.

양쪽 FS는 같은 기존 파티션 배정 조건으로 비교한다. 균등 8개 조건이나 장시간 retention 순환 검증으로 해석하지 않는다. producer 최대 미처리 1,000건, catch-up 10건/100ms 제한을 사용했다. 처리량은 회차 3개의 평균, p95/p99는 회차별 백분위의 중앙값으로 보고하며 전체 요청을 합친 백분위가 아니다.

이 실험의 DM은 당시 Kovalt03 기반이며, 후속 fio/Kafka-only의 큐 분리·계측 빌드와 동일 바이너리라고 가정하지 않는다. 당시 DM GC·WA 구간 계측이 없어 해당 숫자는 제시하지 않는다. 이는 benchmark 4의 80% prefill 방식을 활용한 짧은 성능 평가이며 원래 3,600초 endurance 통과 결과가 아니다.

'''
    prefill_results = '''### 3.2 fio 80% prefill + Kafka 결과

#### 목표별 ACK 처리량

''' + '\n'.join(prefill_lines) + '''

#### 3,000 ops/s 조건의 지연과 장치 사용률

''' + '\n'.join(latency_lines) + '''

36회 모두 Valid=True였고 전송 실패·미처리 요청 및 1초 단위 ACK 정지 최대값은 0이었다. 읽기 포함 18회는 consumer 수 차이·CRC·gap·중복·순서 오류 0이었다. 쓰기 전용의 consumer 필드는 검증 근거로 사용하지 않는다.

두 FS 모두 시험한 최고 목표 3,000 ops/s에서 평균 97.7~99.4%를 달성했다. 쓰기 전용은 EXT4가 약 1.8% 높았고, 쓰기+읽기는 약 0.03% 차이로 사실상 비슷했다. 이 범위에서는 파일시스템에 따른 큰 처리량 차이보다 설정한 목표 부하를 따라가는 양상이 주로 보인다.

평균 raw util이 낮다고 “GC가 없었다”거나 “부하가 전혀 없다”고 단정할 수 없다. Kafka page cache·batching과 producer 스케줄링·backpressure가 함께 작용하며, 당시 구간 GC/WA 계측이 없으므로 디스크 포화나 GC 비용을 정량 입증하지 못한다. p99의 차이 역시 FS 내부 경로를 추적하지 않아 원인을 확정하지 않는다. **3,000은 시험한 최고 목표이지 DM이나 Kafka의 최대 처리량이 아니다.**

'''
    text = (intro + common + '### 2.2 fio 독립 실험\n\n' + fio_method
            + prefill_method + '### 2.4 Kafka-only 공간 순환 실험\n\n' + kafka_method
            + '## 3. 실험 결과\n\n### 3.1 fio 독립 실험 결과\n\n'
            + fio_results.replace('### ', '#### ') + prefill_results
            + '### 3.3 Kafka-only 결과\n\n' + kafka_results.replace('### ', '#### ')
            + '## 4. 종합 결론\n\n' + conclusion
            + '4. 80% prefill+Kafka는 두 FS가 시험 범위 3,000 ops/s에서 비슷한 처리량을 보였다. 짧은 실행·활성 6개 파티션 조건이므로 최대 성능이나 균등 8개 장기 순환 성능으로 확대하지 않는다.\n\n'
            + '## 5. 한계와 재현 자료\n\n' + limits
            + '\n- prefill+Kafka 원본: results/performance-evaluation/kovalt03-prefill-20261014_142216. 토픽 작업을 제외한 full_results.csv 36행을 재집계했다. 기존 JAR SHA-256 f980b3e10c52b26f5afd0cc072ca61369907f4bfbdaafcfbb8836f4c1ebe6315. 당시 모듈 바이너리 해시는 미확보이며 현재 파일의 해시로 소급하지 않는다.\n'
            + '- 추가 첨부 prefill CSV는 36회 원자료의 핵심 필드와 조건별 집계이며 source 열에 원본 경로를 보존했다.\n')
    details = ['#### 단기 평가의 반복 범위와 backpressure', '',
               '| FS | 시나리오 | 목표 ops/s | ACK ops/s 범위(3회) | p95 범위(ms) | p99 범위(ms) | backpressure 발생 회차 |',
               '|---|---|---:|---:|---:|---:|---:|']
    for scenario, label in [('scenario_a', '쓰기'), ('scenario_b', '쓰기+읽기')]:
        for target in (1000, 2000, 3000):
            for fs in ('ext4', 'f2fs'):
                subset = [r for r in prefill if r['filesystem'] == fs and r['scenario'] == scenario and int(r['target_ops']) == target]
                def span(key):
                    vals = [float(r[key]) for r in subset]
                    return f'{min(vals):,.2f}~{max(vals):,.2f}'
                pressured = sum(float(r['backpressure_wait_count']) > 0 for r in subset)
                details.append(f"| {fs.upper()} | {label} | {target:,} | {span('achieved_ops')} | {span('app_p95_ms')} | {span('app_p99_ms')} | {pressured}/3 |")
    details.append('''

목표 달성률의 분모는 설정한 레코드 목표이며, 유효성 판정과는 별개다. 높은 부하에서 producer backpressure 및 스케줄 catch-up 제한이 있으면 요청 공급 자체가 목표보다 낮아질 수 있다. backpressure 발생은 관측 근거지만 이를 단독으로 디스크 포화라고 판정하지 않는다. 3,000 목표 쓰기에서 EXT4 p99 중앙값은 10.73ms, F2FS는 7.24ms였고, 쓰기+읽기는 9.93ms 대 10.52ms로 순서가 바뀐다. 처리량 한 가지로 지연 우열을 대신 판단하지 않는다.

''')
    text = text.replace('### 3.3 Kafka-only 결과', '\n'.join(details) + '### 3.3 Kafka-only 결과')
    path = ROOT / 'docs/제출용-성능평가-20261015.md'
    path.write_text(text)
    print(path)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Kafka만으로 공간을 채운 뒤 같은 토픽에서 부하를 높이는 탐색 실험."""
import argparse
import fcntl
from collections import deque
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time


def ready(samples, now, window=120, low=75, high=85):
    recent = [s for s in samples if s['time'] >= now - window]
    return (len(recent) >= 3 and recent[0]['time'] <= now - window + 15
            and all(low <= s['used_percent'] <= high for s in recent)
            and max(s['used_percent'] for s in recent)
            - min(s['used_percent'] for s in recent) <= 3
            and recent[-1]['retention_partitions'] == 8)


def assessment(result):
    m = result['metrics']
    reasons = list(result['validity']['invalid_reasons'])
    if m.get('achieved_pct', 0) < 95:
        reasons.append('목표 달성률 95% 미만')
    if m.get('ack_stall_max_sec', 0) >= 60:
        reasons.append('ACK 정지 60초 이상')
    if m.get('cpu_st_avg', 0) >= 5:
        reasons.append('CPU steal 평균 5% 이상: 환경 간섭 의심')
    return {'stable': not reasons, 'reasons': reasons}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--result-dir', required=True)
    p.add_argument('--filesystem', choices=['ext4', 'f2fs'], default='ext4')
    p.add_argument('--scenario', choices=['write', 'read-write'], default='write')
    p.add_argument('--rates', default='3000,5000,10000,20000')
    p.add_argument('--prepare-rate', type=int, default=10000)
    p.add_argument('--prepare-timeout', type=int, default=10800)
    p.add_argument('--retention-gib', type=int, default=18)
    p.add_argument('--warmup', type=int, default=60)
    p.add_argument('--measure', type=int, default=180)
    p.add_argument('--jar', help='consumer 시작 경계 수정이 포함된 별도 JAR')
    p.add_argument('--module-provenance', type=Path, help='별도 빌드 모듈의 소스/커밋 증거 JSON')
    p.add_argument('--execute', action='store_true', help='실험 장치 초기화에 동의하고 실행')
    args = p.parse_args()
    from performance_evaluation import parse_rates, configure_result_paths, capture_kernel_window, kernel_errors
    rates = parse_rates(args.rates)
    if min(args.prepare_rate, args.prepare_timeout, args.retention_gib, args.measure) <= 0 or args.warmup < 0:
        p.error('시간과 부하는 양수여야 합니다')
    if not args.execute:
        print(json.dumps(vars(args), ensure_ascii=False, indent=2, default=str))
        return 0
    import settings as cfg
    import system_setup as setup
    import performance
    from bench_runner import benchmark_config
    lock = open('/tmp/kovalt-kafka-only.lock', 'w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (getattr(cfg, 'STORAGE_BACKEND', 'dm-zns') != 'dm-zns'
            or cfg.RAW_ZNS_DEVICE != '/dev/nvme0n1'
            or Path('/sys/block/nvme0n1/queue/zoned').read_text().strip() != 'host-managed'
            or int(Path('/sys/block/nvme0n1/size').read_text()) != 32 * 1024**3 // 512):
        raise RuntimeError('team4의 32GiB FEMU ZNS 실험 장치 조건과 다릅니다')
    root = Path(args.result_dir).resolve()
    if root.exists():
        raise RuntimeError('기존 결과 폴더에는 기록하지 않습니다')
    root.mkdir(parents=True)
    configure_result_paths(cfg, root)
    cfg.initialize_result_directories()
    cfg.configure_dm_implementation('1')
    cfg.configure_profile('latency')
    if setup.zns_logical_sectors() != 24 * 1024**3 // 512:
        raise RuntimeError('본 실험은 논리 용량 24GiB만 허용합니다')
    if args.jar:
        cfg.JAVA_BENCH_CMD = cfg.JAVA_BENCH_CMD.replace(cfg.JAVA_BENCH_JAR, str(Path(args.jar).resolve()))
        cfg.JAVA_BENCH_JAR = str(Path(args.jar).resolve())
    cfg.JAVA_BENCH_CMD = cfg.JAVA_BENCH_CMD.replace('java ',
        'java -Dbenchmark.consumerStartLatest=true -Dbenchmark.explicitPartitions=8 ', 1)
    os.environ['BENCH_INTEGRITY_DIAG'] = '1'
    samples = deque(maxlen=120)
    phase = '환경 확인'
    last_sample = 0
    since = int(time.time())

    def save(name, value):
        (root / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')

    def sample(force=False):
        nonlocal last_sample
        now = time.time()
        if not force and now - last_sample < 10:
            return
        last_sample = now
        usage = setup.filesystem_usage()
        minima = []
        for i in range(8):
            files = list(Path(cfg.MOUNT_POINT, f'{cfg.TOPIC_NAME}-{i}').glob('*.log'))
            minima.append(min((int(f.stem) for f in files if f.stem.isdigit()), default=0))
        row = dict(time=now, phase=phase, **usage,
                   retention_partitions=sum(offset > 0 for offset in minima),
                   earliest_segment_offsets=minima,
                   raw_block_stat=Path('/sys/block/nvme0n1/stat').read_text().strip())
        if getattr(cfg, 'DM_METRICS', False):
            try:
                status = subprocess.run(['sudo', 'dmsetup', 'status', cfg.DM_NAME],
                                        capture_output=True, text=True, timeout=3)
                row['dm_status'] = status.stdout.strip()
                row['dm_status_exit'] = status.returncode
                row['cpu_ticks'] = Path('/proc/stat').read_text().splitlines()[0]
            except subprocess.TimeoutExpired:
                row['dm_status_error'] = '3초 조회 제한 시간 초과'
        samples.append(row)
        with (root / 'progress.jsonl').open('a') as f:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
        save('status.json', row)
        print(f"[상태] {phase}: 점유율={usage['used_percent']:.2f}%, retention 진행={row['retention_partitions']}/8", flush=True)
        if usage['used_percent'] >= 90:
            raise RuntimeError('실험 장치 점유율 90% 안전 한계')
        if os.statvfs(root).f_bavail * os.statvfs(root).f_frsize < 2 * 1024**3:
            raise RuntimeError('결과 디스크 여유 2GiB 미만')

    # 기존 부하기의 통계 수집을 재사용하되 실행 중에도 안전 한계를 감시한다.
    def streaming(command):
        raw = root / 'current-java.log'
        with raw.open('w') as out:
            process = subprocess.Popen(shlex.split(command), stdout=out, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 2 * cfg.DRAIN_TIMEOUT_SECONDS + args.warmup + max(120, args.measure) + 240
                while process.poll() is None:
                    sample()
                    if time.monotonic() > deadline:
                        raise RuntimeError('Java 실행 제한 시간 초과')
                    time.sleep(1)
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        output = raw.read_text(errors='replace')
        print('\n'.join(output.splitlines()[-35:]), flush=True)
        return output, process.returncode

    performance.run_cmd_streaming = streaming
    arguments = dict(vars(args))
    arguments['module_provenance'] = str(args.module_provenance) if args.module_provenance else None
    method = ('Kafka-only 단일 고정 부하: 채우기와 순환에서 같은 목표 유지'
              if rates == (args.prepare_rate,) else 'Kafka-only 연속 부하 탐색; 독립 반복 아님')
    manifest = {'method': method, 'arguments': arguments,
                'started_epoch': since, 'module': cfg.DM_MODULE_PATH,
                'module_sha256': hashlib.sha256(Path(cfg.DM_MODULE_PATH).read_bytes()).hexdigest(),
                'jar_sha256': hashlib.sha256(Path(cfg.JAVA_BENCH_JAR).read_bytes()).hexdigest()}
    if args.module_provenance:
        manifest['actual_module_build'] = json.loads(args.module_provenance.read_text())
    for repo in ['dm-zns-base', 'Kafka-benchmark', 'kafka_python_result']:
        path = Path.home() / repo
        manifest[repo] = {}
        for key, git_args in [('commit', ['rev-parse', 'HEAD']), ('status', ['status', '--short']), ('diff', ['diff', 'HEAD'])]:
            manifest[repo][key] = subprocess.run(['git', '-C', str(path), *git_args], capture_output=True, text=True, check=True).stdout
    source_paths = list(Path(__file__).parent.glob('*.py'))
    manifest['runner_sources_sha256'] = {str(s): hashlib.sha256(s.read_bytes()).hexdigest() for s in source_paths}
    save('manifest.json', manifest)
    outcomes = []
    started = False
    try:
        # 실행 중인 다른 실험이 있으면 절대 중지하거나 초기화하지 않는다.
        occupied = subprocess.run(['pgrep', '-af', 'com.hanyang.cs.KafkaBenchmark|kafka.Kafka|fio --'], capture_output=True, text=True)
        if occupied.returncode == 0:
            raise RuntimeError('다른 실험 프로세스가 있습니다: ' + occupied.stdout)
        setup.validate_kafka_environment()
        setup.prepare_experiment_kraft_config()
        setup.capture_environment()
        started = True
        setup.setup_filesystem(args.filesystem)
        size = subprocess.run(['sudo', 'blockdev', '--getsize64', cfg.FS_DEVICE],
                              capture_output=True, text=True, check=True)
        if int(size.stdout) != 24 * 1024**3:
            raise RuntimeError('생성된 DM 장치의 실제 용량이 24GiB가 아닙니다')
        setup.control_kafka('start')
        setup.recreate_main_topic(retention_total_bytes=args.retention_gib * 1024**3)
        scenario = cfg.SCENARIO_TEMPLATES['scenario_a' if args.scenario == 'write' else 'scenario_b']
        deadline = time.monotonic() + args.prepare_timeout
        index = 0
        phase = 'Kafka로 채우기/공간 순환 준비'
        while True:
            index += 1
            config = benchmark_config(1024, scenario, 120, 0)
            config.update(target_ops=args.prepare_rate, use_consumer=False)
            result = performance.run_benchmark_once(args.filesystem, 'prepare', config, index, 'prepare')
            save(f'prepare-{index:03d}.json', result)
            capture_kernel_window(root, since)
            if not result['validity']['valid'] or kernel_errors(root / 'env/performance-kernel-window.log'):
                raise RuntimeError('준비 중 오류: 원인을 보존하고 종료합니다')
            sample(True)
            if ready(samples, time.time()):
                save('readiness.json', {'ready': True, 'sample': samples[-1], 'window_seconds': 120})
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('준비 제한 시간 안에 점유율/retention 안정 조건을 충족하지 못했습니다')
        for rate in rates:
            phase = f'{args.scenario} 목표 {rate} ops/s 측정'
            config = benchmark_config(1024, scenario, args.measure, args.warmup)
            config['target_ops'] = rate
            config['fs_usage_before'] = setup.filesystem_usage()
            result = performance.run_benchmark_once(args.filesystem, args.scenario, config, 1, 'measure')
            capture_kernel_window(root, since)
            verdict = assessment(result)
            if kernel_errors(root / 'env/performance-kernel-window.log'):
                verdict = {'stable': False, 'reasons': ['커널 저장장치 오류']}
            result['assessment'] = verdict
            save(f'result-{rate}.json', result)
            outcomes.append({'rate': rate, **verdict, 'metrics': result['metrics']})
            save('summary.json', outcomes)
            print(f'[판정] {rate}: {verdict}', flush=True)
            if not verdict['stable']:
                break
        save('completed.json', {'completed': True, 'time': time.time(), 'points': len(outcomes)})
        return 0
    except BaseException as exc:
        save('failure.json', {'phase': phase, 'error': str(exc), 'time': time.time()})
        raise
    finally:
        if started:
            capture_kernel_window(root, since)
            setup.control_kafka('stop')
            setup.unmount_log_device(strict=False)
            setup.remove_dm_stack(strict=False)


if __name__ == '__main__':
    raise SystemExit(main())

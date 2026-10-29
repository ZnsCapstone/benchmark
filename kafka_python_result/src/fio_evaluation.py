"""FEMU 파일 기반 fio 평가. --prepare는 명시적 장치 초기화/포맷을 수행한다."""
import argparse
import csv
import fcntl
import hashlib
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = {
    'seq-read': ('read', '1M'), 'seq-write': ('write', '1M'),
    'rand-read': ('randread', '4k'), 'rand-write': ('randwrite', '4k'),
    'mixed': ('randrw', '4k'),
}


def fio_command(file, size, rw, bs, qd, ramp, runtime, output, timed=True,
                total_write_bytes=None, randseed=None):
    command = ['fio', '--name=kovalt-fio', f'--filename={file}', f'--size={size}',
               f'--rw={rw}', f'--bs={bs}', '--ioengine=libaio', '--direct=1',
               f'--iodepth={qd}', '--numjobs=1', '--group_reporting=1',
               '--allow_file_create=0', '--end_fsync=1', '--exitall_on_error=1',
               '--percentile_list=50:95:99:99.9', '--output-format=json',
               f'--output={output}']
    if total_write_bytes is not None:
        if not timed or rw not in ('write', 'randwrite') or ramp:
            raise ValueError('fixed-write mode requires write/randwrite and zero warmup')
        if total_write_bytes <= 0 or total_write_bytes % (4096 if bs == '4k' else 1048576):
            raise ValueError('total write bytes must be positive and block aligned')
        command += [f'--io_size={total_write_bytes}']
    if randseed is not None:
        command += ['--randrepeat=1', f'--randseed={randseed}']
    if timed:
        prefix = str(output).removesuffix('.json')
        command += [f'--write_iops_log={prefix}', f'--write_bw_log={prefix}',
                    f'--write_lat_log={prefix}', '--log_avg_msec=1000', '--log_unix_epoch=1']
    if timed and total_write_bytes is None:
        command += ['--time_based=1', f'--runtime={runtime}', f'--ramp_time={ramp}']
    elif not timed:
        command += ['--verify=crc32c', '--do_verify=1', '--verify_fatal=1']
    if rw == 'randrw':
        command += ['--rwmixread=70']
    return command


def validate_fixed_write(payload, expected_bytes):
    parse_fio(payload)  # exactly one successful job
    job = payload['jobs'][0]
    if job.get('write', {}).get('io_bytes') != expected_bytes or job.get('read', {}).get('io_bytes', 0):
        raise RuntimeError('fixed-write workload incomplete or unexpected read I/O')


def fixed_work_costs(record):
    """동일 작업량 비용. DM 계층 비율과 fio→장치 비율은 별도 표기한다."""
    written = record['write_bytes']
    delta = record.get('dm_delta', {})
    logical = delta.get('logical_write_bytes', 0)
    lower = sum(delta.get(k, 0) for k in ('user_write_bytes', 'wal_write_bytes',
                                         'gc_write_bytes', 'sstable_write_bytes'))
    os_data = record.get('os', {})
    devices = [v for name, v in os_data.get('devices', {}).items() if not name.startswith('dm-')]
    device_bytes = sum(v['write_bytes'] for v in devices)
    return dict(fio_write_bytes=written,
                dm_wa_issued=lower/logical if logical else None,
                gc_moved_bytes_per_fio_byte=delta.get('gc_moved_bytes', 0)/written
                    if logical and written else None,
                gc_reclaims=delta.get('gc_reclaims'),
                fio_to_device_write_ratio=device_bytes/written
                    if written and devices and os_data.get('available') else None,
                ratio_scope='DM WA: DM 입력→하위 장치; fio 비율: fio 입력→OS 하위 장치')


def f2fs_cost_snapshot():
    start = time.time_ns() // 1000
    try:
        r = subprocess.run(['sudo', '-n', 'cat', '/sys/kernel/debug/f2fs/status'],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return dict(available=False, error=str(exc), start_us=start)
    patterns = {'gc_calls': r'GC calls:\s*(\d+)', 'gc_bg_calls': r'BG:\s*(\d+)',
                'move_attempt_blocks': r'Try to move\s+(\d+)\s+blocks'}
    values = {key: int(m[1]) for key, regex in patterns.items()
              if (m := re.search(regex, r.stdout))}
    return dict(available=r.returncode == 0 and len(values) == len(patterns)
                and len(re.findall(r'GC calls:', r.stdout)) == 1,
                start_us=start, end_us=time.time_ns() // 1000, counters=values,
                raw=r.stdout, error=r.stderr,
                scope='현재 게스트의 F2FS debugfs 집계; 다른 F2FS 실험 동시 실행 금지')


def parse_fio(payload):
    if len(payload.get('jobs', [])) != 1:
        raise ValueError('expected exactly one fio job')
    job = payload['jobs'][0]
    if job.get('error', 0):
        raise ValueError(f"fio job error={job['error']}")
    result = {}
    for direction in ('read', 'write'):
        values = job.get(direction, {})
        latency = values.get('clat_ns', {})
        percentiles = latency.get('percentile', {})
        result[f'{direction}_iops'] = values.get('iops', 0)
        result[f'{direction}_mib_s'] = values.get('bw_bytes', values.get('bw', 0) * 1024) / 1024**2
        result[f'{direction}_bytes'] = values.get('io_bytes', 0)
        result[f'{direction}_runtime_ms'] = values.get('runtime')
        result[f'{direction}_mean_ms'] = latency.get('mean', 0) / 1e6
        for percentile in ('95', '99', '99.9'):
            value = next((v for k, v in percentiles.items() if float(k) == float(percentile)), None)
            result[f'{direction}_p{percentile}_ms'] = value / 1e6 if value is not None else None
    result['iodepth_level'] = job.get('iodepth_level', {})
    result['fio_cpu_user_pct'] = job.get('usr_cpu')
    result['fio_cpu_system_pct'] = job.get('sys_cpu')
    return result


def dm_metrics(text):
    return {key: int(value) for key, value in re.findall(r'\b([a-z_][a-z_0-9]*)=(\d+)\b', text)}


def wal_diagnostics(delta):
    def mean(value, count):
        return delta.get('wal_diag_'+value, 0) / delta['wal_diag_'+count] / 1e6 if delta.get('wal_diag_'+count) else None
    batches = delta.get('wal_diag_batches', 0)
    return dict(batch_records_mean=delta.get('wal_diag_records', 0)/batches if batches else None,
                data_mean_ms=mean('data_ns','data_count'),
                queue_mean_ms=mean('queue_ns','records'),
                wal_io_batch_mean_ms=mean('io_ns','io_count'),
                commit_mean_ms=mean('commit_ns','commit_count'),
                wal_to_ack_mean_ms=mean('ack_ns','commit_count'),
                ready_to_worker_mean_ms=mean('ready_ns','ready_count'),
                worker_wall_mean_ms=mean('worker_ns','worker_count'),
                dispatch_wait_mean_ms=mean('dispatch_ns','dispatch_count'),
                submit_wait_mean_ms=mean('submit_ns','submit_count'),
                batch_complete_mean_ms=mean('complete_ns','complete_count'),
                ready_over_20ms=delta.get('wal_diag_ready_slow'),
                worker_over_20ms=delta.get('wal_diag_worker_slow'),
                complete_over_20ms=delta.get('wal_diag_complete_slow'),
                note='누적 카운터 차분 기반 평균; 약 1초 표본 경계. 요청별/배치별 분모가 달라 평균을 단순 합산하면 안 됨')


def os_metrics(rows):
    if len(rows) < 2:
        return {'available': False}
    first, last = rows[0], rows[-1]
    elapsed = (last['epoch_us'] - first['epoch_us']) / 1e6
    if elapsed <= 0:
        return {'available': False}
    result = {'available': True, 'sample_seconds': elapsed, 'devices': {}}
    a = [int(value) for value in first.get('cpu_ticks', '').split()[1:9]]
    b = [int(value) for value in last.get('cpu_ticks', '').split()[1:9]]
    if len(a) == len(b) == 8:
        ticks = [y - x for x, y in zip(a, b)]
        result['cpu_iowait_pct'] = 100 * ticks[4] / sum(ticks) if sum(ticks) else None
        result['cpu_steal_pct'] = 100 * ticks[7] / sum(ticks) if sum(ticks) else None
    per_cpu = []
    for cpu in first.get('per_cpu_ticks', {}).keys() & last.get('per_cpu_ticks', {}).keys():
        a_cpu = first['per_cpu_ticks'][cpu]
        b_cpu = last['per_cpu_ticks'][cpu]
        ticks_cpu = [y - x for x, y in zip(a_cpu[:8], b_cpu[:8])]
        if len(ticks_cpu) == 8 and sum(ticks_cpu) > 0:
            per_cpu.append((cpu, 100 * ticks_cpu[7] / sum(ticks_cpu)))
    result['per_cpu_steal_pct'] = dict(per_cpu)
    disks = lambda row: {line.split()[2]: [int(value) for value in line.split()[3:]]
                         for line in row.get('diskstats', [])}
    a, b = disks(first), disks(last)
    for device in a.keys() & b.keys():
        d = [y - x for x, y in zip(a[device], b[device])]
        ios = d[0] + d[4]
        result['devices'][device] = {
            'util_pct': d[9] / (elapsed * 10),
            'await_ms': (d[3] + d[7]) / ios if ios else None,
            'average_queue': d[10] / (elapsed * 1000),
            'read_bytes': d[2] * 512, 'write_bytes': d[6] * 512,
        }
    return result


def femu_delta(path, start_us, end_us):
    with Path(path).open() as file:
        rows = [row for row in csv.DictReader(file)
                if start_us <= int(row['epoch_us']) <= end_us]
    if len(rows) < 2:
        return {'available': False, 'reason': 'insufficient timestamp coverage'}
    first, last = rows[0], rows[-1]
    keys = ('host_write_bytes', 'host_read_bytes', 'nand_read_pages', 'nand_read_ops',
            'nand_program_pages', 'nand_program_ops', 'nand_erase_blocks',
            'nand_program_bytes', 'gc_moved_pages', 'gc_count', 'gc_host_ns',
            'zone_resets', 'zone_appends')
    delta = {key: int(last[key]) - int(first[key]) for key in keys if key in last}
    if any(value < 0 for value in delta.values()):
        return {'available': False, 'reason': 'counter reset in interval'}
    program = delta.get('nand_program_bytes')
    if program is None and 'nand_program_pages' in delta and 'page_bytes' in last:
        program = delta['nand_program_pages'] * int(last['page_bytes'])
    written = delta.get('host_write_bytes', 0)
    return {'available': True, 'delta': delta, 'nand_program_bytes': program,
            'nand_wa_device': program / written if written and program is not None else None,
            'sample_start_us': int(first['epoch_us']), 'sample_end_us': int(last['epoch_us']),
            'boundary_resolution': 'approximately 1 second; samples inside measurement window'}


class Sampler:
    def __init__(self, mount, devices, dm_name, destination):
        self.mount, self.devices, self.dm_name = mount, devices, dm_name
        self.destination = destination
        self.stop = threading.Event()
        self.low_space = threading.Event()
        self.rows = []
        self.thread = threading.Thread(target=self.collect, daemon=True)

    def boundary(self):
        """1-2의 준비/측정 경계를 동기 수집. 주기 sampler 정지 상태에서만 호출."""
        row = {'epoch_us': time.time_ns() // 1000, 'boundary': True}
        usage = os.statvfs(self.mount)
        row['available_bytes'] = usage.f_bavail * usage.f_frsize
        row['used_pct'] = 100 * (usage.f_blocks - usage.f_bfree) / usage.f_blocks
        lines = Path('/proc/stat').read_text().splitlines()
        row['cpu_ticks'] = lines[0]
        row['per_cpu_ticks'] = {line.split()[0]: list(map(int, line.split()[1:9]))
                                for line in lines if re.match(r'^cpu\d+\s', line)}
        row['diskstats'] = [line for line in Path('/proc/diskstats').read_text().splitlines()
                            if line.split()[2] in self.devices]
        if self.dm_name:
            result = subprocess.run(['sudo', '-n', 'dmsetup', 'status', self.dm_name],
                                    capture_output=True, text=True, timeout=5, check=True)
            row['dm'] = dm_metrics(result.stdout)
        self.rows.append(row)
        return row

    def collect(self):
        with self.destination.open('w') as file:
            while not self.stop.is_set():
                row = {'epoch_us': time.time_ns() // 1000}
                try:
                    usage = os.statvfs(self.mount)
                    row['available_bytes'] = usage.f_bavail * usage.f_frsize
                    row['used_pct'] = 100 * (usage.f_blocks - usage.f_bfree) / usage.f_blocks
                    if row['used_pct'] > 90:
                        self.low_space.set()
                    cpu_lines = Path('/proc/stat').read_text().splitlines()
                    row['cpu_ticks'] = cpu_lines[0]
                    row['per_cpu_ticks'] = {line.split()[0]: list(map(int, line.split()[1:9]))
                                            for line in cpu_lines if re.match(r'^cpu\d+\s', line)}
                    row['diskstats'] = [line for line in Path('/proc/diskstats').read_text().splitlines()
                                        if line.split()[2] in self.devices]
                    if self.dm_name:
                        result = subprocess.run(['sudo', '-n', 'dmsetup', 'status', self.dm_name],
                                                capture_output=True, text=True, timeout=2)
                        row['dm'] = dm_metrics(result.stdout)
                        if result.returncode:
                            row['dm_error'] = result.stderr.strip()
                except (OSError, subprocess.TimeoutExpired) as exc:
                    row['sample_error'] = str(exc)
                self.rows.append(row)
                file.write(json.dumps(row) + '\n')
                file.flush()
                self.stop.wait(1)


def run_checked(command, timeout, sampler=None):
    process = subprocess.Popen(command, start_new_session=True)
    deadline = time.monotonic() + timeout
    while process.poll() is None:
        if time.monotonic() >= deadline or (sampler and sampler.low_space.is_set()):
            # Target only this dedicated fio process group, never other experiments.
            import signal
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
            raise RuntimeError('fio watchdog or space limit reached; inspect processes before retry')
        time.sleep(.25)
    if process.returncode:
        raise RuntimeError(f'fio exit={process.returncode}')


def prepare_storage(args, mount):
    """Reuse Kafka setup only after explicit destructive opt-in and preflight."""
    os.environ['BENCH_STORAGE_BACKEND'] = args.backend
    os.environ['BENCH_FILESYSTEMS'] = args.filesystem
    import settings as cfg
    import system_setup

    if mount != Path(cfg.MOUNT_POINT).resolve():
        raise RuntimeError('--prepare must use the configured benchmark mount point')
    if args.backend == 'cns' and getattr(cfg, 'STORAGE_BACKEND', None) != 'cns':
        raise RuntimeError('this checkout does not include CNS setup support')
    model = subprocess.run(['lsblk', '-dn', '-o', 'MODEL', cfg.RAW_ZNS_DEVICE],
                           capture_output=True, text=True, check=True).stdout
    if 'FEMU' not in model:
        raise RuntimeError('refusing to initialize a non-FEMU raw device')
    subprocess.run(['sudo', '-n', 'true'], check=True)
    if args.backend == 'dm-zns':
        requested = args.module_path or os.environ.get('DM_ZNS_MODULE_PATH')
        if requested and not Path(requested).expanduser().is_file():
            raise RuntimeError('requested DM module not found; refusing fallback')
        candidates = [args.module_path, os.environ.get('DM_ZNS_MODULE_PATH'),
                      str(Path.home() / 'dm-zns-metrics-build-20261012/src/dm-zns-base.ko'),
                      cfg.DM_MODULE_PATH]
        chosen = next((Path(path).expanduser().resolve() for path in candidates
                       if path and Path(path).expanduser().is_file()), None)
        if chosen is None or (requested and chosen != Path(requested).expanduser().resolve()):
            raise RuntimeError('requested DM module not found; build it for the guest kernel first')
        if not re.fullmatch(r'[/A-Za-z0-9_.-]+', str(chosen)):
            raise RuntimeError('module path must not contain shell metacharacters or whitespace')
        parameters = subprocess.run(['modinfo', '-p', str(chosen)], capture_output=True,
                                    text=True, check=True).stdout
        vermagic = subprocess.run(['modinfo', '-F', 'vermagic', str(chosen)], capture_output=True,
                                  text=True, check=True).stdout.split()
        if not vermagic or vermagic[0] != os.uname().release:
            raise RuntimeError('module kernel version differs from the running guest')
        if not re.search(r'^metrics_enabled:', parameters, flags=re.M):
            raise RuntimeError('module lacks metrics_enabled; refusing silent uninstrumented setup')
        # Validate zoned geometry before setup tears down the old target.
        cfg.configure_dm_implementation('1')
        system_setup.zns_logical_sectors()
        cfg.DM_MODULE_PATH = str(chosen)
        cfg.DM_METRICS = True
        cfg.WAL_DIAGNOSTIC_DELAY_MS = getattr(args, 'wal_delay_ms', None)
        if cfg.WAL_DIAGNOSTIC_DELAY_MS is not None:
            if not re.search(r'^wal_timing_enabled:', parameters, flags=re.M):
                raise RuntimeError('module lacks WAL diagnostics; refusing setup')
        args.dm_name = cfg.DM_NAME
    system_setup.setup_filesystem(args.filesystem)
    if args.backend == 'dm-zns':
        status = subprocess.run(['sudo', '-n', 'dmsetup', 'status', cfg.DM_NAME],
                                capture_output=True, text=True, check=True)
        if dm_metrics(status.stdout).get('metrics') != 1:
            raise RuntimeError('DM metrics did not activate after setup')
        if cfg.WAL_DIAGNOSTIC_DELAY_MS is not None:
            actual = dm_metrics(status.stdout)
            if actual.get('wal_diag') != 1 or actual.get('wal_delay_ms') != cfg.WAL_DIAGNOSTIC_DELAY_MS:
                raise RuntimeError('WAL diagnostic parameters did not activate')
    return {'backend': args.backend, 'filesystem': args.filesystem,
            'raw_device': cfg.RAW_ZNS_DEVICE, 'filesystem_device': cfg.FS_DEVICE,
            'module_path': str(chosen) if args.backend == 'dm-zns' else None,
            'module_sha256': hashlib.sha256(chosen.read_bytes()).hexdigest()
            if args.backend == 'dm-zns' else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mount', default='/result/kafka-logs')
    parser.add_argument('--prepare', action='store_true', help='initialize FEMU, create DM, mkfs and mount (destroys old data)')
    parser.add_argument('--backend', choices=('dm-zns', 'cns', 'native-f2fs-zns'), default='dm-zns')
    parser.add_argument('--evaluation', choices=('performance', 'gc-efficiency'), default='performance',
                        help='1-1 고정 시간 처리 성능 / 1-2 고정 쓰기량 효율')
    parser.add_argument('--total-write-gib', type=int, default=None,
                        help='1-2 본 측정의 총 논리 쓰기량. 준비 쓰기는 제외')
    parser.add_argument('--safety-timeout', type=int, default=10800,
                        help='1-2 안전 watchdog 초. 도달은 성공 종료가 아닌 실패')
    parser.add_argument('--randseed', type=int, default=None)
    parser.add_argument('--filesystem', choices=('ext4', 'f2fs'), default='ext4')
    parser.add_argument('--module-path', help='guest-built DM module with metrics_enabled support')
    parser.add_argument('--wal-delay-ms', type=int, choices=(0, 2), default=None,
                        help='WAL 진단 전용: 계측 활성화 및 명시적 배치 지연; 기본 동작 불변')
    parser.add_argument('--file-gib', type=int, default=1)
    parser.add_argument('--workloads', default=','.join(WORKLOADS))
    parser.add_argument('--bs', choices=('4k', '1M'), default=None,
                        help='override request size; omit for legacy workload defaults')
    parser.add_argument('--qds', default='1,4,16,32')
    parser.add_argument('--warmup', type=int, default=30)
    parser.add_argument('--measure', type=int, default=120)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--dm-name')
    parser.add_argument('--devices', default='nvme0n1,dm-0')
    parser.add_argument('--femu-csv', action='append', default=[])
    parser.add_argument('--output-dir')
    parser.add_argument('--resume', action='store_true', help='resume existing output and bounded data file without initialization')
    parser.add_argument('--run', action='store_true', help='perform writes; default is dry-run')
    args = parser.parse_args()
    fixed = args.evaluation == 'gc-efficiency'
    if args.backend == 'native-f2fs-zns' and (args.prepare or args.filesystem != 'f2fs' or args.dm_name):
        parser.error('native requires separately prepared F2FS mount and forbids --prepare/--dm-name')
    if args.wal_delay_ms is not None and (not args.prepare or args.backend != 'dm-zns'):
        parser.error('--wal-delay-ms requires --prepare --backend dm-zns')
    names = args.workloads.split(',')
    try:
        qds = [int(value) for value in args.qds.split(',')]
    except ValueError:
        parser.error('qds must be positive integers')
    if any(name not in WORKLOADS for name in names) or not qds or any(q < 1 for q in qds):
        parser.error('invalid workload or queue depth')
    if min(args.file_gib, args.measure, args.rounds) < 1 or args.warmup < 0:
        parser.error('size, measure and rounds must be positive; warmup >= 0')
    if fixed:
        if args.total_write_gib is None or args.total_write_gib < 1:
            parser.error('1-2 requires positive --total-write-gib')
        if args.warmup or args.resume or len(names) != 1 or len(qds) != 1 or args.rounds != 1:
            parser.error('1-2 requires warmup=0, one workload/QD/round, no resume; fresh state per case')
        if names[0] not in ('seq-write', 'rand-write') or args.safety_timeout < 1:
            parser.error('1-2 supports write-only workloads and positive safety timeout')
        if args.randseed is None:
            parser.error('1-2 requires explicit --randseed')
    elif args.total_write_gib is not None:
        parser.error('--total-write-gib is only valid for 1-2')
    mount = Path(args.mount).resolve()
    output = Path(args.output_dir or ROOT / 'results' / f'fio_{dt.datetime.now():%Y%m%d_%H%M%S}').resolve()
    plan = {**vars(args), 'mount': str(mount), 'output_dir': str(output),
            'file_bytes': args.file_gib * 1024**3, 'phase': 'dry-run' if not args.run else 'run'}
    plan.update(experiment_id='1-2' if fixed else '1-1',
                termination='fixed-write-bytes' if fixed else 'fixed-time',
                measurement_seconds=None if fixed else args.measure,
                total_write_bytes=args.total_write_gib * 1024**3 if fixed else None,
                interpretation='동일 작업량 비용' if fixed else '고정 QD/단일 작업 처리 성능; 절대 최대 성능 아님')
    print(json.dumps(plan, indent=2))
    if not args.run:
        return 0
    if mount == Path('/'):
        parser.error('guest root cannot be an experiment mount')
    if args.resume and (args.prepare or not output.is_dir()):
        parser.error('resume requires existing results and forbids device initialization')
    if args.resume:
        previous = json.loads((output / 'manifest.json').read_text())
        if previous.get('evaluation', 'performance') != args.evaluation or previous.get('randseed') != args.randseed:
            parser.error('resume evaluation/seed mismatch')
        for key in ('mount', 'backend', 'filesystem', 'file_gib', 'workloads', 'qds', 'warmup', 'measure', 'rounds', 'bs'):
            if previous.get(key) != plan.get(key):
                parser.error(f'resume configuration mismatch: {key}')
    if output.exists() and not args.resume:
        parser.error('output directory already exists')
    if args.prepare and output.is_relative_to(mount):
        parser.error('results cannot be inside the filesystem being initialized')
    if not shutil.which('fio'):
        parser.error('fio is not installed')
    version = subprocess.run(['fio', '--version'], capture_output=True, text=True, check=True)
    if not version.stdout.strip().startswith('fio-'):
        parser.error('fio executable is not the Flexible I/O Tester')
    for command in (['pgrep', '-x', 'fio'], ['pgrep', '-f', 'kafka[.]Kafka']):
        if subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            parser.error('another fio or Kafka broker is active; stop it before evaluation')
    lock = open('/tmp/kovalt-fio-evaluation.lock', 'a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('another fio evaluation owns the lock')
    plan['fio_version'] = version.stdout.strip()
    revision = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'],
                              capture_output=True, text=True)
    plan['runner_commit'] = revision.stdout.strip() if revision.returncode == 0 else None
    plan['runner_files_sha256'] = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for folder, suffix in [('src', '*.py'), ('scripts', '*.sh')]
        for path in sorted((ROOT / folder).glob(suffix))
    }
    if args.prepare:
        print('[Prepare] Initializing configured FEMU device; previous experiment data will be erased.')
        plan['storage_setup'] = prepare_storage(args, mount)
        plan['dm_name'] = args.dm_name
    check = subprocess.run(['findmnt', '-n', '-o', 'TARGET', '--target', str(mount)],
                           capture_output=True, text=True, check=True)
    if Path(check.stdout.strip()).resolve() != mount or mount == Path('/'):
        parser.error('mount must be a separate mounted filesystem, never guest root')
    if args.backend == 'native-f2fs-zns':
        source = subprocess.run(['findmnt', '-n', '-o', 'SOURCE,FSTYPE', '--target', str(mount)],
                                capture_output=True, text=True, check=True).stdout.split()
        if len(source) != 2 or not re.fullmatch(r'/dev/nvme\d+n\d+', source[0]) or source[1] != 'f2fs':
            parser.error('native requires preconfigured multi-device F2FS, not DM/root')
        if (Path('/sys/block/nvme1n1/queue/zoned').read_text().strip() != 'host-managed'
                or Path('/sys/block/nvme0n1/queue/zoned').read_text().strip() != 'none'
                or set(args.devices.split(',')) != {'nvme0n1', 'nvme1n1'}):
            parser.error('native requires explicit team4 CNS nvme0n1 + ZNS nvme1n1 accounting')
        for device in ('/dev/nvme0n1', '/dev/nvme1n1'):
            model = subprocess.run(['lsblk', '-dn', '-o', 'MODEL', device],
                                   capture_output=True, text=True, check=True).stdout
            if 'FEMU' not in model:
                parser.error('native experiment requires both devices to be FEMU')
        plan['native_mount_source'] = source
        plan['effective_io_limit'] = 'direct=1은 요청값. Native F2FS buffered fallback 가능; 실제 경로 별도 검증'
    existing_parent = output
    while not existing_parent.exists():
        existing_parent = existing_parent.parent
    if existing_parent.stat().st_dev == mount.stat().st_dev:
        parser.error('results must be on a different filesystem from the test file')
    usage = os.statvfs(mount)
    size = args.file_gib * 1024**3
    if not args.resume and (size > usage.f_bavail * usage.f_frsize or (usage.f_blocks - usage.f_bfree) * usage.f_frsize + size > usage.f_blocks * usage.f_frsize * .8):
        parser.error('test file would exceed 80% usage; include file size in prefill budget')
    output.mkdir(parents=True, exist_ok=args.resume)
    (output / ('resume-manifest.json' if args.resume else 'manifest.json')).write_text(json.dumps(plan, indent=2))
    data_dir = mount / f'.kovalt-{output.name}'
    data_dir.mkdir(exist_ok=args.resume)
    file = data_dir / 'data.bin'
    # Fix the extent range first; the following direct writes initialize every
    # byte. This is not used as a substitute for real-data preparation.
    if args.resume:
        if not file.is_file() or file.stat().st_size != size:
            parser.error('resume data file missing or size mismatch')
    else:
        with file.open('xb') as stream:
            stream.truncate(size)
    preparation = fio_command(file, size, 'write', '1M', 4, 0, 0, output / 'prepare.json', False)
    if not args.resume:
        run_checked(preparation, max(600, args.file_gib * 600))
    records = []
    for name in names:
        for qd in qds:
            for repetition in range(1, args.rounds + 1):
                tag = f'{name}-qd{qd}-r{repetition}'
                sampler = Sampler(mount, args.devices.split(','), args.dm_name, output / f'{tag}.samples.jsonl')
                raw = output / f'{tag}.fio.json'
                if args.resume and raw.exists():
                    payload = json.loads(raw.read_text())
                    if any(job.get('error') for job in payload.get('jobs', [])):
                        raise RuntimeError('cannot resume from failed fio measurement')
                    parse_fio(payload)  # reject incomplete or failed measurements
                    sampler.rows = [json.loads(line) for line in sampler.destination.read_text().splitlines()]
                    start = sampler.rows[0]['epoch_us']
                    end = payload['timestamp_ms'] * 1000
                else:
                    if fixed:
                        # 준비 쓰기 및 검증/metadata를 본 측정 비용에 섞지 않는다.
                        run_checked(['sync', '-f', str(file)], 120)
                        f2fs_before = f2fs_cost_snapshot() if args.filesystem == 'f2fs' else None
                        sampler.boundary()
                    sampler.thread.start()
                    start = sampler.rows[0]['epoch_us'] if fixed else time.time_ns() // 1000
                    rw, default_bs = WORKLOADS[name]
                    command = fio_command(file, size, rw, args.bs or default_bs, qd, args.warmup, args.measure, raw,
                                          total_write_bytes=plan['total_write_bytes'], randseed=args.randseed)
                    try:
                        run_checked(command, args.safety_timeout if fixed else args.warmup + args.measure + 120, sampler)
                        if fixed:
                            validate_fixed_write(json.loads(raw.read_text()), plan['total_write_bytes'])
                            run_checked(['sync', '-f', str(file)], 120, sampler)
                    except Exception as exc:
                        if fixed:
                            (output / f'{tag}.incomplete.json').write_text(json.dumps(dict(
                                experiment_id='1-2', fixed_work_complete=False,
                                expected_write_bytes=plan['total_write_bytes'], error=str(exc)), indent=2))
                        raise
                    finally:
                        sampler.stop.set()
                        sampler.thread.join(timeout=5)
                    if fixed:
                        if sampler.thread.is_alive():
                            raise RuntimeError('sampler did not stop; fixed-work boundary is invalid')
                        sampler.boundary()
                        f2fs_after = f2fs_cost_snapshot() if args.filesystem == 'f2fs' else None
                        (output / f'{tag}.boundaries.json').write_text(json.dumps(
                            [sampler.rows[0], sampler.rows[-1]], indent=2))
                    end = time.time_ns() // 1000
                record = {'workload': name, 'qd': qd, 'round': repetition,
                          'experiment_id': plan['experiment_id'], 'evaluation': args.evaluation,
                          'termination': plan['termination'], 'expected_write_bytes': plan['total_write_bytes'],
                          'wall_seconds': (end - start) / 1e6,
                          'block_size': args.bs or WORKLOADS[name][1],
                          'start_us': start, 'measurement_start_us_approx': start + args.warmup * 1000000,
                          'end_us': end, **parse_fio(json.loads(raw.read_text()))}
                record['femu'] = {path: femu_delta(path, record['measurement_start_us_approx'], end)
                                  for path in args.femu_csv}
                rows = [row for row in sampler.rows if row['epoch_us'] >= record['measurement_start_us_approx']]
                record['os'] = os_metrics(rows)
                try:
                    kernel = subprocess.run(['sudo', '-n', 'journalctl', '-k', '--no-pager',
                                         '--since', f'@{start // 1000000}', '--until', f'@{end // 1000000}'],
                                            capture_output=True, text=True, timeout=60)
                except subprocess.TimeoutExpired as exc:
                    partial = exc.stdout or b''
                    if isinstance(partial, bytes):
                        partial = partial.decode(errors='replace')
                    kernel = subprocess.CompletedProcess([], 124, partial,
                        'Kernel log collection timed out; coverage incomplete, not an error-free verdict.\n')
                (output / f'{tag}.kernel.log').write_text(kernel.stdout + kernel.stderr)
                record['kernel_log_available'] = kernel.returncode == 0
                storage_errors = re.findall(r'^.*(?:nvme.*timeout|Buffer I/O error|EXT4-fs error|F2FS-fs.*(?:ERROR|corrupt)).*$',
                                            kernel.stdout, flags=re.M | re.I)
                record['kernel_storage_errors'] = storage_errors
                record['dm_available'] = bool(len(rows) > 1 and rows[0].get('dm', {}).get('metrics') == 1
                                              and rows[-1].get('dm', {}).get('metrics') == 1)
                if record['dm_available']:
                    a, b = rows[0]['dm'], rows[-1]['dm']
                    delta = {key: b[key] - a[key] for key in b.keys() & a.keys()
                             if key not in ('metrics', 'mapping_resident_bytes', 'mapping_sampled_peak_bytes', 'zones', 'zone_sectors',
                                            'gc_scratch_requested_bytes', 'compaction_scratch_requested_bytes')}
                    for gauge in ('free_zones', 'memtable_entries', 'frozen_entries', 'sstable_count'):
                        delta.pop(gauge, None)
                    if any(value < 0 for value in delta.values()):
                        raise RuntimeError('DM counters reset during measurement')
                    record['dm_delta'] = delta
                    if a.get('wal_diag') == b.get('wal_diag') == 1:
                        record['wal_diagnostics'] = wal_diagnostics(delta)
                    written = delta.get('logical_write_bytes', 0)
                    lower = sum(delta.get(key, 0) for key in ('user_write_bytes', 'gc_write_bytes', 'wal_write_bytes', 'sstable_write_bytes'))
                    record['dm_wa_issued'] = lower / written if written else None
                    record['mapping_resident_sampled_max_bytes'] = max(row.get('dm', {}).get('mapping_resident_bytes', 0) for row in rows)
                    record['gc_scratch_sampled_max_bytes'] = max(row.get('dm', {}).get('gc_scratch_requested_bytes', 0) for row in rows)
                    record['compaction_scratch_sampled_max_bytes'] = max(row.get('dm', {}).get('compaction_scratch_requested_bytes', 0) for row in rows)
                if fixed:
                    record['fixed_work_complete'] = True
                    record['fixed_work_costs'] = fixed_work_costs(record)
                    if f2fs_before is not None:
                        snapshots = dict(before=f2fs_before, after=f2fs_after)
                        (output / f'{tag}.f2fs-boundaries.json').write_text(json.dumps(snapshots, indent=2))
                        if f2fs_before['available'] and f2fs_after['available']:
                            delta = {k: v - f2fs_before['counters'][k] for k, v in f2fs_after['counters'].items()}
                            if any(v < 0 for v in delta.values()):
                                raise RuntimeError('F2FS counters reset during fixed work')
                            record['f2fs_fixed_work_delta'] = delta
                        else:
                            record['f2fs_fixed_work_delta'] = None
                records.append(record)
                (output / 'summary.json').write_text(json.dumps(records, indent=2))
                columns = ['workload', 'block_size', 'qd', 'round', 'read_iops', 'write_iops', 'read_mib_s', 'write_mib_s',
                           'read_p95_ms', 'read_p99_ms', 'write_p95_ms', 'write_p99_ms', 'dm_wa_issued']
                with (output / 'summary.csv').open('w', newline='') as stream:
                    writer = csv.DictWriter(stream, columns, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(records)
                if storage_errors:
                    raise RuntimeError('kernel storage error; higher loads were not attempted')
    print(f'Results: {output}; bounded test file retained at {file}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

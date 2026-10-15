"""Bounded filesystem fio evaluation; never formats or writes a raw device."""
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


def fio_command(file, size, rw, bs, qd, ramp, runtime, output, timed=True):
    command = ['fio', '--name=kovalt-fio', f'--filename={file}', f'--size={size}',
               f'--rw={rw}', f'--bs={bs}', '--ioengine=libaio', '--direct=1',
               f'--iodepth={qd}', '--numjobs=1', '--group_reporting=1',
               '--allow_file_create=0', '--end_fsync=1', '--exitall_on_error=1',
               '--percentile_list=50:95:99:99.9', '--output-format=json',
               f'--output={output}']
    if timed:
        prefix = str(output).removesuffix('.json')
        command += [f'--write_iops_log={prefix}', f'--write_bw_log={prefix}',
                    f'--write_lat_log={prefix}', '--log_avg_msec=1000', '--log_unix_epoch=1']
    if timed:
        command += ['--time_based=1', f'--runtime={runtime}', f'--ramp_time={ramp}']
    else:
        command += ['--verify=crc32c', '--do_verify=1', '--verify_fatal=1']
    if rw == 'randrw':
        command += ['--rwmixread=70']
    return command


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
        result[f'{direction}_mean_ms'] = latency.get('mean', 0) / 1e6
        for percentile in ('95', '99', '99.9'):
            value = next((v for k, v in percentiles.items() if float(k) == float(percentile)), None)
            result[f'{direction}_p{percentile}_ms'] = value / 1e6 if value is not None else None
    result['iodepth_level'] = job.get('iodepth_level', {})
    result['fio_cpu_user_pct'] = job.get('usr_cpu')
    result['fio_cpu_system_pct'] = job.get('sys_cpu')
    return result


def dm_metrics(text):
    return {key: int(value) for key, value in re.findall(r'\b([a-z_]+)=(\d+)\b', text)}


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
        args.dm_name = cfg.DM_NAME
    system_setup.setup_filesystem(args.filesystem)
    if args.backend == 'dm-zns':
        status = subprocess.run(['sudo', '-n', 'dmsetup', 'status', cfg.DM_NAME],
                                capture_output=True, text=True, check=True)
        if dm_metrics(status.stdout).get('metrics') != 1:
            raise RuntimeError('DM metrics did not activate after setup')
    return {'backend': args.backend, 'filesystem': args.filesystem,
            'raw_device': cfg.RAW_ZNS_DEVICE, 'filesystem_device': cfg.FS_DEVICE,
            'module_path': str(chosen) if args.backend == 'dm-zns' else None,
            'module_sha256': hashlib.sha256(chosen.read_bytes()).hexdigest()
            if args.backend == 'dm-zns' else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mount', default='/result/kafka-logs')
    parser.add_argument('--prepare', action='store_true', help='initialize FEMU, create DM, mkfs and mount (destroys old data)')
    parser.add_argument('--backend', choices=('dm-zns', 'cns'), default='dm-zns')
    parser.add_argument('--filesystem', choices=('ext4', 'f2fs'), default='ext4')
    parser.add_argument('--module-path', help='guest-built DM module with metrics_enabled support')
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
    names = args.workloads.split(',')
    try:
        qds = [int(value) for value in args.qds.split(',')]
    except ValueError:
        parser.error('qds must be positive integers')
    if any(name not in WORKLOADS for name in names) or not qds or any(q < 1 for q in qds):
        parser.error('invalid workload or queue depth')
    if min(args.file_gib, args.measure, args.rounds) < 1 or args.warmup < 0:
        parser.error('size, measure and rounds must be positive; warmup >= 0')
    mount = Path(args.mount).resolve()
    output = Path(args.output_dir or ROOT / 'results' / f'fio_{dt.datetime.now():%Y%m%d_%H%M%S}').resolve()
    plan = {**vars(args), 'mount': str(mount), 'output_dir': str(output),
            'file_bytes': args.file_gib * 1024**3, 'phase': 'dry-run' if not args.run else 'run'}
    print(json.dumps(plan, indent=2))
    if not args.run:
        return 0
    if mount == Path('/'):
        parser.error('guest root cannot be an experiment mount')
    if args.resume and (args.prepare or not output.is_dir()):
        parser.error('resume requires existing results and forbids device initialization')
    if args.resume:
        previous = json.loads((output / 'manifest.json').read_text())
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
                    sampler.thread.start()
                    start = time.time_ns() // 1000
                    rw, default_bs = WORKLOADS[name]
                    command = fio_command(file, size, rw, args.bs or default_bs, qd, args.warmup, args.measure, raw)
                    try:
                        run_checked(command, args.warmup + args.measure + 120, sampler)
                    finally:
                        sampler.stop.set()
                        sampler.thread.join(timeout=5)
                    end = time.time_ns() // 1000
                record = {'workload': name, 'qd': qd, 'round': repetition,
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
                    written = delta.get('logical_write_bytes', 0)
                    lower = sum(delta.get(key, 0) for key in ('user_write_bytes', 'gc_write_bytes', 'wal_write_bytes', 'sstable_write_bytes'))
                    record['dm_wa_issued'] = lower / written if written else None
                    record['mapping_resident_sampled_max_bytes'] = max(row.get('dm', {}).get('mapping_resident_bytes', 0) for row in rows)
                    record['gc_scratch_sampled_max_bytes'] = max(row.get('dm', {}).get('gc_scratch_requested_bytes', 0) for row in rows)
                    record['compaction_scratch_sampled_max_bytes'] = max(row.get('dm', {}).get('compaction_scratch_requested_bytes', 0) for row in rows)
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

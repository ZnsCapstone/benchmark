#!/usr/bin/env python3
"""team4 CNS fio: 매 회차 FEMU 재기동으로 FTL 초기화. 로컬에서 실행."""
import argparse
import csv
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
GUEST_ROOT = '/home/zns/kafka_python_result'
HOST_ROOT = '/home/zns-team4'
FEMU = HOST_ROOT + '/femu/src/build-femu/qemu-system-x86_64'
EXPECTED_FEMU_SHA = 'ae27d095f0fe527ae4fc8488590d1ec10279f38cee3db6fc376ae8a02eec0246'
RELAY = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=6',
         '-o', 'StrictHostKeyChecking=yes', '-o', 'ServerAliveInterval=15',
         '-o', 'ServerAliveCountMax=3', '-o',
         'UserKnownHostsFile=/home/loloy/.ssh/femu-host-relay-known-hosts',
         '-p', '12223', 'zns-team4@192.168.67.1']
GUEST = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=6',
         '-o', 'StrictHostKeyChecking=yes', '-o', 'ServerAliveInterval=15',
         '-o', 'ServerAliveCountMax=3', '-o',
         'UserKnownHostsFile=/home/loloy/.ssh/femu-guest-known-hosts', '-o',
         'ProxyCommand=ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=yes '
         '-o UserKnownHostsFile=/home/loloy/.ssh/femu-host-relay-known-hosts '
         '-p 12223 zns-team4@192.168.67.1 -W 127.0.0.1:2204', 'zns@cns-team4']


def call(target, command, timeout=60, check=True):
    return subprocess.run(['ssh', *(RELAY if target == 'host' else GUEST), command],
                          capture_output=True, text=True, timeout=timeout, check=check)


def save(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def make_plan():
    spec = importlib.util.spec_from_file_location('zns_plan', Path(__file__).with_name('run-fio-zns-suite.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.plan(5, 1)


def femu_delta(path, start_us, end_us):
    with path.open() as f:
        rows = [r for r in csv.DictReader(f) if start_us <= int(r['epoch_us']) <= end_us]
    if len(rows) < 2:
        raise RuntimeError('CNS FEMU 통계의 측정 구간 표본 부족')
    keys = ['host_write_bytes', 'host_read_bytes', 'nand_read_pages',
            'nand_program_pages', 'nand_erase_blocks', 'gc_moved_pages', 'gc_count', 'gc_host_ns']
    delta = {k: int(rows[-1][k]) - int(rows[0][k]) for k in keys}
    if any(v < 0 for v in delta.values()):
        raise RuntimeError('CNS FEMU counter reset 감지')
    page_bytes = int(rows[-1]['page_bytes'])
    program = delta['nand_program_pages'] * page_bytes
    written = delta['host_write_bytes']
    return dict(available=True, delta=delta, nand_program_bytes=program,
                gc_moved_bytes=delta['gc_moved_pages'] * page_bytes,
                nand_wa_device=program / written if written else None,
                sample_start_us=int(rows[0]['epoch_us']), sample_end_us=int(rows[-1]['epoch_us']),
                boundary_resolution='FEMU 1초 표본; fio 측정 구간 내부 표본 차분')


def validate_cell(row, payload):
    jobs = payload.get('jobs', [])
    if len(jobs) != 1 or jobs[0].get('error', 0):
        raise RuntimeError('fio job 오류 또는 결과 불완전')
    if not row.get('kernel_log_available') or row.get('kernel_storage_errors'):
        raise RuntimeError('커널 로그 수집 실패 또는 저장장치 오류')
    if row.get('dm_available'):
        raise RuntimeError('CNS 실험에 DM이 개입함')
    steal = row.get('os', {}).get('cpu_steal_pct')
    return dict(cpu_steal_pct=steal, host_interference_warning=steal is None or steal > 1)


def check_manifest(manifest, cell):
    expected = dict(backend='cns', filesystem=cell['filesystem'], workloads=cell['workload'],
                    bs=cell['bs'], qds='32', rounds=1, file_gib=cell['file_gib'],
                    warmup=cell['warmup'], measure=cell['measure'])
    if any(manifest.get(k) != v for k, v in expected.items()):
        raise RuntimeError('회차 원본 manifest와 재개 계획 불일치')


def collect_cell(local, destination, host_cell, cell, index):
    manifest = json.loads(call('guest', 'cat ' + shlex.quote(destination + '/manifest.json')).stdout)
    check_manifest(manifest, cell)
    summary = json.loads(call('guest', 'cat ' + shlex.quote(destination + '/summary.json')).stdout)
    if len(summary) != 1:
        raise RuntimeError('summary 수 불일치')
    raw_text = call('guest', 'cat ' + shlex.quote(destination + '/' +
                    cell['workload'] + '-qd32-r1.fio.json')).stdout
    raw = json.loads(raw_text)
    validation = validate_cell(summary[0], raw)
    if raw['jobs'][0].get('write', {}).get('runtime', 0) < cell['measure'] * 1000 * .95:
        raise RuntimeError('fio 측정 runtime 불완전')
    stats_file = local / f'cell-{index:02}.femu.csv'
    scp_options = RELAY[:-3] + ['-P', RELAY[-2]]
    subprocess.run(['scp', *scp_options,
                    'zns-team4@192.168.67.1:' + host_cell + '/stats.csv', str(stats_file)],
                   check=True, timeout=60)
    summary[0]['femu'] = {host_cell + '/stats.csv': femu_delta(
        stats_file, summary[0]['measurement_start_us_approx'], summary[0]['end_us'])}
    return dict(cell=cell, output=destination, host_cell=host_cell,
                summary=summary[0], raw_sha256=hashlib.sha256(raw_text.encode()).hexdigest(), **validation)


def run_detached(command, destination, index, timeout):
    """SSH 접속 수명과 fio 실행 수명을 분리하고 완료 artifact를 확인한다."""
    session = f'cns-fio-cell-{index:02}'
    log = destination + '.launch.log'
    shell = shlex.join(command) + ' > ' + shlex.quote(log) + ' 2>&1'
    # 데이터 결과 폴더 자체는 fio runner만 생성한다.
    parent = str(Path(destination).parent)
    launch = 'mkdir -p ' + shlex.quote(parent) + ' && ' + shlex.join(
        ['tmux', 'new-session', '-d', '-s', session, 'bash', '-lc', shell])
    result = call('guest', launch, check=False)
    if result.returncode not in (0, 255):
        raise RuntimeError('detached fio 시작 실패: ' + result.stderr)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(30)
        check = ('if test -s ' + shlex.quote(destination + '/summary.json')
                 + '; then echo COMPLETE; elif tmux has-session -t ' + shlex.quote(session)
                 + ' 2>/dev/null; then echo RUNNING; else echo STOPPED; fi')
        try:
            result = call('guest', check, timeout=20, check=False)
        except (subprocess.TimeoutExpired, OSError):
            continue
        if result.returncode == 255:
            continue
        if result.returncode:
            raise RuntimeError('fio 상태 확인 실패: ' + result.stderr)
        if result.stdout.strip() == 'COMPLETE':
            return call('guest', 'cat ' + shlex.quote(log)).stdout
        if result.stdout.strip() == 'STOPPED':
            details = call('guest', 'tail -50 ' + shlex.quote(log), check=False).stdout
            raise RuntimeError('fio 세션이 결과 없이 종료됨: ' + details)
    raise RuntimeError('detached fio 전체 제한 시간 초과; 실행 상태를 보존하고 중단')


def active_guest_guard():
    command = ('test "$(hostname)" = zns-guest && '
               '! pgrep -x fio && ! pgrep -f "kafka[.]Kafka|com[.]hanyang[.]cs[.]KafkaBenchmark"')
    call('guest', command, timeout=20)


def shutdown_guest():
    active_guest_guard()
    call('guest', 'set -e; if mountpoint -q /result/kafka-logs; then sudo -n umount /result/kafka-logs; fi; '
         'sudo -n sync', timeout=60)
    # SSH 종료는 정상 poweroff의 예상 결과지만 이후 host process 종료를 별도 확인한다.
    result = call('guest', 'sudo -n poweroff', timeout=30, check=False)
    if result.returncode not in (0, 255):
        raise RuntimeError('게스트 poweroff 실패: ' + result.stderr)
    for _ in range(45):
        result = call('host', "pgrep -a -u zns-team4 -f '[q]emu-system-x86_64'", timeout=15, check=False)
        if result.returncode == 1:
            return
        if result.returncode != 0:
            raise RuntimeError('FEMU 종료 확인 중 호스트 연결 오류')
        time.sleep(2)
    raise RuntimeError('정상 poweroff 뒤 team4 FEMU가 종료되지 않음; 강제 종료하지 않음')


def boot_cell(host_cell, index, name):
    result = call('host', "pgrep -a -u zns-team4 -f '[q]emu-system-x86_64'", check=False)
    if result.returncode != 1:
        raise RuntimeError('다른 team4 FEMU가 존재하거나 호스트 확인 실패')
    actual_sha = call('host', 'sha256sum ' + shlex.quote(FEMU)).stdout.split()[0]
    if actual_sha != EXPECTED_FEMU_SHA:
        raise RuntimeError('FEMU 바이너리 변경 감지')
    call('host', 'test ! -e ' + shlex.quote(host_cell) + ' && mkdir -p ' + shlex.quote(host_cell))
    shell = ('env FEMU_BIN=' + shlex.quote(FEMU) + ' FEMU_STATS_FILE=' + shlex.quote(host_cell + '/stats.csv')
             + ' bash ' + shlex.quote(HOST_ROOT + '/run-cns.sh') + ' > '
             + shlex.quote(host_cell + '/console.log') + ' 2>&1; '
             + 'rc=$?; printf "%s\\n" "$rc" > ' + shlex.quote(host_cell + '/exit-code.txt'))
    command = shlex.join(['tmux', 'new-session', '-d', '-s', f'{name}-{index:02}', 'bash', '-lc', shell])
    call('host', command)
    for _ in range(80):
        result = call('guest', 'test -e /sys/block/nvme0n1/queue/zoned && '
                      'test "$(cat /sys/block/nvme0n1/queue/zoned)" = none && '
                      'test "$(cat /sys/block/nvme0n1/size)" = 50331648 && sudo -n true',
                      timeout=15, check=False)
        if result.returncode == 0:
            return
        time.sleep(3)
    raise RuntimeError('CNS 부팅·장치 확인 제한 초과')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', default='cns-fio-short5-gc1-20261016')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--resume', action='store_true', help='검증된 완료분을 보존하고 이어 실행')
    args = parser.parse_args()
    if not args.name.replace('-', '').replace('_', '').isalnum():
        parser.error('실험 이름에는 영숫자·하이픈·밑줄만 허용')
    cells = make_plan()
    local = ROOT / 'results/cns' / args.name
    guest_root = GUEST_ROOT + '/results/cns/' + args.name
    host_root = HOST_ROOT + '/results/' + args.name
    print(json.dumps(dict(cells=cells, local=str(local), guest=guest_root,
                         host=host_root, fresh_femu_per_cell=True), ensure_ascii=False, indent=2), flush=True)
    if not args.run:
        return 0
    lock = open('/tmp/kovalt-cns-fio-controller.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if args.resume:
        previous_plan = json.loads((local / 'plan.json').read_text())
        if previous_plan.get('cells') != cells or previous_plan.get('femu_sha256') != EXPECTED_FEMU_SHA:
            parser.error('기존 계획·FEMU와 재개 조건 불일치')
        completed = json.loads((local / 'completed-cells.json').read_text())
        if [r['cell'] for r in completed] != cells[:len(completed)]:
            parser.error('완료 회차가 계획의 연속 prefix와 다릅니다')
        history = local / ('resume-history-' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        history.mkdir()
        for name in ('status.json', 'exit-code.json', 'completed-cells.json'):
            if (local / name).exists():
                shutil.copy2(local / name, history / name)
        if (local / 'exit-code.json').exists():
            (local / 'exit-code.json').unlink()
    else:
        local.mkdir(parents=True, exist_ok=False)
        completed = []
        save(local / 'plan.json', dict(cells=cells, fresh_femu_per_cell=True,
                                 physical_gib=32, logical_gib=24, discard=False,
                                 femu_sha256=EXPECTED_FEMU_SHA,
                                 cpu_ram='8 vCPU / 16GiB; same team4 qcow2',
                                 statistics_scope='CNS model NAND WA; not ZNS DM WA'))
    try:
        launcher = call('host', 'cat ' + HOST_ROOT + '/run-cns.sh').stdout
        if args.resume and (local / 'run-cns.sh.snapshot').read_text() != launcher:
            raise RuntimeError('CNS launcher 변경 감지')
        (local / 'run-cns.sh.snapshot').write_text(launcher)
        (local / 'femu-provenance.txt').write_text(call('host',
            'git -C ' + HOST_ROOT + '/femu/src rev-parse HEAD; sha256sum '
            + FEMU + ' ' + HOST_ROOT + '/run-cns.sh').stdout)
        previous = json.loads(call('guest', 'cat ' + GUEST_ROOT
                                  + '/results/zns/fio-short5-gc1-20261015/status.json').stdout)
        if previous.get('state') != 'completed':
            raise RuntimeError('기존 ZNS suite 미완료')
        active_guest_guard()
        if args.resume and len(completed) < len(cells):
            # 단절된 직후 회차가 게스트에서 완료된 경우에만 재실행 없이 수집한다.
            index = len(completed) + 1
            cell = cells[index - 1]
            destination = guest_root + '/' + cell['filesystem'] + '/' + cell['group'] + '/' + (
                f"{cell['workload']}-{cell['bs']}-qd32-r{cell['repeat']}")
            exists = call('guest', 'test -s ' + shlex.quote(destination + '/summary.json'), check=False)
            if exists.returncode == 0:
                record = collect_cell(local, destination, host_root + f'/cell-{index:02}', cell, index)
                record['recovered_after_ssh_disconnect'] = True
                completed.append(record)
                save(local / 'completed-cells.json', completed)
                print(f'[{index}/{len(cells)}] 완료 원본 검증·수집; 재실행하지 않음', flush=True)
            elif exists.returncode != 1:
                raise RuntimeError('재개할 회차 존재 여부 확인 실패')
        if len(completed) == len(cells):
            save(local / 'status.json', dict(state='completed', completed=len(completed), total=len(cells)))
            save(local / 'exit-code.json', dict(exit_code=0))
            return 0
        shutdown_guest()
        for index, cell in enumerate(cells, 1):
            if index <= len(completed):
                continue
            host_cell = host_root + f'/cell-{index:02}'
            destination = guest_root + '/' + cell['filesystem'] + '/' + cell['group'] + '/' + (
                f"{cell['workload']}-{cell['bs']}-qd32-r{cell['repeat']}")
            status = dict(state='booting', index=index, total=len(cells), completed=len(completed),
                          cell=cell, destination=destination, host_statistics=host_cell + '/stats.csv')
            save(local / 'status.json', status)
            print(f'[{index}/{len(cells)}] 새 CNS FEMU: {destination}', flush=True)
            boot_cell(host_cell, index, args.name)
            # 첫 표본은 fresh FTL의 근거로 보존하고 누적 기록이 있으면 거절한다.
            first = call('host', 'head -2 ' + shlex.quote(host_cell + '/stats.csv')).stdout
            initial = list(csv.DictReader(first.splitlines()))
            if len(initial) != 1 or int(initial[0]['host_write_bytes']) or int(initial[0]['gc_count']):
                raise RuntimeError('CNS 초기 FTL 통계가 비어 있지 않음')
            status['state'] = 'running'
            save(local / 'status.json', status)
            command = ['env', 'BENCH_STORAGE_BACKEND=cns', 'BENCH_F2FS_DISCARD=0',
                       'BENCH_EXT4_DISCARD=0', 'PYTHONUNBUFFERED=1', 'python3',
                       GUEST_ROOT + '/src/fio_evaluation.py', '--prepare', '--backend', 'cns',
                       '--filesystem', cell['filesystem'], '--workloads', cell['workload'],
                       '--bs', cell['bs'], '--qds', '32', '--rounds', '1', '--devices', 'nvme0n1',
                       '--file-gib', str(cell['file_gib']), '--warmup', str(cell['warmup']),
                       '--measure', str(cell['measure']), '--output-dir', destination, '--run']
            # fio 자체 준비/측정 제한 외에 SSH 전체 실행 제한도 둔다.
            output = run_detached(command, destination, index, cell['file_gib'] * 600 + 1800)
            (local / f'cell-{index:02}.log').write_text(output)
            record = collect_cell(local, destination, host_cell, cell, index)
            completed.append(record)
            save(local / 'completed-cells.json', completed)
            # 집계·상태를 guest 결과 폴더에도 보존한다. 원본 summary는 수정하지 않는다.
            subprocess.run(['ssh', *GUEST, 'mkdir -p ' + shlex.quote(guest_root)], check=True, timeout=20)
            subprocess.run(['scp', *GUEST[:-1], str(local / 'completed-cells.json'),
                            'zns@cns-team4:' + guest_root + '/completed-cells.json'], check=True, timeout=60)
            status.update(state='cell-completed', completed=len(completed))
            save(local / 'status.json', status)
            subprocess.run(['scp', *GUEST[:-1], str(local / 'status.json'), str(local / 'plan.json'),
                            'zns@cns-team4:' + guest_root + '/'], check=True, timeout=60)
            if index != len(cells):
                shutdown_guest()
        save(local / 'status.json', dict(state='completed', completed=len(completed), total=len(cells)))
        save(local / 'exit-code.json', dict(exit_code=0))
        subprocess.run(['scp', *GUEST[:-1], str(local / 'status.json'), str(local / 'exit-code.json'),
                        'zns@cns-team4:' + guest_root + '/'], check=True, timeout=60)
        return 0
    except BaseException as exc:
        save(local / 'status.json', dict(state='failed', completed=len(completed),
                                        total=len(cells), error=str(exc)))
        save(local / 'exit-code.json', dict(exit_code=1))
        raise


if __name__ == '__main__':
    raise SystemExit(main())

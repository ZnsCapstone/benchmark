#!/usr/bin/env python3
"""ZNS 독립 fio 반복: short를 많이, 18GiB GC를 적게. 기본은 계획만 출력."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CELLS = [('seq-write', '4k'), ('rand-write', '4k'), ('seq-write', '1M')]


def plan(short_repeats, gc_repeats):
    cells = []
    for group, repeats, size, warmup, measure in [
            ('short', short_repeats, 1, 20, 120),
            ('gc18', gc_repeats, 18, 30, 600)]:
        for repeat in range(1, repeats + 1):
            for workload, bs in CELLS:
                # FS를 가까이 배치하고 반복마다 먼저 실행하는 FS를 교대한다.
                filesystems = ('ext4', 'f2fs') if repeat % 2 else ('f2fs', 'ext4')
                for fs in filesystems:
                    cells.append(dict(group=group, repeat=repeat, filesystem=fs,
                                      workload=workload, bs=bs, qd=32,
                                      file_gib=size, warmup=warmup, measure=measure))
    return cells


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def validate(output):
    rows = json.loads((output / 'summary.json').read_text())
    if len(rows) != 1 or not rows[0].get('kernel_log_available'):
        raise RuntimeError('결과 또는 커널 로그 수집 불완전')
    if rows[0].get('kernel_storage_errors') or not rows[0].get('dm_available'):
        raise RuntimeError('커널 저장장치 오류 또는 DM 계측 누락')
    raw = list(output.glob('*.fio.json'))
    if len(raw) != 1:
        raise RuntimeError('fio 결과 수 불일치')
    jobs = json.loads(raw[0].read_text()).get('jobs', [])
    if len(jobs) != 1 or jobs[0].get('error', 0):
        raise RuntimeError('fio 실행 오류')
    steal = rows[0].get('os', {}).get('cpu_steal_pct')
    return dict(cpu_steal_pct=steal, host_interference_warning=steal is None or steal > 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--short-repeats', type=int, default=5)
    parser.add_argument('--gc-repeats', type=int, default=1)
    parser.add_argument('--module-path', type=Path,
                        default=Path('/home/zns/dm-zns-base/src/dm-zns-base.ko'))
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if min(args.short_repeats, args.gc_repeats) < 1:
        parser.error('반복 수는 양수여야 합니다')
    cells = plan(args.short_repeats, args.gc_repeats)
    output = args.output_dir.resolve()
    module = args.module_path.resolve()
    manifest = dict(short_repeats=args.short_repeats, gc_repeats=args.gc_repeats,
                    total=len(cells), cells=cells, module_path=str(module),
                    independent_initialization=True, discard=False,
                    origin='ZNS physical 32GiB / DM logical 24GiB; GC 2/4/5')
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    if not args.run:
        return 0
    if not module.is_file() or output.exists():
        parser.error('모듈이 없거나 결과 폴더가 이미 존재합니다')
    if output == Path('/') or not output.is_relative_to(ROOT / 'results/zns'):
        parser.error('새 결과는 이 runner의 results/zns 아래에만 기록합니다')
    if Path('/sys/block/nvme0n1/queue/zoned').read_text().strip() != 'host-managed':
        parser.error('실험 장치가 host-managed ZNS가 아닙니다')
    if int(Path('/sys/block/nvme0n1/size').read_text()) * 512 != 32 * 1024**3:
        parser.error('실험 장치가 32GiB가 아닙니다')
    lock = open('/tmp/kovalt-fio-zns-suite.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for command in (['pgrep', '-x', 'fio'], ['pgrep', '-f', 'kafka[.]Kafka'],
                    ['pgrep', '-f', 'com[.]hanyang[.]cs[.]KafkaBenchmark']):
        if subprocess.run(command, stdout=subprocess.DEVNULL).returncode == 0:
            parser.error('다른 fio 또는 Kafka 실험이 실행 중입니다')
    output.mkdir(parents=True)
    manifest['module_sha256'] = sha(module)
    manifest['suite_script_sha256'] = sha(Path(__file__))
    for repo in ('dm-zns-base', 'Kafka-benchmark', 'kafka_python_result'):
        directory = Path('/home/zns') / repo
        metadata = {}
        for key, command in [('commit', ['rev-parse', 'HEAD']),
                             ('status', ['status', '--short', '--untracked-files=no', '--', '.', ':!results']),
                             ('diff', ['diff', '--binary', 'HEAD', '--', '.', ':!results'])]:
            result = subprocess.run(['git', '-C', str(directory), *command],
                                    capture_output=True, text=True, check=True)
            metadata[key] = result.stdout
        manifest[repo] = metadata
    manifest['dm_sources_sha256'] = {
        str(p): sha(p) for p in (Path('/home/zns/dm-zns-base/src') / name
                                 for name in ('dm-zns-base-main.c', 'skiplist.c', 'skiplist.h'))}
    jar = Path('/home/zns/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar')
    manifest['jar_sha256'] = sha(jar) if jar.is_file() else None
    save(output / 'suite-manifest.json', manifest)
    environment = dict(os.environ, BENCH_F2FS_DISCARD='0', BENCH_EXT4_DISCARD='0',
                       DM_GC_RESERVED_ZONES='2', DM_GC_LOW_WATERMARK='4',
                       DM_GC_HIGH_WATERMARK='5', DM_GC_DIAG_BUDGET='0')
    completed = []
    try:
        for index, cell in enumerate(cells, 1):
            if sha(module) != manifest['module_sha256']:
                raise RuntimeError('실행 중 모듈 파일 변경 감지')
            if os.statvfs(output).f_bavail * os.statvfs(output).f_frsize < 2 * 1024**3:
                raise RuntimeError('결과 디스크 여유 2GiB 미만')
            destination = output / cell['filesystem'] / cell['group'] / (
                f"{cell['workload']}-{cell['bs']}-qd32-r{cell['repeat']}")
            status = dict(state='running', index=index, total=len(cells),
                          completed=len(completed), cell=cell, output=str(destination),
                          updated=datetime.datetime.now().isoformat())
            save(output / 'status.json', status)
            command = ['bash', str(ROOT / 'scripts/run-fio-evaluation.sh'),
                       '--prepare', '--backend', 'dm-zns', '--module-path', str(module),
                       '--filesystem', cell['filesystem'], '--workloads', cell['workload'],
                       '--bs', cell['bs'], '--qds', '32', '--rounds', '1',
                       '--file-gib', str(cell['file_gib']), '--warmup', str(cell['warmup']),
                       '--measure', str(cell['measure']), '--output-dir', str(destination), '--run']
            print(f"[{index}/{len(cells)}] {destination}", flush=True)
            subprocess.run(command, env=environment, check=True)
            completed.append(dict(cell=cell, output=str(destination), **validate(destination)))
            save(output / 'completed-cells.json', completed)
        save(output / 'status.json', dict(state='completed', completed=len(completed), total=len(cells)))
        save(output / 'exit-code.json', dict(exit_code=0))
        return 0
    except BaseException as exc:
        save(output / 'status.json', dict(state='failed', completed=len(completed),
                                         total=len(cells), error=str(exc)))
        save(output / 'exit-code.json', dict(exit_code=1))
        raise


if __name__ == '__main__':
    raise SystemExit(main())

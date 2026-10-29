#!/usr/bin/env python3
"""게스트 전용 native F2FS 사전 검증. 기존 비교용 결과와 분리한다."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import fio_evaluation as fio


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True)
    p.add_argument('--run', action='store_true')
    p.add_argument('--file-gib', type=int, default=1)
    p.add_argument('--warmup', type=int, default=20)
    p.add_argument('--measure', type=int, default=120)
    p.add_argument('--workload', choices=['all','seq-write','rand-write','seq-write-1M'], default='all')
    a = p.parse_args()
    if a.file_gib < 1 or a.warmup < 0 or a.measure < 1:
        p.error('size/measure는 양수, warmup은 0 이상')
    if not a.run:
        print(f'native F2FS: {a.file_gib}GiB, {a.workload}, QD32, warmup{a.warmup}s+{a.measure}s')
        return
    mount = Path('/result/native-f2fs')
    out = Path(a.output).resolve()
    if os.uname().nodename != 'zns-guest' or out.exists():
        raise RuntimeError('게스트 신원 또는 신규 결과 경로 확인 실패')
    check = subprocess.check_output(['findmnt', '-n', '-o', 'SOURCE,FSTYPE,TARGET', str(mount)], text=True).split()
    if check != ['/dev/nvme0n1', 'f2fs', str(mount)]:
        raise RuntimeError('native F2FS 마운트 불일치')
    if out.is_relative_to(mount) or out.parent.stat().st_dev == mount.stat().st_dev:
        raise RuntimeError('결과는 시험 파일과 다른 파일시스템에 저장해야 함')
    for d, size, zone in [('nvme0n1', 2097152, 'none'), ('nvme1n1', 67108864, 'host-managed')]:
        b = Path('/sys/block') / d
        if int((b/'size').read_text()) != size or (b/'queue/zoned').read_text().strip() != zone:
            raise RuntimeError('native FEMU 장치 구성 불일치')
    if int(Path('/sys/block/nvme1n1/queue/chunk_sectors').read_text()) != 1048576:
        raise RuntimeError('512MiB zone이 아님')
    for cmd in (['pgrep', '-x', 'fio'], ['pgrep', '-f', 'kafka[.]Kafka']):
        if subprocess.run(cmd, stdout=subprocess.DEVNULL).returncode == 0:
            raise RuntimeError('다른 부하기 실행 중')
    import fcntl
    lock = open('/tmp/kovalt-fio-evaluation.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    out.mkdir(parents=True)
    usage = os.statvfs(mount)
    size = a.file_gib * 1024**3
    if size > usage.f_bavail * usage.f_frsize * .8:
        raise RuntimeError('시험 파일 가용 공간 부족')
    manifest = dict(backend='native-f2fs-zns', filesystem='f2fs', purpose='비교 가능성 사전 진단',
                    independent_repetitions=False, file_gib=a.file_gib, qd=32, warmup=a.warmup, measure=a.measure,
                    requested_direct=True, effective_direct='검증 필요: zoned F2FS는 buffered fallback 가능',
                    results_filesystem=str(out.parent), zone_bytes=512*1024**2,
                    available_bytes=usage.f_bavail*usage.f_frsize,
                    fio_sha256=__import__('hashlib').sha256(Path(fio.__file__).read_bytes()).hexdigest())
    (out/'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    data = mount / ('.kovalt-' + out.name)
    data.mkdir()
    file = data/'data.bin'
    with file.open('xb') as f:
        f.truncate(size)
    try:
        # 전용 tracing instance: 준비 단계에만 사용하고 본 측정 전에 해제한다.
        # 기존 전역 tracing 설정이나 다른 instance는 건드리지 않는다.
        trace='/sys/kernel/tracing/instances/kovalt-native-f2fs-probe'
        subprocess.run(['sudo','-n','mkdir',trace],check=True)
        try:
            subprocess.run(['sudo','-n','sh','-c',
                'printf "f2fs_direct_IO\\nf2fs_write_begin\\n" > '+trace+'/set_ftrace_filter; '
                'echo function > '+trace+'/current_tracer; echo 1 > '+trace+'/tracing_on'],check=True)
            fio.run_checked(fio.fio_command(file,size,'write','1M',4,0,0,out/'prepare.json',False),max(600,a.file_gib*600))
        finally:
            subprocess.run(['sudo','-n','sh','-c','echo 0 > '+trace+'/tracing_on'],check=True)
            trace_text=subprocess.check_output(['sudo','-n','cat',trace+'/trace'],text=True)
            (out/'prepare-io-path.trace').write_text(trace_text)
            subprocess.run(['sudo','-n','sh','-c','echo nop > '+trace+'/current_tracer'],check=True)
            subprocess.run(['sudo','-n','rmdir',trace],check=True)
        (out/'io-path.json').write_text(json.dumps(dict(
            requested_direct=True,
            buffered_write_begin_observed='f2fs_write_begin' in trace_text,
            direct_io_function_observed='f2fs_direct_IO' in trace_text,
            note='준비 단계 함수 추적; direct_IO 진입만으로 실제 direct 완료를 의미하지 않음'),ensure_ascii=False,indent=2))
        records=[]
        for name, rw, bs in [('seq-write','write','4k'),('rand-write','randwrite','4k'),('seq-write-1M','write','1M')]:
            if a.workload != 'all' and a.workload != name:
                continue
            for phase in ['before']:
                status=subprocess.run(['sudo','-n','cat','/sys/kernel/debug/f2fs/status'],capture_output=True,text=True,timeout=10)
                (out/f'{name}.f2fs-{phase}.txt').write_text(status.stdout+status.stderr)
            sampler=fio.Sampler(mount,['nvme0n1','nvme1n1'],None,out/f'{name}.samples.jsonl')
            sampler.thread.start()
            start=time.time_ns()//1000
            try:
                fio.run_checked(fio.fio_command(file,size,rw,bs,32,a.warmup,a.measure,out/f'{name}.fio.json'),a.warmup+a.measure+120,sampler)
            finally:
                sampler.stop.set(); sampler.thread.join(timeout=5)
            end=time.time_ns()//1000
            status=subprocess.run(['sudo','-n','cat','/sys/kernel/debug/f2fs/status'],capture_output=True,text=True,timeout=10)
            (out/f'{name}.f2fs-after.txt').write_text(status.stdout+status.stderr)
            raw=json.loads((out/f'{name}.fio.json').read_text())
            kernel=subprocess.run(['sudo','-n','journalctl','-k','--no-pager','--since',f'@{start//1000000}'],capture_output=True,text=True,timeout=30)
            (out/f'{name}.kernel.log').write_text(kernel.stdout+kernel.stderr)
            record=dict(workload=name,block_size=bs,start_us=start,
                        measurement_start_us_approx=start+a.warmup*1000000,end_us=end,
                        **fio.parse_fio(raw),os=fio.os_metrics([r for r in sampler.rows if r['epoch_us']>=start+a.warmup*1000000]))
            records.append(record)
            (out/'summary.json').write_text(json.dumps(records,indent=2))
            if kernel.returncode or __import__('re').search(r'nvme.*timeout|Buffer I/O error|F2FS-fs.*(?:ERROR|corrupt)',kernel.stdout,__import__('re').I):
                raise RuntimeError('커널 로그 수집 실패 또는 저장장치 오류')
        (out/'exit-code.json').write_text('{"exit_code":0}\n')
    except BaseException as exc:
        (out/'failure.json').write_text(json.dumps({'error':str(exc)}))
        raise


if __name__ == '__main__':
    main()

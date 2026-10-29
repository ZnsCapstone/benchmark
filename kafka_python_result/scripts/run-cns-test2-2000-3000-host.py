"""CNS 2번: 실패도 보존하며 각 FS/시나리오의 2000, 3000 조건 실행."""
import fcntl
import os
from pathlib import Path
import shlex
import signal
import time

HERE = Path(__file__).resolve().parent
import importlib.util
spec = importlib.util.spec_from_file_location('legacy', HERE / 'controller-legacy.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
RUN = 'cns-test2-2000-3000-20261028'
c.RUN = RUN
c.ROOT = Path('/home/zns-team4/results/final') / RUN
c.BUILD = '/home/zns/' + RUN
c.m.CONTROL = c.ROOT


def finish_vm():
    """정상 종료 우선. 멈춘 경우에도 identity guard를 통과한 실험 VM만 종료."""
    try:
        c.m.shutdown()
        return
    except Exception as error:
        c.m.save(c.ROOT / 'last-shutdown-error.json', dict(error=str(error)))
    for pid in c.qemu():
        os.kill(int(pid), signal.SIGTERM)
    for _ in range(30):
        if not c.qemu():
            return
        time.sleep(2)
    raise RuntimeError('실험 VM 종료 실패: 호스트 및 다른 VM은 건드리지 않음')


def main():
    if os.uname().nodename != 'splab' or os.environ.get('USER') != 'zns-team4':
        raise RuntimeError('team4 host 전용')
    c.ROOT.mkdir(parents=True, exist_ok=True)
    lock = (c.ROOT / 'controller.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (c.ROOT / 'plan.json').exists():
        raise RuntimeError('기존 실행 덮어쓰기 금지')
    if c.qemu():
        c.m.active_guard()  # 시작 전에 다른 사용자의 진행 중 부하가 있으면 중단
    cells = [dict(index=i + 1, filesystem=fs, scenario=scenario, rate=rate, backend='cns')
             for i, (fs, scenario, rate) in enumerate(
                 (fs, scenario, rate) for fs in ['ext4', 'f2fs']
                 for scenario in ['write', 'read-write'] for rate in [2000, 3000])]
    c.m.save(c.ROOT / 'plan.json', dict(cells=cells, build=c.META,
             prefill_percent=80, measure=1800, warmup=0,
             post_retention_ack_bytes=10 * 2**30, minimum_achieved_pct=90,
             continue_after_failure=True, condition_deadline_hours=8,
             note='실패는 비교 성능 통과로 간주하지 않음. 기존 FEMU/JAR 유지'))
    done = []
    for cell in cells:
        host = c.ROOT / 'device' / f"cell-{cell['index']:02}"
        out = '/home/zns/kafka_python_result/results/final/' + RUN + f"/cell-{cell['index']:02}"
        row = dict(cell=cell, status='failed')
        # 이전 조건의 오류/정체와 다음 조건을 분리한다.
        finish_vm()
        c.m.status('booting', completed=len(done), total=len(cells), cell=cell)
        c.boot(cell, host)
        c.deploy()
        row['boot_id'] = c.m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()
        try:
            c.launch(cell, out)
            deadline = time.monotonic() + 8 * 3600
            while True:
                if not c.qemu():
                    raise RuntimeError('FEMU 종료')
                code = 'from pathlib import Path; p=Path(' + repr(out + '.exit.json') + '); print(p.read_text() if p.exists() else "{}")'
                mark = c.json.loads(c.m.guest('python3 -c ' + shlex.quote(code), timeout=20, retry=True).stdout)
                c.m.status('running', completed=len(done), total=len(cells), cell=cell)
                if mark:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('8시간 조건 한계')
                time.sleep(60)
            row.update(c.collect(cell, out, host, row['boot_id']))
            row['status'] = 'completed'
        except Exception as error:
            row['error'] = str(error)
            # collect가 거절한 결과도 collected/ 및 classification.json에 남는다.
            dest = c.ROOT / 'collected' / cell['filesystem'] / cell['scenario'] / str(cell['rate'])
            dest.mkdir(parents=True, exist_ok=True)
            for remote, local in [(out, dest / 'partial-result'), (out + '.launch.log', dest / 'partial-launch.log')]:
                if (dest / 'result').exists():
                    break
                try:
                    c.m.copy_from(remote, local)
                except Exception as copy_error:
                    row.setdefault('collection_errors', []).append(str(copy_error))
            c.m.save(host / 'failure.json', row)
        done.append(row)
        c.m.save(c.ROOT / 'cell-records.json', done)
        c.m.status('cell-finished', completed=len(done), total=len(cells), result=row)
    finish_vm()
    c.m.status('completed', completed=len(done), total=len(cells),
               failed=sum(r['status'] == 'failed' for r in done))


if __name__ == '__main__':
    main()

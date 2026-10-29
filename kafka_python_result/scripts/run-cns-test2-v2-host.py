"""CNS 2번: 실패도 보존하며 각 FS/시나리오의 2000, 3000 조건 실행."""
import fcntl
import os
from pathlib import Path
import shlex
import signal
import sys
import time

HERE = Path(__file__).resolve().parent
import importlib.util
spec = importlib.util.spec_from_file_location('legacy', HERE / 'controller-legacy.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
RUN = 'cns-test2-v2-rw-20261028-r2'
c.RUN = RUN
c.ROOT = Path('/home/zns-team4/results/final') / RUN
c.BUILD = '/home/zns/' + RUN
c.m.CONTROL = c.ROOT


def launch(cell,out):
    mode=cell['scenario']
    cmd=['env','BENCH_STORAGE_BACKEND=cns','BENCH_LONG_GC=1','BENCH_DM_METRICS=0',
         'BENCH_EXT4_DISCARD=0','BENCH_F2FS_DISCARD=0',
         'BENCH_JAVA_JAR='+c.BUILD+'/kafka-benchmark-1.0.jar',
         'BENCH_READ_MODE='+mode,'BENCH_BACKLOG_CLASSES='+c.BUILD+'/classes',
         'JAVA_TOOL_OPTIONS=-Dbenchmark.explicitPartitions=8','python3','-u',
         c.BUILD+'/src/cns_test2_entry.py','--single-rate','--backend','cns',
         '--filesystem',cell['filesystem'],'--scenario','read-write' if mode=='read-write' else 'write',
         '--workload','prefill','--rate',str(cell['rate']),'--rounds','1','--warmup','0',
         '--measure','900','--occupancy','80','--stall-limit','60','--run-dir',out]
    c.m.guest('mkdir -p '+shlex.quote(str(Path(out).parent)))
    shell=shlex.join(cmd)+' > '+shlex.quote(out+'.launch.log')+' 2>&1; rc=$?; printf \'{"exit_code":%s}\\n\' "$rc" > '+shlex.quote(out+'.exit.json')
    c.m.guest(shlex.join(['tmux','new-session','-d','-s',RUN+f"-cell-{cell['index']:02}",'bash','-lc',shell]))


def collect(cell,out,host,boot_id):
    dest=c.ROOT/'collected'/cell['filesystem']/cell['scenario']/str(cell['rate'])/f"r{cell['repeat']}"
    dest.mkdir(parents=True,exist_ok=True)
    c.m.copy_from(out,dest/'result')
    c.m.copy_from(out+'.launch.log',dest/'launch.log')
    c.m.copy_from(out+'.exit.json',dest/'wrapper-exit.json')
    c.shutil.copy2(host/'cns.csv',dest/'femu-cns.csv')
    folder=dest/'result'
    result=c.pe.classify_run(folder,90,60)
    c.m.save(dest/'classification.json',result)
    if c.json.loads((dest/'wrapper-exit.json').read_text())['exit_code'] or result['runner_exit_code'] or result.get('kernel_storage_errors'):
        raise RuntimeError('runner/장치 오류: 원본 보존')
    if [s for s in result['reasons'] if 'achieved_pct=' not in s]:
        raise RuntimeError('요청/정합성/안전 오류: '+str(result['reasons']))
    ready=c.json.loads((folder/'long-readiness.json').read_text())
    if ready.get('protocol')!='test2-v2' or not ready['retention_all_partitions'] or ready['post_retention_seconds']<600 or ready['measurement_seconds']!=900:
        raise RuntimeError('v2 준비/측정 조건 불일치')
    if c.json.loads((folder/'long-sampler-errors.json').read_text()):raise RuntimeError('계측 오류')
    if not (folder/'long-measurement-end.json').exists():raise RuntimeError('종료 경계 누락')
    if cell['scenario']=='backlog' and not (folder/'backlog-complete.json').exists():raise RuntimeError('backlog 읽기 미완료')
    return dict(cell=cell,boot_id=boot_id,result=result,collected=str(dest))


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
    resume='--resume' in sys.argv
    if (c.ROOT / 'plan.json').exists() and not resume:
        raise RuntimeError('기존 실행 덮어쓰기 금지')
    if c.qemu() and not resume:
        c.m.active_guard()  # 시작 전에 다른 사용자의 진행 중 부하가 있으면 중단
    cells = [dict(index=i + 1, filesystem=fs, scenario=scenario, rate=rate, repeat=repeat, backend='cns')
             for i, (repeat, fs, scenario, rate) in enumerate(
                 (repeat,fs,scenario,rate) for repeat in [1] for fs in ['ext4','f2fs']
                 for scenario in ['write','read-write','backlog'] for rate in [2000,3000])]
    if '--stop-after-cell' in sys.argv:
        stop_after=int(sys.argv[sys.argv.index('--stop-after-cell')+1])
        if not 1<=stop_after<=len(cells):raise RuntimeError('stop-after-cell 범위 오류')
        excluded=cells[stop_after:]
        cells=cells[:stop_after]
        c.m.save(c.ROOT/'excluded-by-user.json',dict(reason='사용자 요청: CNS F2FS 첫 조건 후 중단',cells=excluded))
    if resume:
        old=c.json.loads((c.ROOT/'plan.json').read_text())
        if old['build']!=c.META or old['cells'][:len(cells)]!=cells:
            raise RuntimeError('재개 입력/조건 불일치')
        c.m.save(c.ROOT/f'plan-before-single-repeat-{time.time_ns()}.json',old)
    c.m.save(c.ROOT / 'plan.json', dict(cells=cells, build=c.META,
             prefill_percent=80, measure=900, warmup=0,
             post_retention_seconds=600,prepare_timeout_seconds=2700,minimum_achieved_pct=90,
             continue_after_failure=True, condition_deadline_hours=3,
             note='실패는 비교 성능 통과로 간주하지 않음. 기존 FEMU/JAR 유지'))
    done=c.json.loads((c.ROOT/'cell-records.json').read_text()) if resume and (c.ROOT/'cell-records.json').exists() else []
    if [r['cell'] for r in done]!=cells[:len(done)]:raise RuntimeError('완료 prefix 불일치')
    for cell in cells:
        if cell['index']<=len(done):continue
        host = c.ROOT / 'device' / f"cell-{cell['index']:02}"
        out = '/home/zns/kafka_python_result/results/final/' + RUN + f"/cell-{cell['index']:02}"
        row = dict(cell=cell, status='failed')
        # 이전 조건의 오류/정체와 다음 조건을 분리한다.
        attached=False
        if resume and host.exists():
            c.qemu()  # 동일 team4 VM 외에는 재개하지 않는다.
            attached=c.m.guest('test -f '+shlex.quote(out+'.launch.log'),timeout=20,check=False).returncode==0
            if not attached:raise RuntimeError('기존 셀 준비 흔적: 자동 초기화 금지')
        if not attached:
            finish_vm()
            c.m.status('booting', completed=len(done), total=len(cells), cell=cell)
        try:
            if not attached:
                c.boot(cell, host)
                c.deploy()
                c.m.guest('mkdir -p '+c.BUILD+'/classes && cp '+c.BUILD+'/src/BacklogReadProbe.class '+c.BUILD+'/classes/',timeout=30)
            artifact=c.m.guest('sha256sum '+c.BUILD+'/classes/BacklogReadProbe.class',timeout=30)
            if artifact.stdout.split()[0]!=c.META['input_hashes']['src/BacklogReadProbe.class']:raise RuntimeError('consumer class SHA 불일치')
            row['backlog_class_sha256']=artifact.stdout.split()[0]
            row['boot_id'] = c.m.guest('cat /proc/sys/kernel/random/boot_id').stdout.strip()
            if not attached:launch(cell, out)
            deadline = time.monotonic() + max(0,3*3600-(time.time()-host.stat().st_mtime)) if attached else time.monotonic()+3*3600
            while True:
                if not c.qemu():
                    raise RuntimeError('FEMU 종료')
                code = 'from pathlib import Path; p=Path(' + repr(out + '.exit.json') + '); print(p.read_text() if p.exists() else "{}")'
                mark = c.json.loads(c.m.guest('python3 -c ' + shlex.quote(code), timeout=20, retry=True).stdout)
                c.m.status('running', completed=len(done), total=len(cells), cell=cell)
                if mark:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('3시간 조건 한계')
                time.sleep(60)
            row.update(collect(cell, out, host, row['boot_id']))
            row['status'] = 'completed'
        except Exception as error:
            row['error'] = str(error)
            # collect가 거절한 결과도 collected/ 및 classification.json에 남는다.
            dest = c.ROOT / 'collected' / cell['filesystem'] / cell['scenario'] / str(cell['rate']) / f"r{cell['repeat']}"
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

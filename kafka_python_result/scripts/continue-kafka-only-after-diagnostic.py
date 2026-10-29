"""team4 host에서 진단 성공을 확인한 경우에만 본 실험을 시작한다."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time

HERE = Path(__file__).resolve().parent
DIAG = Path('/home/zns-team4/results/final/kafka-only-compare-5c0cdbd-20261023-r3-diagnostic')
RUN = 'kafka-only-compare-5c0cdbd-20261023-r3'

def main():
    lock = (HERE/'continue.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    deadline = time.monotonic() + 3600
    while True:
        state = json.loads((DIAG/'status.json').read_text())
        print('진단 상태:', state['state'], flush=True)
        if state['state'] == 'failed':
            raise RuntimeError('진단 실패: 본 실험 실행 안 함')
        if state['state'] == 'completed':
            break
        if time.monotonic() > deadline:
            raise RuntimeError('진단 대기 한계 초과: 진행 중 부하 보존, 본 실험 실행 안 함')
        time.sleep(60)
    folder = DIAG/'collected/dm-zns/ext4/10000'
    assert json.loads((folder/'diagnostic-completed.json').read_text())['rounds'] == 5
    assert json.loads((folder/'metadata-placement.json').read_text())['separate_filesystem']
    summary = []
    for index in range(1, 6):
        result = json.loads((folder/f'prepare-{index:03}.json').read_text())
        metrics = result['metrics']
        assert result['validity']['valid'], result['validity']
        assert metrics['drain_completed']
        for key in ['failed_requests','send_errors','unresolved_after_drain']:
            assert metrics[key] == 0, (index, key, metrics[key])
        summary.append(dict(round=index, achieved_ops=metrics['achieved_ops'],
                            p99_ms=metrics['p99_ms'], failed_requests=metrics['failed_requests']))
    target = Path('/home/zns-team4/results/final')/RUN
    if target.exists():
        raise RuntimeError('본 실험 기존 경로 보존: 중복 실행 금지')
    (HERE/'diagnostic-gate.json').write_text(json.dumps(dict(
        passed=True, diagnostic=str(DIAG), rounds=summary,
        limitation='쓰기 복구 진단 통과는 consumer 정합성 및 근본 원인 해결을 뜻하지 않음'),
        ensure_ascii=False,indent=2)+'\n')
    env = dict(os.environ, KAFKA_ONLY_RUN=RUN)
    env.pop('KAFKA_ONLY_DIAGNOSTIC', None)
    print('진단 게이트 통과: 본 8조건 시작', flush=True)
    subprocess.run(['python3','-u',str(HERE/'full-controller.py')],env=env,check=True)

if __name__ == '__main__':
    main()

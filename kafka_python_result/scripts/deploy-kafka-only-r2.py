"""기존 실패 패키지를 보존하고 진단 보완 입력만 별도 배포한다."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import shutil
import subprocess

BASE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('connection', BASE/'scripts/run-fio-cns-controller.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
OLD = '/home/zns-team4/benchmark-control/kafka-only-compare-5c0cdbd-20261023'
NEW = OLD+'-r4'
local = BASE/'results/final/kafka-only-compare-input-20261023-r4'

def host(cmd):
    r = c.call('host', cmd, timeout=40)
    return r.stdout

def upload(src, dest):
    opts = c.RELAY[:-1].copy()
    opts[opts.index('-p')] = '-P'
    subprocess.run(['scp', '-r', *opts, str(src), c.RELAY[-1]+':'+dest], check=True, timeout=60)

if __name__ == '__main__':
    # 이미 존재하는 새 실험도 덮어쓰지 않는다.
    host('test ! -e '+shlex.quote(NEW)+' && cp -a '+shlex.quote(OLD)+' '+shlex.quote(NEW))
    manifest = json.loads(host('cat '+OLD+'/build-manifest.json'))
    jar = Path('/home/loloy/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar')
    shutil.copy2(jar, local/jar.name)
    hashes = {str(p.relative_to(local)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted((local/'src').glob('*.py'))}
    hashes[jar.name] = hashlib.sha256(jar.read_bytes()).hexdigest()
    manifest.update(input_hashes=hashes, jar_sha256=hashes[jar.name],
                    diagnostic_revision='r4: Kafka 전용 data 하위 폴더 및 retention 탐색 경로 일치; r3 성공 DM조건 별도 보존; 나머지 7조건')
    manifest['java_commit'] = subprocess.check_output(['git','rev-parse','HEAD'], cwd=jar.parents[2], text=True).strip()
    (local/'java-working-tree.patch').write_bytes(subprocess.check_output(['git','diff'],cwd=jar.parents[2]))
    shutil.copy2(jar.parents[2]/'src/main/java/com/hanyang/cs/ProducerFailureDiagnostics.java',local/'ProducerFailureDiagnostics.java')
    (local/'source-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
    (local/'build-manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    upload(local/'src',NEW+'/dm-input/')
    for name in [jar.name,'source-hashes.json']:
        upload(local/name,NEW+'/dm-input/'+name)
    upload(local/'build-manifest.json',NEW+'/build-manifest.json')
    upload(BASE/'scripts/run-kafka-only-compare-20261023.py',NEW+'/controller.py')
    host('python3 -m py_compile '+NEW+'/controller.py '+NEW+'/dm-input/src/kafka_only_evaluation.py')
    print(host('tmux new-session -d -s kafka-only-compare-r4 '+shlex.quote('KAFKA_ONLY_RUN=kafka-only-compare-5c0cdbd-20261023-r4 KAFKA_ONLY_START_CELL=2 python3 -u '+NEW+'/controller.py > '+NEW+'/controller.log 2>&1')))
    print('실행 요청 완료:',NEW)

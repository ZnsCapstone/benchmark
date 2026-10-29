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
NEW = '/home/zns-team4/benchmark-control/kafka-only-10k-5c0cdbd-20261023'
local = BASE/'results/final/kafka-only-compare-input-20261023-10k'

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
                    diagnostic_revision='1만 우선: CNS F2FS, DM F2FS, Native F2FS; 2만 자동 실행 안 함')
    manifest['launchers']['run-native-headless.sh']=hashlib.sha256((BASE/'scripts/run-native-kafka-headless.sh').read_bytes()).hexdigest()
    manifest['java_commit'] = subprocess.check_output(['git','rev-parse','HEAD'], cwd=jar.parents[2], text=True).strip()
    (local/'java-working-tree.patch').write_bytes(subprocess.check_output(['git','diff'],cwd=jar.parents[2]))
    shutil.copy2(jar.parents[2]/'src/main/java/com/hanyang/cs/ProducerFailureDiagnostics.java',local/'ProducerFailureDiagnostics.java')
    (local/'source-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
    (local/'build-manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    upload(local/'src',NEW+'/dm-input/')
    for name in [jar.name,'source-hashes.json']:
        upload(local/name,NEW+'/dm-input/'+name)
    upload(local/'build-manifest.json',NEW+'/build-manifest.json')
    upload(BASE/'scripts/run-native-kafka-headless.sh',NEW+'/run-native-headless.sh')
    upload(BASE/'scripts/prepare-native-f2fs-user24g.sh',NEW+'/prepare-native-f2fs-user24g.sh')
    upload(BASE/'scripts/run-kafka-only-10k.py',NEW+'/controller.py')
    host('python3 -m py_compile '+NEW+'/controller.py '+NEW+'/dm-input/src/kafka_only_evaluation.py')
    print(host('tmux new-session -d -s kafka-only-10k-20261023 '+shlex.quote('python3 -u '+NEW+'/controller.py > '+NEW+'/controller.log 2>&1')))
    print('실행 요청 완료:',NEW)

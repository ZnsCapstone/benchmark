"""보존 Kafka-only runner에 고정 backlog 읽기만 추가하는 별도 실행 진입점."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import performance_evaluation as pe

configure=pe.configure_result_paths


def install(cfg,root):
    configure(cfg,root)
    import performance
    original=performance.run_benchmark_once
    def measure(fs,scenario,config,round_idx,phase_tag='measure'):
        if phase_tag!='measure':return original(fs,scenario,config,round_idx,phase_tag)
        root=Path(root_dir)
        subprocess.run(['sudo','-n','sync'],check=True,timeout=90)
        subprocess.run(['sudo','-n','sh','-c','echo 1 > /proc/sys/vm/drop_caches'],check=True,timeout=15)
        (root/'cold-start.json').write_text(json.dumps(dict(epoch_us=time.time_ns()//1000,drop_caches=1)))
        logfile=root/'current-java.log'
        if logfile.exists():logfile.rename(root/'prepare-last-java.log')
        consumer=subprocess.Popen(['java','-cp',os.environ['BENCH_BACKLOG_CLASSES']+os.pathsep+cfg.JAVA_BENCH_JAR,
            'BacklogReadProbe',cfg.BOOTSTRAP,cfg.TOPIC_NAME,str(root/'backlog-read.jsonl'),
            str(config['measure_sec']+60)],stdout=(root/'backlog-reader.log').open('w'),stderr=subprocess.STDOUT)
        stop=threading.Event()
        try:
            for _ in range(300):
                if (root/'backlog-ready.json').exists():break
                if consumer.poll() is not None:raise RuntimeError('과거 로그 consumer 준비 실패')
                time.sleep(.1)
            else:raise RuntimeError('과거 로그 consumer 준비 시간 초과')
            def gate():
                # 과거 준비 로그를 제거한 뒤 실제 producer 시작 메시지를 기다린다.
                while not stop.wait(.1):
                    if logfile.exists() and '[Run] Producers started.' in logfile.read_text(errors='replace'):
                        (root/'backlog-go').touch();return
            thread=threading.Thread(target=gate,daemon=True);thread.start()
            result=original(fs,scenario,config,round_idx,phase_tag)
            (root/'producer-result-before-backlog-check.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
            if not (root/'backlog-complete.json').exists():raise RuntimeError('고정 과거 로그 범위 읽기 미완료 또는 retention 손실')
            return result
        finally:
            stop.set()
            if consumer.poll() is None:
                consumer.terminate()
                try:consumer.wait(timeout=15)
                except subprocess.TimeoutExpired:consumer.kill();consumer.wait()
    root_dir=str(root)
    performance.run_benchmark_once=measure


pe.configure_result_paths=install
if __name__=='__main__':
    import kafka_only_evaluation
    raise SystemExit(kafka_only_evaluation.main())

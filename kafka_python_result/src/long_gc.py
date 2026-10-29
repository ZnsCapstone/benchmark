"""2번 장기 retention 준비·구간 계측. 기본 benchmark에는 적용하지 않는다."""
import json
import math
import os
import signal
import sys
from pathlib import Path
import subprocess
import threading
import time

POST_RETENTION_BYTES=10*1024**3

def retention_offsets(mount,topic):
    return [min((int(p.stem) for p in (Path(mount)/f'{topic}-{i}').glob('*.log')
                 if p.stem.isdigit()),default=0) for i in range(8)]

def ready(offsets,post_bytes):
    return len(offsets)==8 and all(x>0 for x in offsets) and post_bytes>=POST_RETENTION_BYTES

def prepare_seconds(remaining,rate):
    return min(120,max(1,math.ceil(remaining/(rate*1024))))

def install(cfg):
    import bench_runner as runner
    import performance
    import system_setup as setup
    original=runner.run_benchmark_once
    active=[None]
    def streaming(command):
        output=[]
        process=subprocess.Popen(command,shell=True,start_new_session=True,
            stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        active[0]=process
        try:
            for line in process.stdout:
                print('  > '+line.rstrip(),flush=True);output.append(line)
            return ''.join(output),process.wait()
        finally:
            active[0]=None
            if process.poll() is None:
                os.killpg(process.pid,signal.SIGTERM)
    performance.run_cmd_streaming=streaming
    cfg.JAVA_BENCH_CMD=cfg.JAVA_BENCH_CMD.replace('java ','java -Dbenchmark.consumerStartLatest=true ',1)
    os.environ['BENCH_INTEGRITY_DIAG']='1'
    out=Path(cfg.RESULT_DIR);out.mkdir(parents=True,exist_ok=True)
    native=os.environ.get('BENCH_NATIVE_LONG')=='1'
    if native:
        subprocess.run(['sudo','-n','sh','-c',
            'mountpoint -q /sys/kernel/debug || mount -t debugfs debugfs /sys/kernel/debug'],check=True,timeout=15)

    def measure(fs,scenario,config,round_idx,phase_tag='measure'):
        stop=threading.Event();lock=threading.Lock();errors=[];phase='retention-prepare'
        def sample():
            with lock:
                row=dict(epoch_us=time.time_ns()//1000,phase=phase,
                         usage=setup.filesystem_usage(),earliest_offsets=retention_offsets(cfg.MOUNT_POINT,cfg.TOPIC_NAME))
                if row['usage']['used_percent']>=95:raise RuntimeError('사용자 공간 점유율 95% 안전 한계')
                st=os.statvfs(out)
                if st.f_bavail*st.f_frsize<2*1024**3:raise RuntimeError('결과 디스크 여유 2GiB 미만')
                row['cpu_ticks']=Path('/proc/stat').read_text().splitlines()[0]
                row['block_stats']={name:Path('/sys/block',name,'stat').read_text().strip()
                    for name in (['nvme0n1','nvme1n1'] if native else ['nvme0n1'])}
                if native:
                    r=subprocess.run(['sudo','-n','cat','/sys/kernel/debug/f2fs/status'],capture_output=True,text=True,timeout=3)
                    if r.returncode:raise RuntimeError('Native F2FS GC 계측 불가: '+r.stderr)
                    row['f2fs_status']=r.stdout
                else:
                    r=subprocess.run(['sudo','-n','dmsetup','status',cfg.DM_NAME],capture_output=True,text=True,timeout=3)
                    if r.returncode:raise RuntimeError('DM GC 계측 불가: '+r.stderr)
                    row['dm_status']=r.stdout
                with (out/'long-gc-samples.jsonl').open('a') as f:f.write(json.dumps(row,ensure_ascii=False)+'\n')
                return row
        def background():
            while not stop.wait(10):
                try:sample()
                except Exception as exc:
                    errors.append(str(exc));stop.set()
                    process=active[0]
                    if process is not None and process.poll() is None:
                        try:os.killpg(process.pid,signal.SIGTERM)
                        except ProcessLookupError:pass
        def save(name,value):
            (out/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
        thread=threading.Thread(target=background,daemon=True)
        ack_total=0;post_bytes=0;seen=False;deadline=time.monotonic()+6*3600;index=0
        try:
            sample();thread.start()
            while True:
                if errors:raise RuntimeError(errors[0])
                if time.monotonic()>deadline:raise RuntimeError('retention 준비 6시간 제한 초과')
                index+=1
                seconds=prepare_seconds(POST_RETENTION_BYTES-post_bytes,config['target_ops']) if seen else 120
                prep=dict(config,measure_sec=seconds,warmup_sec=0,phase='retention-prepare')
                result=original(fs,scenario,prep,index,phase_tag=f'retention-prepare-{index:03}')
                save(f'retention-prepare-{index:03}.json',result)
                if not result['validity']['valid']:raise RuntimeError('retention 준비 부하기 오류')
                written=int(result['metrics'].get('eventual_ack_requests',0))*config['record_size']
                ack_total+=written
                if seen:post_bytes+=written
                row=sample();offsets=row['earliest_offsets']
                if not seen and all(x>0 for x in offsets):
                    seen=True
                    save('retention-first-all-partitions.json',dict(sample=row,ack_payload_bytes=ack_total))
                save('long-phase.json',dict(phase=phase,chunk=index,retention_all_partitions=seen,
                    ack_payload_bytes=ack_total,post_retention_ack_bytes=post_bytes,
                    required_post_retention_ack_bytes=POST_RETENTION_BYTES))
                if ready(offsets,post_bytes):break
            phase='measurement'
            start=sample();save('long-measurement-start.json',start)
            save('long-readiness.json',dict(retention_all_partitions=True,prepare_ack_payload_bytes=ack_total,
                post_retention_ack_bytes=post_bytes,measurement_seconds=config['measure_sec'],warmup_seconds=config['warmup_sec']))
            result=original(fs,scenario,config,round_idx,phase_tag=phase_tag)
            end=sample();save('long-measurement-end.json',end)
            if errors:raise RuntimeError(errors[0])
            return result
        finally:
            stop.set()
            if thread.is_alive():thread.join(timeout=10)
            save('long-sampler-errors.json',errors)
    runner.run_benchmark_once=measure

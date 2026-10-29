"""기존 완료 자료를 제외한 DM/Native 추가 평가 계획. 각 조건 독립1회."""
import json


def cells():
    rows=[]
    layouts=[('dm-zns','ext4'),('dm-zns','f2fs'),('native','f2fs')]
    def add(kind,backend,fs,**kwargs):
        rows.append(dict(index=len(rows)+1,kind=kind,backend=backend,filesystem=fs,repeat=1,**kwargs))
    for backend,fs in layouts:
        for workload,bs in [('seq-read','4k'),('rand-read','4k'),('seq-read','1M')]:
            add('fio-read',backend,fs,workload=workload,bs=bs,measure=120,warmup=20,file_gib=1,module='final')
    add('prefill','dm-zns','f2fs',scenario='write',rate=20000,measure=180,warmup=60,module='legacy-test2')
    for fs in ['ext4','f2fs']:
        add('prefill','dm-zns',fs,scenario='read-write',rate=10000,measure=180,warmup=60,module='legacy-test2')
    for backend,fs in [('dm-zns','f2fs'),('native','f2fs')]:
        add('kafka-only',backend,fs,scenario='write',rate=20000,measure=1800,warmup=0,module='final')
    for backend,fs in layouts:
        for scenario in ['read-write','backlog']:
            for rate in [10000,20000]:
                add('kafka-only',backend,fs,scenario=scenario,rate=rate,measure=1800,warmup=0,module='final')
    # 이전54GiB 순차4KiB DM 두 회차는 보존하고 빈 패턴만 추가한다.
    for backend,fs in layouts:
        patterns=[('seq-write','4k'),('rand-write','4k'),('seq-write','1M')] if backend=='native' else [('rand-write','4k'),('seq-write','1M')]
        for workload,bs in patterns:
            add('fio-gc',backend,fs,workload=workload,bs=bs,file_gib=18,total_write_gib=54,measure=None,warmup=0,module='final')
    return rows


if __name__=='__main__':
    print(json.dumps(cells(),ensure_ascii=False,indent=2))

"""완료 outbox를 노션에 덧붙인다. 인증이 없으면 대기하며 기존 본문을 덮지 않는다."""
import argparse
import json
import os
from pathlib import Path
import stat
import time
import urllib.request

PAGE='4062efa4278d80b69ed0e594231a9f4e'


def credentials():
    token=os.environ.get('NOTION_TOKEN') or os.environ.get('NOTION_API_KEY')
    config=Path.home()/'.config/zns-benchmark/notion.json'
    if not token and config.exists():
        if stat.S_IMODE(config.stat().st_mode)&0o077:raise RuntimeError('notion.json 권한은600이어야 함')
        token=json.loads(config.read_text()).get('token')
    return token


def api(token,method,path,payload=None):
    data=json.dumps(payload,ensure_ascii=False).encode() if payload is not None else None
    req=urllib.request.Request('https://api.notion.com/v1/'+path,data=data,method=method,
        headers={'Authorization':'Bearer '+token,'Notion-Version':'2025-09-03','Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=30) as response:return json.load(response)


def title(row):
    c=row['cell'];backend='Native F2FS' if c['backend']=='native' else 'DM+'+c['filesystem'].upper()
    kind={'fio-read':'fio 읽기','fio-gc':'동일 쓰기량 GC','prefill':'80% prefill+Kafka','kafka-only':'Kafka-only'}[c['kind']]
    pattern={'seq-read':'순차 읽기','rand-read':'랜덤 읽기','seq-write':'순차 쓰기','rand-write':'랜덤 쓰기'}
    scenario={'write':'쓰기 전용','read-write':'실시간 소비 병행','backlog':'과거 로그 소비 병행'}
    detail=(f"{pattern[c['workload']]} {c['bs']}" if 'workload' in c else f"{scenario[c['scenario']]} {c['rate']:,} ops/s")
    return f"추가 평가 {c['index']:02}: {backend} / {kind} / "+detail


def paragraph(text):
    return {'object':'block','type':'paragraph','paragraph':{'rich_text':[{'type':'text','text':{'content':text[:1900]}}]}}


def blocks(row):
    label=title(row)
    state='실패: 정상 성능 비교에서 제외했습니다.' if row['status']=='failed' else ('목표 미달의 완료 결과입니다.' if row.get('saturated') else '측정과 수집을 완료했습니다.')
    text=state
    text+=(' 기존 단기 비교에 사용한 구현으로 측정했습니다.' if row['cell']['module']=='legacy-test2' else ' 최종 구현을 사용했습니다.') if row['cell']['backend']=='dm-zns' else ''
    s=row.get('summary')
    if s:
        direction='read' if row['cell']['kind']=='fio-read' else 'write'
        text+=f" 처리량 {s.get(direction+'_iops',0):,.2f} IOPS, 대역폭 {s.get(direction+'_mib_s',0):,.2f} MiB/s, p95 {s.get(direction+'_p95_ms')}ms, p99 {s.get(direction+'_p99_ms')}ms."
        if s.get('dm_wa_issued') is not None:text+=f" DM WA {s['dm_wa_issued']:.3f}배."
        if s.get('dm_delta'):text+=f" zone 회수 {s['dm_delta'].get('gc_reclaims')}회."
        if row.get('cache_warning'):text+=' 실제 장치 읽기량이 작아 캐시 영향 여부를 별도로 확인해야 합니다.'
    metrics=row.get('metrics')
    if isinstance(metrics,list):metrics=metrics[0] if metrics else {}
    if metrics:
        text+=f" 실제 처리량 {metrics.get('achieved_ops')} ops/s, 달성률 {metrics.get('achieved_pct')}%, ACK p95 {metrics.get('p95_ms',metrics.get('app_p95_ms'))}ms, p99 {metrics.get('p99_ms',metrics.get('app_p99_ms'))}ms."
    if row.get('backlog'):
        b=row['backlog'];text+=f" 과거 로그 {b['payload_bytes']/2**30:.2f}GiB 읽기에 {b['seconds']:.2f}초가 걸렸습니다. 고정 범위를 한 번 읽은 결과입니다."
    if row.get('dm_wa') is not None:
        text+=f" DM WA {row['dm_wa']:.3f}배, zone 회수 {row['dm_delta']['gc_reclaims']}회, 이주 {row['dm_delta']['gc_moved_bytes']/2**30:.3f}GiB."
    if row.get('f2fs_delta'):
        d=row['f2fs_delta'];text+=f" F2FS GC 전경 {d.get('gc_foreground_calls','미확보')}회·배경 {d.get('gc_background_calls','미확보')}회. DM과 같은 기준의 WA는 정의하지 않았습니다."
    return [paragraph(label),paragraph(text)]


def existing_titles(token):
    titles=set();cursor=None
    while True:
        path=f'blocks/{PAGE}/children?page_size=100'+('&start_cursor='+cursor if cursor else '')
        value=api(token,'GET',path)
        for b in value['results']:
            content=b.get(b['type'],{}).get('rich_text',[])
            titles.add(''.join(t.get('plain_text',t.get('text',{}).get('content','')) for t in content))
        if not value.get('has_more'):return titles
        cursor=value['next_cursor']


def once(root):
    root.mkdir(parents=True,exist_ok=True)
    token=credentials()
    status=root/'notion-publisher-status.json'
    if not token:
        status.write_text(json.dumps(dict(state='waiting-for-credentials',message='결과는outbox에보존됨;노션자동게시는인증설정후가능'),ensure_ascii=False))
        return
    posted=root/'notion-posted';posted.mkdir(exist_ok=True)
    pending=[p for p in sorted((root/'notion-outbox').glob('*.json')) if not (posted/p.name).exists()]
    if not pending:return
    titles=existing_titles(token)
    for f in pending:
        row=json.loads(f.read_text());label=title(row)
        if label not in titles:
            response=api(token,'PATCH',f'blocks/{PAGE}/children',{'children':blocks(row)})
            titles.add(label)
        else:response={'deduplicated':True}
        (posted/f.name).write_text(json.dumps(response,ensure_ascii=False))
    status.write_text(json.dumps(dict(state='published',count=len(pending))))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--watch',action='store_true');a=p.parse_args()
    while True:
        try:once(a.root)
        except Exception as error:
            # 인증 토큰·HTTP 요청 헤더를 로그에 출력하지 않는다.
            (a.root/'notion-publisher-status.json').write_text(json.dumps(dict(state='retry-pending',error=type(error).__name__)))
        if not a.watch:break
        time.sleep(30)

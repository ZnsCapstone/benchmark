"""보존 원본으로 GC/WA 표를 재계산한다. 장치·실험을 변경하지 않는다."""
import argparse
import json
from pathlib import Path
import re
import statistics


def kafka(folder):
    a=json.loads((folder/'retention-start.json').read_text())['sample']
    b=json.loads((folder/'retention-end.json').read_text())['sample']
    row=dict(source=str(folder),boundary_seconds=b['time']-a['time'],
             scope='부하기 호출부터 drain/정리까지; Java1800초와 경계가 다름')
    if 'dm_status' in a:
        parse=lambda value:{k:int(v) for k,v in re.findall(r'([a-z_]+)=(\d+)',value)}
        x,y=parse(a['dm_status']),parse(b['dm_status'])
        keys=['gc_runs','gc_reclaims','gc_moved_bytes','logical_write_bytes',
              'user_write_bytes','gc_write_bytes','wal_write_bytes','sstable_write_bytes']
        delta={k:y[k]-x[k] for k in keys}
        if any(v<0 for v in delta.values()):raise ValueError('counter reset')
        if delta['logical_write_bytes']<=0:raise ValueError('logical write denominator')
        row.update(dm_delta=delta,dm_wa=sum(delta[k] for k in
            ['user_write_bytes','gc_write_bytes','wal_write_bytes','sstable_write_bytes'])/delta['logical_write_bytes'])
    else:
        keys=['gc_foreground_calls','gc_background_calls','gc_reclaimed_segments',
              'moved_blocks_foreground','moved_blocks_background']
        row['f2fs_delta']={k:int(b['f2fs_status'][k])-int(a['f2fs_status'][k]) for k in keys}
        row['dm_wa']=None
        row['wa_note']='Native는DM없음;동일계층WA미확보'
    return row


def fio(path):
    groups={}
    for r in json.loads(path.read_text()):
        c=r['cell']
        if c.get('group')!='short':continue
        s=r['summary'];key=(c['filesystem'],c['workload'],c['bs'])
        row={}
        if s.get('dm_available'):
            d=s['dm_delta'];row.update(dm_wa=s['dm_wa_issued'],gc_reclaims=d['gc_reclaims'],
                                      gc_moved_bytes=d['gc_moved_bytes'])
        elif 'f2fs_measurement_delta' in s:
            row.update(s['f2fs_measurement_delta'])
            z=[v for k,v in s.get('femu',{}).items() if 'zns' in k and v.get('available')]
            if len(z)==1:row['zone_resets']=z[0]['delta']['zone_resets']
        else:continue
        groups.setdefault(key,[]).append(row)
    result=[]
    for key,rows in groups.items():
        stats={}
        for name in rows[0]:
            v=[r[name] for r in rows]
            stats[name]=dict(median=statistics.median(v),minimum=min(v),maximum=max(v))
        result.append(dict(source=str(path),filesystem=key[0],pattern=key[1],bs=key[2],n=len(rows),statistics=stats))
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--fio',type=Path,action='append',default=[])
    p.add_argument('--kafka',type=Path,action='append',default=[])
    args=p.parse_args()
    print(json.dumps(dict(fio=[r for f in args.fio for r in fio(f)],
                          kafka=[kafka(f) for f in args.kafka]),ensure_ascii=False,indent=2))

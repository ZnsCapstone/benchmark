from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import long_gc

class LongGcTests(unittest.TestCase):
    def test_measurement_waits_for_retention_and_post_write_budget(self):
        calls=[]
        def original(fs,scenario,config,index,phase_tag='measure'):
            calls.append(dict(config))
            return {'validity':{'valid':True},'metrics':{'eventual_ack_requests':1}}
        runner=SimpleNamespace(run_benchmark_once=original)
        setup=SimpleNamespace(filesystem_usage=lambda:{'used_percent':80})
        perf=SimpleNamespace()
        class Thread:
            def __init__(self,**kw):pass
            def start(self):pass
            def is_alive(self):return False
        read_text=Path.read_text
        def read(path,*args,**kwargs):
            if str(path).startswith('/sys/block/'):return '0 0 0'
            return read_text(path,*args,**kwargs)
        with tempfile.TemporaryDirectory() as folder:
            cfg=SimpleNamespace(RESULT_DIR=folder,JAVA_BENCH_CMD='java benchmark',
                                MOUNT_POINT=folder,TOPIC_NAME='bench',DM_NAME='kafka-zns')
            with patch.dict('sys.modules',{'bench_runner':runner,'system_setup':setup,'performance':perf}), \
                 patch.dict('os.environ',{'BENCH_NATIVE_LONG':'0'}), \
                 patch.object(long_gc,'POST_RETENTION_BYTES',1024), \
                 patch.object(long_gc,'retention_offsets',return_value=[1]*8), \
                 patch.object(long_gc.threading,'Thread',Thread), \
                 patch.object(long_gc.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout='metrics=1',stderr='')), \
                 patch.object(Path,'read_text',read):
                long_gc.install(cfg)
                runner.run_benchmark_once('ext4','scenario_a',
                    {'record_size':1024,'target_ops':3000,'measure_sec':3600,'warmup_sec':0},1)
            self.assertEqual([c['measure_sec'] for c in calls],[120,1,3600])
            self.assertTrue((Path(folder)/'long-measurement-start.json').exists())
            self.assertTrue((Path(folder)/'long-measurement-end.json').exists())

    def test_all_partitions_and_fixed_post_retention_payload_required(self):
        target=10*1024**3
        self.assertFalse(long_gc.ready([1]*7+[0],target))
        self.assertFalse(long_gc.ready([1]*8,target-1))
        self.assertTrue(long_gc.ready([1]*8,target))
        self.assertFalse(long_gc.ready([],target))

    def test_chunk_duration_is_bounded_and_final_chunk_shrinks(self):
        self.assertEqual(long_gc.prepare_seconds(10*1024**3,3000),120)
        self.assertEqual(long_gc.prepare_seconds(1024,20000),1)
        self.assertEqual(long_gc.prepare_seconds(20000*1024*2,20000),2)

    def test_retention_checks_earliest_live_segment_in_every_partition(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for i in range(8):
                part=root/f'bench-{i}';part.mkdir()
                (part/'00000000000000000010.log').touch()
                (part/'00000000000000000020.log').touch()
            self.assertEqual(long_gc.retention_offsets(root,'bench'),[10]*8)
            (root/'bench-3/00000000000000000000.log').touch()
            self.assertEqual(long_gc.retention_offsets(root,'bench')[3],0)

import importlib.util
from pathlib import Path
import unittest
import tempfile
import json
import hashlib
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('v11',Path(__file__).resolve().parents[1]/'scripts/run-fio-v11-controller.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Version11Tests(unittest.TestCase):
    def test_resume_rejects_changed_original(self):
        cells=m.plan()
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);folder=root/m.relative_output(cells[0]);folder.mkdir(parents=True)
            rawname=cells[0]['workload']+'-qd32-r1.fio.json'
            for name in ['manifest.json',rawname,'prepare.json']:(folder/name).write_text('{}')
            record=dict(cell=cells[0],summary={},boot_id='fixture-boot',
                        raw_sha256=hashlib.sha256((folder/rawname).read_bytes()).hexdigest())
            (root/'completed-cells.json').write_text(json.dumps([record]))
            with patch.object(m,'validate'):
                self.assertEqual(len(m.validate_resume(root,cells)),1)
                (folder/rawname).write_text('{"changed":true}')
                with self.assertRaises(RuntimeError):m.validate_resume(root,cells)

    def test_unique_paths_and_controlled_work(self):
        plan=m.plan()
        self.assertEqual(len(plan),18)
        self.assertEqual(len({m.relative_output(c) for c in plan}),18)
        self.assertEqual(len([c for c in plan if c['experiment']=='1-1']),12)
        m.validate_plan(plan)
        self.assertEqual({c['filesystem'] for c in plan},{'ext4','f2fs'})
        self.assertEqual({c['delay_ms'] for c in plan},{0})
        for c in plan[12:]:
            self.assertEqual(c['total_write_gib'],54)
            self.assertEqual(c['file_gib'],18)
            self.assertEqual(c['warmup'],0)
            self.assertIsNone(c['measure'])

    def test_validator_distinguishes_runtime_and_bytes(self):
        for cell in [m.plan()[0],m.plan()[12]]:
            efficiency=cell['experiment']=='1-2'
            manifest=dict(backend='dm-zns',filesystem=cell['filesystem'],experiment_id=cell['experiment'],
                file_gib=cell['file_gib'],workloads=cell['workload'],bs=cell['bs'],qds='32',
                rounds=1,warmup=cell['warmup'],wal_delay_ms=cell['delay_ms'],randseed=cell['randseed'],
                termination='fixed-write-bytes' if efficiency else 'fixed-time',
                measure=cell['measure'],total_write_gib=cell['total_write_gib'])
            row=dict(experiment_id=cell['experiment'],kernel_log_available=True,
                kernel_storage_errors=[],dm_available=True,wal_diagnostics={'queue_mean_ms':1},
                dm_delta={'wal_diag_commit_count':10},fixed_work_complete=True,
                fixed_work_costs={'fio_write_bytes':54*2**30})
            prep={'jobs':[{'error':0,'write':{'io_bytes':cell['file_gib']*2**30},
                           'read':{'io_bytes':cell['file_gib']*2**30}}]}
            raw={'jobs':[{'error':0,'write':{'io_bytes':54*2**30,'runtime':120000}}]}
            m.validate(cell,manifest,row,raw,prep)
            bad={'jobs':[{'error':0,'write':{'io_bytes':4096,'runtime':100}}]}
            with self.assertRaises(RuntimeError):m.validate(cell,manifest,row,bad,prep)

    def test_rejects_ab_plan_and_wrong_filesystem(self):
        plan=m.plan()
        bad=[dict(c,delay_ms=2) if i==0 else c for i,c in enumerate(plan)]
        with self.assertRaises(RuntimeError):m.validate_plan(bad)
        bad=[dict(c,filesystem='f2fs') for c in plan]
        with self.assertRaises(RuntimeError):m.validate_plan(bad)
        m.validate_mount('/dev/mapper/kafka-zns ext4 rw,relatime','ext4')
        m.validate_mount('/dev/mapper/kafka-zns f2fs rw,nodiscard','f2fs')
        for value,fs in [('/dev/mapper/kafka-zns f2fs rw,nodiscard','ext4'),
                         ('/dev/mapper/kafka-zns ext4 rw,discard','ext4'),
                         ('/dev/mapper/kafka-zns f2fs rw,discard','f2fs')]:
            with self.assertRaises(RuntimeError):m.validate_mount(value,fs)

if __name__=='__main__':unittest.main()

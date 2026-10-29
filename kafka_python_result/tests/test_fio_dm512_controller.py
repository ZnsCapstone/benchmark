import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('dm512',Path(__file__).resolve().parents[1]/'scripts/run-fio-dm512-controller.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Dm512Tests(unittest.TestCase):
    def test_byte_budgets_are_preserved(self):
        self.assertIn('DM_GC_RESERVED_ZONES=8',m.ENV)
        self.assertIn('DM_GC_LOW_WATERMARK=16',m.ENV)
        self.assertIn('DM_GC_HIGH_WATERMARK=20',m.ENV)
        self.assertEqual([4,8,10],[x*.5 for x in [8,16,20]])
        self.assertEqual(18,len(m.n.plan()))

    def test_complete_dm_metrics_required(self):
        cell=m.n.plan()[0]
        manifest=dict(backend='dm-zns',filesystem='f2fs',file_gib=1,workloads='seq-write',
                      bs='4k',qds='32',warmup=20,measure=120)
        row=dict(kernel_log_available=True,kernel_storage_errors=[],dm_available=True)
        raw={'jobs':[{'error':0,'write':{'runtime':120000}}]}
        m.check_result(cell,manifest,row,raw)
        m.check_result(dict(cell,filesystem='ext4'),dict(manifest,filesystem='ext4'),row,raw)
        with self.assertRaises(RuntimeError):
            m.check_result(dict(cell,filesystem='ext4'),manifest,row,raw)
        with self.assertRaises(RuntimeError): m.check_result(cell,manifest,dict(row,dm_available=False),raw)
        with self.assertRaises(RuntimeError): m.check_result(cell,dict(manifest,file_gib=18),row,raw)


if __name__=='__main__': unittest.main()

import importlib.util
from pathlib import Path
import unittest
from fio_evaluation import dm_metrics, wal_diagnostics

spec=importlib.util.spec_from_file_location('wal_plan',Path(__file__).resolve().parents[1]/'scripts/plan-fio-wal-ab.py')
plan=importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan)

class WalDiagnosticsTests(unittest.TestCase):
    def test_remaining_plan_includes_big_requests_and_gc(self):
        cells=plan.remaining_plan('f2fs')
        self.assertEqual(len(cells),10)
        self.assertEqual([x['index'] for x in cells],list(range(1,11)))
        self.assertEqual([x['delay_ms'] for x in cells[:4]],[2,0,0,2])
        self.assertTrue(all(x['bs']=='1M' and x['file_gib']==1 for x in cells[:4]))
        for cell in cells[4:]:
            self.assertEqual(cell['file_gib'],18)
            self.assertEqual(cell['measure'],600)
            cmd=plan.command(cell,'/tmp/diag.ko','/tmp/new-results')
            for key,value in [('--bs',cell['bs']),('--file-gib','18'),('--measure','600')]:
                self.assertEqual(cmd[cmd.index(key)+1],value)
    def test_ab_controller_rejects_missing_instrumentation(self):
        spec=importlib.util.spec_from_file_location('wal_controller',Path(__file__).resolve().parents[1]/'scripts/run-fio-wal-ab-controller.py')
        controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
        cell={'delay_ms':2}
        with self.assertRaises(RuntimeError):
            controller.validate({}, {'wal_delay_ms':2},cell)
        row={'wal_diagnostics':{'queue_mean_ms':1},'dm_delta':{'wal_diag_commit_count':10,'wal_diag_io_errors':0}}
        controller.validate(row,{'wal_delay_ms':2},cell)
        with self.assertRaises(RuntimeError):
            controller.validate(row,{'wal_delay_ms':0},cell)
    def test_numeric_bucket_names(self):
        self.assertEqual(dm_metrics('wal_diag_batch_2_8=12 wal_delay_ms=0'),
                         dict(wal_diag_batch_2_8=12,wal_delay_ms=0))

    def test_request_and_batch_denominators(self):
        d=dict(wal_diag_batches=2,wal_diag_records=8,wal_diag_queue_ns=16000000,
               wal_diag_io_count=2,wal_diag_io_ns=6000000)
        result=wal_diagnostics(d)
        self.assertEqual(result['batch_records_mean'],4)
        self.assertEqual(result['queue_mean_ms'],2)
        self.assertEqual(result['wal_io_batch_mean_ms'],3)
        self.assertIsNone(result['commit_mean_ms'])

    def test_balanced_plan_and_explicit_loading(self):
        for fs in ['ext4','f2fs']:
            cells=plan.plan(fs)
            self.assertEqual(len(cells),8)
            for workload in ['seq-write','rand-write']:
                self.assertEqual([x['delay_ms'] for x in cells if x['workload']==workload],[2,0,0,2])
            command=plan.command(cells[0],'/tmp/diagnostic.ko','/tmp/results')
            self.assertIn('--prepare',command)
            self.assertIn('--wal-delay-ms',command)

if __name__=='__main__':
    unittest.main()

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'fio_zns_suite', Path(__file__).resolve().parents[1] / 'scripts/run-fio-zns-suite.py')
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)


class FioZnsSuiteTests(unittest.TestCase):
    def test_default_plan(self):
        cells = suite.plan(5, 1)
        self.assertEqual(36, len(cells))
        self.assertEqual(30, sum(c['group'] == 'short' for c in cells))
        self.assertEqual(6, sum(c['group'] == 'gc18' for c in cells))
        self.assertEqual({'ext4', 'f2fs'}, {c['filesystem'] for c in cells})
        self.assertEqual({32}, {c['qd'] for c in cells})

    def test_independent_conditions_and_order(self):
        cells = suite.plan(5, 1)
        keys = {(c['filesystem'], c['group'], c['workload'], c['bs'], c['repeat']) for c in cells}
        self.assertEqual(len(cells), len(keys))
        self.assertEqual('ext4', cells[0]['filesystem'])
        self.assertEqual('f2fs', cells[6]['filesystem'])
        for c in cells:
            self.assertEqual((1, 20, 120) if c['group'] == 'short' else (18, 30, 600),
                             (c['file_gib'], c['warmup'], c['measure']))

    def test_validation_requires_metrics_and_kernel(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            suite.save(out / 'summary.json', [dict(kernel_log_available=True,
                       kernel_storage_errors=[], dm_available=True, os={'cpu_steal_pct': .1})])
            suite.save(out / 'seq-write-qd32-r1.fio.json', {'jobs': [{'error': 0}]})
            self.assertFalse(suite.validate(out)['host_interference_warning'])
            records = json.loads((out / 'summary.json').read_text())
            records[0]['dm_available'] = False
            suite.save(out / 'summary.json', records)
            with self.assertRaises(RuntimeError):
                suite.validate(out)


if __name__ == '__main__':
    unittest.main()

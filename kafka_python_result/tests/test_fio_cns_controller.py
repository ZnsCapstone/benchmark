import importlib.util
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location(
    'cns_controller', Path(__file__).resolve().parents[1] / 'scripts/run-fio-cns-controller.py')
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class CnsControllerTests(unittest.TestCase):
    def test_same_plan(self):
        cells = controller.make_plan()
        self.assertEqual(36, len(cells))
        self.assertEqual(30, sum(c['group'] == 'short' for c in cells))
        self.assertEqual(6, sum(c['group'] == 'gc18' for c in cells))
        self.assertEqual({'ext4', 'f2fs'}, {c['filesystem'] for c in cells})

    def test_cns_validation(self):
        row = dict(kernel_log_available=True, kernel_storage_errors=[], dm_available=False,
                   os={'cpu_steal_pct': .2})
        self.assertFalse(controller.validate_cell(row, {'jobs': [{'error': 0}]})['host_interference_warning'])
        with self.assertRaises(RuntimeError):
            controller.validate_cell(dict(row, dm_available=True), {'jobs': [{'error': 0}]})
        with self.assertRaises(RuntimeError):
            controller.validate_cell(row, {'jobs': [{'error': 5}]})

    def test_femu_window(self):
        header = 'epoch_us,host_write_bytes,host_read_bytes,nand_read_pages,nand_program_pages,nand_erase_blocks,gc_moved_pages,gc_count,gc_host_ns,page_bytes\n'
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / 'stats.csv'
            p.write_text(header + '0,0,0,0,0,0,0,0,0,4096\n'
                         + '100,4096,0,0,2,0,1,1,50,4096\n'
                         + '200,8192,0,0,4,1,2,2,100,4096\n')
            result = controller.femu_delta(p, 100, 200)
            self.assertEqual(2, result['nand_wa_device'])
            self.assertEqual(4096, result['gc_moved_bytes'])
            with self.assertRaises(RuntimeError):
                controller.femu_delta(p, 150, 200)

    def test_manifest_matches_before_recovery(self):
        cell = controller.make_plan()[31]
        manifest = dict(backend='cns', filesystem=cell['filesystem'], workloads=cell['workload'],
                        bs=cell['bs'], qds='32', rounds=1, file_gib=18, warmup=30, measure=600)
        controller.check_manifest(manifest, cell)
        with self.assertRaises(RuntimeError):
            controller.check_manifest(dict(manifest, file_gib=1), cell)

    def test_detached_poll_survives_disconnect(self):
        result = lambda code, text: SimpleNamespace(returncode=code, stdout=text, stderr='')
        replies = [result(0, ''), result(255, ''), result(0, 'RUNNING\n'),
                   result(0, 'COMPLETE\n'), result(0, 'finished log')]
        with patch.object(controller, 'call', side_effect=replies) as mocked, \
                patch.object(controller.time, 'sleep'):
            self.assertEqual('finished log', controller.run_detached(
                ['python3', 'runner.py'], '/tmp/test/result', 33, 300))
            self.assertIn('tmux', mocked.call_args_list[0].args[1])


if __name__ == '__main__':
    unittest.main()

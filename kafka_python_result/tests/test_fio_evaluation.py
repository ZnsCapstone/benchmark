import tempfile
from pathlib import Path
import unittest
import argparse
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fio_evaluation import fio_command, parse_fio, femu_delta, dm_metrics, os_metrics, prepare_storage


class FioEvaluationTests(unittest.TestCase):
    def test_prepare_loads_metrics_module_and_checks_status(self):
        with tempfile.TemporaryDirectory() as folder:
            module = Path(folder) / 'dm-zns-base.ko'
            module.write_bytes(b'fixture')
            cfg = MagicMock(MOUNT_POINT='/mnt/fio', RAW_ZNS_DEVICE='/dev/nvme0n1',
                            FS_DEVICE='/dev/mapper/kafka-zns', DM_NAME='kafka-zns',
                            DM_MODULE_PATH=str(module))
            setup = MagicMock()
            args = argparse.Namespace(backend='dm-zns', filesystem='ext4', module_path=str(module), dm_name=None)
            replies = [SimpleNamespace(stdout='FEMU ZNS Controller'), None,
                       SimpleNamespace(stdout='metrics_enabled:bool\n'),
                       SimpleNamespace(stdout=os.uname().release + ' SMP'),
                       SimpleNamespace(stdout='0 100 zns-base metrics=1')]
            with patch.dict('sys.modules', {'settings': cfg, 'system_setup': setup}), \
                    patch.dict(os.environ), patch('fio_evaluation.subprocess.run', side_effect=replies):
                record = prepare_storage(args, Path('/mnt/fio'))
            self.assertTrue(cfg.DM_METRICS)
            self.assertEqual(args.dm_name, 'kafka-zns')
            self.assertEqual(record['module_path'], str(module))
            setup.setup_filesystem.assert_called_once_with('ext4')
            setup.zns_logical_sectors.assert_called_once()

    def test_prepare_rejects_non_femu_before_teardown(self):
        cfg = MagicMock(MOUNT_POINT='/mnt/fio', RAW_ZNS_DEVICE='/dev/nvme0n1')
        setup = MagicMock()
        args = argparse.Namespace(backend='dm-zns', filesystem='ext4', module_path=None)
        with patch.dict('sys.modules', {'settings': cfg, 'system_setup': setup}), \
                patch.dict(os.environ), patch('fio_evaluation.subprocess.run', return_value=SimpleNamespace(stdout='Real SSD')):
            with self.assertRaisesRegex(RuntimeError, 'non-FEMU'):
                prepare_storage(args, Path('/mnt/fio'))
        setup.setup_filesystem.assert_not_called()

    def test_fixed_range_and_completion_percentiles(self):
        cmd = fio_command('/mnt/data', 1024, 'randrw', '4k', 4, 30, 120, '/tmp/out')
        self.assertIn('--size=1024', cmd)
        self.assertIn('--time_based=1', cmd)
        self.assertIn('--allow_file_create=0', cmd)
        self.assertIn('--rwmixread=70', cmd)
        metrics = parse_fio({'jobs': [{'error': 0, 'write': {'iops': 20, 'bw_bytes': 1048576,
                   'clat_ns': {'percentile': {'95.000000': 2000000, '99.000000': 5000000}}}}]})
        self.assertEqual(metrics['write_p95_ms'], 2)
        self.assertEqual(metrics['write_p99_ms'], 5)
        self.assertEqual(metrics['write_mib_s'], 1)

    def test_wa_requires_timestamp_coverage_and_nonzero_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'stats.csv'
            path.write_text('epoch_us,host_write_bytes,nand_program_bytes\n1,0,0\n2,10,30\n3,20,60\n')
            self.assertEqual(femu_delta(path, 1, 3)['nand_wa_device'], 3)
            self.assertFalse(femu_delta(path, 5, 6)['available'])
            path.write_text('epoch_us,host_write_bytes,nand_program_bytes\n1,0,0\n2,0,0\n')
            self.assertIsNone(femu_delta(path, 1, 2)['nand_wa_device'])

    def test_errors_are_not_performance_results(self):
        with self.assertRaises(ValueError):
            parse_fio({'jobs': [{'error': 28}]})
        self.assertEqual(dm_metrics('0 100 zns-base gc_runs=3 mapping_resident_bytes=64'),
                         {'gc_runs': 3, 'mapping_resident_bytes': 64})

    def test_block_counters_are_interval_not_lifetime_values(self):
        rows = [
            {'epoch_us': 1000000, 'diskstats': ['259 0 nvme0n1 0 0 0 0 0 0 0 0 0 0 0']},
            {'epoch_us': 3000000, 'diskstats': ['259 0 nvme0n1 2 0 8 20 4 0 16 80 0 1000 2000']},
        ]
        value = os_metrics(rows)['devices']['nvme0n1']
        self.assertEqual(value['util_pct'], 50)
        self.assertAlmostEqual(value['await_ms'], 100 / 6)
        self.assertEqual(value['average_queue'], 1)
        self.assertEqual(value['write_bytes'], 8192)

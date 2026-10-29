import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

spec=importlib.util.spec_from_file_location('native_kafka',Path(__file__).resolve().parents[1]/'scripts/run-native-kafka-cell.py')
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)

class NativeKafkaTests(unittest.TestCase):
    def stats(self,available):
        return SimpleNamespace(f_bavail=available//4096,f_frsize=4096,
                               f_blocks=32*1024**3//4096,f_bfree=available//4096)

    def test_reserved_eight_gib_is_not_counted_as_user_data(self):
        result=native.logical_usage(self.stats(24*1024**3))
        self.assertEqual(result['used_percent'],0)
        self.assertEqual(result['usable_bytes'],24*1024**3)
        self.assertEqual(result['statfs_total_bytes'],32*1024**3)

    def test_eighty_percent_uses_twenty_four_gib_denominator(self):
        result=native.logical_usage(self.stats(24*1024**3//5))
        self.assertAlmostEqual(result['used_percent'],80,places=4)

    def test_capacity_mismatch_rejected(self):
        with self.assertRaises(RuntimeError):native.logical_usage(self.stats(25*1024**3))

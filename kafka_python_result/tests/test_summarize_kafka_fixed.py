import unittest
from summarize_kafka_fixed import interval


class PhaseCounterTests(unittest.TestCase):
    def row(self, time, logical, user, gc):
        names = ['logical_write_bytes', 'user_write_bytes', 'gc_write_bytes',
                 'wal_write_bytes', 'sstable_write_bytes', 'gc_runs', 'gc_ns',
                 'gc_reclaims', 'gc_moved_bytes', 'compaction_runs', 'compaction_ns', 'flush_runs']
        values = dict.fromkeys(names, 0)
        values.update(logical_write_bytes=logical, user_write_bytes=user, gc_write_bytes=gc)
        return {'time': time, 'dm_status': ' '.join(f'{k}={v}' for k, v in values.items()),
                'raw_block_stat': f'0 0 0 0 0 0 {(user + gc) // 512}'}

    def test_wa_uses_deltas_and_includes_relocation(self):
        r = interval(self.row(0, 1024, 1024, 512), self.row(10, 2048, 2048, 2560))
        self.assertEqual(r['dm_wa'], 3)
        self.assertEqual(r['raw_wa'], 3)

    def test_counter_reset_is_rejected(self):
        with self.assertRaises(ValueError):
            interval(self.row(0, 2048, 2048, 0), self.row(10, 1024, 1024, 0))

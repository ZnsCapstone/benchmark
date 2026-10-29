import unittest
from kafka_only_evaluation import ready, assessment


class KafkaOnlyTests(unittest.TestCase):
    def samples(self):
        return [{'time': t, 'used_percent': 80, 'retention_partitions': 8}
                for t in range(0, 131, 10)]

    def test_ready_requires_full_window_and_all_partition_rotation(self):
        rows = self.samples()
        self.assertTrue(ready(rows, 130))
        self.assertFalse(ready(rows[-3:], 130))
        rows[-1]['retention_partitions'] = 7
        self.assertFalse(ready(rows, 130))

    def test_unstable_occupancy_is_not_ready(self):
        rows = self.samples()
        rows[5]['used_percent'] = 70
        self.assertFalse(ready(rows, 130))
        rows[5]['used_percent'] = 84
        self.assertFalse(ready(rows, 130))

    def test_low_achievement_or_steal_stops_sweep(self):
        result = {'metrics': {'achieved_pct': 99}, 'validity': {'invalid_reasons': []}}
        self.assertTrue(assessment(result)['stable'])
        result['metrics']['achieved_pct'] = 90
        self.assertFalse(assessment(result)['stable'])
        result['metrics'].update(achieved_pct=99, cpu_st_avg=8)
        self.assertFalse(assessment(result)['stable'])

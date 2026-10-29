import unittest
from analyze_kafka_fill import milestones, render


class FillAnalysisTests(unittest.TestCase):
    def test_first_crossing_not_exact_occupancy_measurement(self):
        rows = [{'source': 'first', 'occupancy_max_pct': 12},
                {'source': 'second', 'occupancy_max_pct': 25},
                {'source': 'third', 'occupancy_max_pct': 11}]
        result = dict(milestones(rows))
        self.assertEqual(result[10]['source'], 'first')
        self.assertEqual(result[20]['source'], 'second')
        self.assertIsNone(result[30])

    def test_empty_results_do_not_claim_measurements(self):
        report = render([])
        self.assertIn('아직 완료된 구간 없음', report)
        self.assertIn('근사값', report)

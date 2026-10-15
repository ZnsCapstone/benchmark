import csv
import os
import tempfile
import unittest

import reporting


def entry(valid, state, sent_ops):
    return {
        "round": 1 if valid else 2,
        "config": {
            "profile": "latency", "target_ops": 100, "measure_sec": 60,
            "drain_timeout_sec": 180, "warmup_sec": 20, "producers": 8,
            "use_consumer": False, "dynamic_topics": False,
            "dynamic_topic_rate": 1, "max_in_flight_records": 1000,
            "max_catch_up_records": 10, "max_schedule_lag_ms": 100,
        },
        "metrics": {
            "sent_ops": sent_ops, "ack_window_ops": sent_ops,
            "eventual_ack_ops": sent_ops, "drain_completed": valid,
        },
        "bottleneck": {"is_bottleneck": False, "reasons": []},
        "validity": {
            "valid": valid, "state": state,
            "invalid_reasons": [] if valid else ["drain_completed=false"],
        },
        "java_exit_code": 0, "raw_output_path": "raw.txt",
        "monitor_files": {"iostat": "iostat.txt", "vmstat": "vmstat.txt"},
    }


class ReportingTest(unittest.TestCase):
    def test_summary_excludes_invalid_rounds(self):
        results = {
            "ext4": {1024: {"scenario_a": {
                "rounds": [entry(True, "NOT_SATURATED", 100),
                           entry(False, "INCOMPLETE", 10000)]
            }}}
        }
        with tempfile.TemporaryDirectory() as directory:
            old_csv_dir = reporting.cfg.CSV_DIR
            reporting.cfg.CSV_DIR = directory
            try:
                reporting.save_csv_reports(results)
                path = os.path.join(directory, "summary_by_fs_recordsize_scenario.csv")
                with open(path, newline="", encoding="utf-8") as file:
                    row = next(csv.DictReader(file))
            finally:
                reporting.cfg.CSV_DIR = old_csv_dir
        self.assertEqual("1", row["valid_rounds"])
        self.assertEqual("1", row["invalid_rounds"])
        self.assertEqual("100", row["sent_ops_mean"])


if __name__ == "__main__":
    unittest.main()

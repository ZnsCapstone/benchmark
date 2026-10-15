import csv
from pathlib import Path
import tempfile
import unittest
import os
from unittest.mock import MagicMock, patch

from performance_evaluation import (
    build_parser, child_command, classify_run, midpoint, parse_rates, run_single,
)


FIELDS = [
    "valid", "invalid_reasons", "run_state", "drain_completed", "achieved_pct",
    "achieved_ops", "ack_stall_max_sec", "send_errors", "failed_requests",
    "unresolved_after_drain", "consumer_record_delta", "malformed_headers",
    "payload_crc_errors", "sequence_gaps", "duplicate_records", "out_of_order_records",
    "use_consumer", "consumer_drain_completed", "topics_failed",
]


class PerformanceEvaluationTest(unittest.TestCase):
    def test_kafka_only_selects_runner_without_prefill_and_propagates_settings(self):
        args = build_parser().parse_args([
            "--filesystem", "f2fs", "--workload", "kafka-only",
            "--scenario", "read-write",
            "--warmup", "10800", "--kafka-retention-gib", "16",
        ])
        with tempfile.TemporaryDirectory() as directory:
            args.run_dir = directory
            args.rate = 2000
            cfg = MagicMock()
            runner = MagicMock()
            runner.run.return_value = 0
            with patch.dict(os.environ), patch.dict(
                "sys.modules", {"settings": cfg, "bench_runner": runner}
            ), patch("performance_evaluation.capture_kernel_window"):
                self.assertEqual(run_single(args), 0)
                self.assertEqual(runner.run.call_args.args[0][-1], "steady")
                self.assertEqual(os.environ["BENCH_STEADY_WARMUP_SECONDS"], "10800")
                self.assertEqual(os.environ["BENCH_STEADY_RETENTION_TOTAL_BYTES"],
                                 str(16 * 1024**3))
            command = child_command(Path("evaluation.py"), args, 2000, Path(directory))
            self.assertEqual(command[command.index("--workload") + 1], "kafka-only")

    def write_result(self, root: Path, **overrides):
        row = {field: "0" for field in FIELDS}
        row.update({
            "valid": "True",
            "run_state": "COMPLETED",
            "drain_completed": "True",
            "achieved_pct": "99.5",
            "achieved_ops": "3980",
            "ack_stall_max_sec": "3",
            "use_consumer": "True",
            "consumer_drain_completed": "True",
        })
        row.update({key: str(value) for key, value in overrides.items()})
        (root / "csv").mkdir(parents=True)
        (root / "env").mkdir(parents=True)
        with (root / "csv" / "full_results.csv").open(
            "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerow(row)
        (root / "runner_exit_code.txt").write_text("0\n", encoding="utf-8")
        (root / "env" / "performance-kernel-window.log").write_text(
            "clean kernel window\n", encoding="utf-8"
        )

    def test_stable_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root)
            result = classify_run(root, 95.0, 60)
            self.assertTrue(result["stable"])
            self.assertEqual(result["status"], "stable")

    def test_low_achievement_is_saturation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root, achieved_pct="94.9")
            result = classify_run(root, 95.0, 60)
            self.assertFalse(result["stable"])
            self.assertTrue(any("achieved_pct" in reason for reason in result["reasons"]))

    def test_integrity_error_is_saturation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(
                root, valid="False", invalid_reasons="sequence_gaps=2", sequence_gaps="2"
            )
            result = classify_run(root, 95.0, 60)
            self.assertFalse(result["stable"])
            self.assertTrue(any("sequence_gaps=2" in reason for reason in result["reasons"]))

    def test_kernel_storage_error_is_saturation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root)
            (root / "env" / "performance-kernel-window.log").write_text(
                "nvme nvme0: I/O 7 QID 1 timeout\n", encoding="utf-8"
            )
            result = classify_run(root, 95.0, 60)
            self.assertFalse(result["stable"])
            self.assertEqual(result["kernel_storage_errors"], 1)

    def test_rates_and_midpoint(self):
        self.assertEqual(parse_rates("2000,4000,6000"), (2000, 4000, 6000))
        self.assertEqual(midpoint(2000, 4000), 3000)
        self.assertEqual(midpoint(10000, 15000), 12000)

    def test_write_only_does_not_require_consumer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root, use_consumer="False", consumer_record_delta=-1,
                              consumer_drain_completed="False")
            self.assertTrue(classify_run(root, 95, 60)["stable"])

    def test_topic_failure_and_incomplete_consumer_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root, topics_failed=1, consumer_drain_completed="False")
            reasons = classify_run(root, 95, 60)["reasons"]
            self.assertIn("round1: topics_failed=1", reasons)
            self.assertIn("round1: consumer_drain_completed=false", reasons)

    def test_runner_failure_is_infrastructure_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_result(root)
            (root / "runner_exit_code.txt").write_text("1\n")
            self.assertEqual(classify_run(root, 95, 60)["status"], "infrastructure_error")

    def test_all_scenarios_have_independent_sweeps(self):
        from performance_evaluation import run_sweep
        args = build_parser().parse_args(["--filesystem", "ext4", "--dry-run"])
        with patch("performance_evaluation.run_sweep", return_value=0) as child:
            self.assertEqual(run_sweep(args), 0)
            self.assertEqual([call.args[0].scenario for call in child.call_args_list],
                             ["write", "read-write", "read-write-topics"])


if __name__ == "__main__":
    unittest.main()

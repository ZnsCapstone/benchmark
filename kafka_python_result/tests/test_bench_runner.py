import unittest
from unittest.mock import call, patch

import bench_runner
import settings as cfg


class BenchRunnerTest(unittest.TestCase):
    def setUp(self):
        self.profile = cfg.ACTIVE_PROFILE
        self.scenario_group = cfg.ACTIVE_SCENARIO_GROUP
        self.scenario_keys = cfg.SCENARIO_KEYS
        self.workload_mode = cfg.ACTIVE_WORKLOAD_MODE

    def tearDown(self):
        cfg.configure_profile(self.profile)
        cfg.ACTIVE_SCENARIO_GROUP = self.scenario_group
        cfg.SCENARIO_KEYS = self.scenario_keys
        cfg.ACTIVE_WORKLOAD_MODE = self.workload_mode

    def test_parse_arguments_accepts_endurance_mode(self):
        rounds = bench_runner.parse_arguments(
            ["bench_final.py", "1", "2", "latency", "baseline", "endurance"]
        )
        self.assertEqual(2, rounds)
        self.assertEqual("endurance", cfg.ACTIVE_WORKLOAD_MODE)
        self.assertEqual(["scenario_a", "scenario_b"], cfg.SCENARIO_KEYS)

    def test_parse_arguments_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            bench_runner.parse_arguments(
                ["bench_final.py", "1", "1", "latency", "baseline", "overnight"]
            )

    @patch.object(bench_runner, "measure_one")
    @patch.object(bench_runner, "fill_filesystem_to")
    @patch.object(bench_runner, "wait_for_filesystem_usage")
    @patch.object(bench_runner, "recreate_main_topic")
    @patch.object(bench_runner, "setup_filesystem")
    @patch.object(bench_runner, "control_kafka")
    def test_occupancy_resets_once_per_record_size(
        self, control_kafka, setup_filesystem, recreate_main_topic,
        wait_for_filesystem_usage, fill_filesystem_to, measure_one,
    ):
        with patch.object(cfg, "FILESYSTEMS", ("ext4",)), \
             patch.object(cfg, "RECORD_SIZES", [1024, 10240]), \
             patch.object(cfg, "OCCUPANCY_POINTS", (20, 40)), \
             patch.object(cfg, "SCENARIO_KEYS", ["scenario_a"]):
            bench_runner.run_occupancy({}, 1)

        self.assertEqual([call("ext4"), call("ext4")], setup_filesystem.call_args_list)
        self.assertEqual([call(20), call(40), call(20), call(40)],
                         fill_filesystem_to.call_args_list)
        self.assertEqual(
            [
                call({}, "ext4", 1024, "scenario_a", 1,
                     occupancy_target=20, phase="occupancy"),
                call({}, "ext4", 1024, "scenario_a", 1,
                     occupancy_target=40, phase="occupancy"),
                call({}, "ext4", 10240, "scenario_a", 1,
                     occupancy_target=20, phase="occupancy"),
                call({}, "ext4", 10240, "scenario_a", 1,
                     occupancy_target=40, phase="occupancy"),
            ],
            measure_one.call_args_list,
        )

    @patch.object(bench_runner, "measure_one")
    @patch.object(bench_runner, "fill_filesystem_to")
    @patch.object(bench_runner, "wait_for_filesystem_usage")
    @patch.object(bench_runner, "recreate_main_topic")
    @patch.object(bench_runner, "setup_filesystem")
    @patch.object(bench_runner, "control_kafka")
    def test_endurance_only_prefills_final_occupancy(
        self, control_kafka, setup_filesystem, recreate_main_topic,
        wait_for_filesystem_usage, fill_filesystem_to, measure_one,
    ):
        with patch.object(cfg, "FILESYSTEMS", ("ext4",)), \
             patch.object(cfg, "RECORD_SIZES", [1024]), \
             patch.object(cfg, "SCENARIO_KEYS", ["scenario_b"]), \
             patch.object(cfg, "OCCUPANCY_POINTS", (20, 40, 60, 80)), \
             patch.object(cfg, "LONG_RECORD_SIZE", 1024), \
             patch.object(cfg, "LONG_SCENARIO", "scenario_b"):
            bench_runner.run_endurance({}, 1)

        fill_filesystem_to.assert_called_once_with(80)
        measure_one.assert_called_once()
        self.assertEqual(
            cfg.ENDURANCE_RETENTION_TOTAL_BYTES,
            measure_one.call_args.kwargs["retention_total_bytes"],
        )


if __name__ == "__main__":
    unittest.main()

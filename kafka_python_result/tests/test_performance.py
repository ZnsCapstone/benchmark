import os
import tempfile
import unittest

from performance import (
    build_java_cmd, evaluate_run, parse_iostat_file, parse_java_metrics,
)


JAVA_OUTPUT = """
 Total Requests : 5950
 Average        : 2.50 ms
 p50 (Median)   : 2.00 ms
 p90            : 4.00 ms
 p99            : 8.00 ms
 p999           : 12.00 ms
 Max            : 20.00 ms
 Achieved OP/s       : 99.17
 Achieved/Target (%) : 99.2
 Total Sent (incl. warmup) : 7950
 Send Errors               : 0
 Drain Time                : 0.25 sec
 Drain Completed           : true
 Sent Requests             : 6000
 Sent OP/s                 : 100.00
 ACK Window Requests       : 5940
 ACK Window OP/s           : 99.00
 Eventual ACK Requests     : 5950
 Eventual ACK OP/s         : 99.17
 Outstanding at End        : 60
 Failed Requests           : 0
 Unresolved After Drain    : 50
 Latency Dropped Samples   : 0
 Backpressure Wait Count   : 3
 Backpressure Wait Time    : 1.25 ms
 Max Observed Outstanding  : 1000
 Catch-up Resets           : 2
 Catch-up Records Skipped  : 7
 Max Schedule Lag          : 10.50 ms
 ACK Stall Count          : 2
 ACK Stall Total          : 7 sec
 ACK Stall Max            : 5 sec
 Consumer Records         : 7950
 Producer ACKed Records   : 7950
 Consumer Record Delta    : 0
 Consumer Drain Completed : true
 Malformed Headers        : 0
 Payload CRC Errors       : 0
 Sequence Gaps            : 0
 Duplicate Records        : 0
 Out-of-order Records     : 0
[TopicCreator] created=10 failed=0 (rate=1/sec)
[TopicCreator] first_failure_elapsed_ms=-1 first_failure=none
"""


class PerformanceTest(unittest.TestCase):
    def test_parses_extended_java_metrics(self):
        metrics = parse_java_metrics(JAVA_OUTPUT)
        self.assertEqual(6000, metrics["sent_requests"])
        self.assertEqual(5940, metrics["ack_window_requests"])
        self.assertEqual(5950, metrics["eventual_ack_requests"])
        self.assertEqual(60, metrics["outstanding_at_end"])
        self.assertEqual(10.5, metrics["max_schedule_lag_ms"])
        self.assertEqual(10, metrics["topics_created"])
        self.assertEqual(5, metrics["ack_stall_max_sec"])
        self.assertEqual(7950, metrics["consumer_records"])

    def test_consumer_integrity_error_is_invalid(self):
        metrics = parse_java_metrics(
            JAVA_OUTPUT.replace("Payload CRC Errors       : 0",
                                "Payload CRC Errors       : 1")
        )
        result = evaluate_run(metrics, 0)
        self.assertFalse(result["valid"])
        self.assertEqual("FAILED", result["state"])
        self.assertIn("payload_crc_errors=1", result["invalid_reasons"])

    def test_incomplete_run_is_invalid(self):
        result = evaluate_run(parse_java_metrics(JAVA_OUTPUT), 0)
        self.assertFalse(result["valid"])
        self.assertEqual("INCOMPLETE", result["state"])
        self.assertIn("unresolved_after_drain=50", result["invalid_reasons"])

    def test_send_error_is_failed(self):
        metrics = parse_java_metrics(
            JAVA_OUTPUT.replace("Send Errors               : 0",
                                "Send Errors               : 1")
        )
        result = evaluate_run(metrics, 0)
        self.assertFalse(result["valid"])
        self.assertEqual("FAILED", result["state"])

    def test_java_command_includes_profile_controls(self):
        command = build_java_cmd({
            "record_size": 1024, "target_ops": 20000, "producers": 8,
            "use_consumer": False, "dynamic_topics": False,
            "dynamic_topic_rate": 1, "warmup_sec": 20, "measure_sec": 60,
            "drain_timeout_sec": 180, "max_in_flight_records": 1000,
            "max_catch_up_records": 10, "max_schedule_lag_ms": 100,
            "fail_fast_stall_sec": 60,
        })
        self.assertIn("--max-in-flight-records 1000", command)
        self.assertIn("--max-catch-up-records 10", command)
        self.assertIn("--max-schedule-lag-ms 100", command)
        self.assertIn("--fail-fast-stall-sec 60", command)

    def test_parses_raw_and_mapper_rows_separately(self):
        content = """
Device r/s w/s rMB/s wMB/s r_await w_await aqu-sz %util
nvme0n1 0 10 0 2 0 3 1 40
kafka-zns 0 10 0 2 0 5 2 80

"""
        with tempfile.NamedTemporaryFile("w", delete=False) as file:
            file.write(content)
            path = file.name
        try:
            raw = parse_iostat_file(path, "nvme0n1")
            mapper = parse_iostat_file(path, "kafka-zns")
        finally:
            os.unlink(path)
        self.assertEqual(40, raw["util_avg"])
        self.assertEqual(80, mapper["util_avg"])
        self.assertEqual(5, mapper["await_avg"])


if __name__ == "__main__":
    unittest.main()

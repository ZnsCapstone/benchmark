"""Exercise diagnostic collection with fake commands; never opens a block device."""
import datetime
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class DiagnosticCollectionTest(unittest.TestCase):
    def test_wrapper_uses_legacy_journal_timestamp_and_collects_logs(self):
        with tempfile.TemporaryDirectory(prefix="gc-diagnostic-test-") as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            for name, body in {
                "sudo": 'if [ "$1" = -n ] && [ "$2" = true ]; then exit 0; fi\nexec "$@"\n',
                "modinfo": "echo 'gc_diag_budget:diagnostic budget'\n",
                "git": "echo fake-commit\n",
                "journalctl": 'printf "%s\\n" "$@"\n',
            }.items():
                path = binary / name
                path.write_text("#!/bin/sh\n" + body)
                path.chmod(0o755)
            (root / "scripts").mkdir()
            shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts/run-gc-diagnostic.sh",
                            root / "scripts/run-gc-diagnostic.sh")
            (root / "scripts/run-benchmark.sh").write_text("#!/bin/sh\necho fake-benchmark\n")
            for name in ("dm-zns-base/src/dm-zns-base.ko",
                         "dm-zns-base/src/dm-zns-base-main.c",
                         "Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar",
                         "kafka-4.2.0-src/logs/server.log"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("test fixture\n")
            subprocess.run(["bash", str(root / "scripts/run-gc-diagnostic.sh"), "ext4", "82", "0"],
                           env={"PATH": f"{binary}:{os.environ['PATH']}", "HOME": str(root),
                                "DIAG_DURATION_SECONDS": "1"},
                           check=True, capture_output=True, text=True)
            folders = list((root / "results/gc-diagnostics").glob("run.*"))
            self.assertEqual(len(folders), 1)
            result = folders[0]
            args = (result / "kernel.log").read_text().splitlines()
            for option in ("--since", "--until"):
                timestamp = args[args.index(option) + 1]
                datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
            self.assertEqual((result / "status.txt").read_text().strip(),
                             "benchmark_exit=0 journal_exit=0 broker_copy_exit=0")
            self.assertTrue((result / "broker-logs/server.log").is_file())
            environment = (result / "environment.txt").read_text()
            self.assertIn("DM_ZNS_GIT_BRANCH=", environment)
            self.assertIn("DM_ZNS_GIT_COMMIT=", environment)
            self.assertIn("DM_ZNS_GIT_STATUS_SHA256=", environment)
            self.assertIn("DM_LOGICAL_CAPACITY_PERCENT=82", environment)
            self.assertIn("dm-zns-base-main.c", environment)

    def test_wrapper_rejects_capacity_outside_percent_range(self):
        script = Path(__file__).resolve().parents[1] / "scripts/run-gc-diagnostic.sh"
        result = subprocess.run(
            ["bash", str(script), "ext4", "100", "0"],
            check=False, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("integer from 1 to 99", result.stderr)


if __name__ == "__main__":
    unittest.main()

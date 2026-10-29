#!/usr/bin/env python3
"""Adaptive rate sweep for FEMU filesystem performance evaluation.

Each rate is an independent endurance-style run on a freshly initialized
filesystem.  The sweep stops at the first saturated/invalid point and may run
one midpoint between the last stable rate and the first saturated rate.

The existing run-benchmark.sh and benchmark 4 implementation are not changed.
This entrypoint injects a single target rate into the existing Python runner in
an isolated child process and writes results below a performance_eval directory.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback
from copy import copy

SCENARIOS = {
    "write": ("scenario_a", "baseline"),
    "read-write": ("scenario_b", "baseline"),
    "read-write-topics": ("scenario_b_dynamic", "dynamic"),
}


DEFAULT_RATES = (1000, 1500, 2000, 2500, 3000)
ZERO_FIELDS = (
    "send_errors",
    "failed_requests",
    "unresolved_after_drain",
    "consumer_record_delta",
    "malformed_headers",
    "payload_crc_errors",
    "sequence_gaps",
    "duplicate_records",
    "out_of_order_records",
    "topics_failed",
)
KERNEL_ERROR_PATTERNS = (
    re.compile(r"nvme.*(?:I/O.*timeout|Abort status|reset controller)", re.I),
    re.compile(r"(?:blk_update_request|Buffer I/O|end_request).*I/O error", re.I),
    re.compile(r"EXT4-fs (?:error|warning.*aborted journal)", re.I),
    re.compile(r"F2FS-fs.*(?:ERROR|error|corrupt)", re.I),
    re.compile(r"Remounting filesystem read-only", re.I),
)


def parse_rates(value: str) -> tuple[int, ...]:
    try:
        rates = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("rates must be comma-separated integers") from exc
    if not rates or any(rate <= 0 for rate in rates):
        raise argparse.ArgumentTypeError("rates must be positive")
    if tuple(sorted(set(rates))) != rates:
        raise argparse.ArgumentTypeError("rates must be unique and increasing")
    return rates


def as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def as_int(row: dict[str, str], field: str) -> int:
    value = row.get(field, "")
    return int(float(value)) if value else 0


def as_float(row: dict[str, str], field: str) -> float:
    value = row.get(field, "")
    return float(value) if value else 0.0


def kernel_errors(path: Path) -> list[str]:
    if not path.exists():
        return []
    matches = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if any(pattern.search(line) for pattern in KERNEL_ERROR_PATTERNS):
            matches.append(line)
    return matches


def classify_run(run_dir: Path, minimum_achieved_pct: float,
                 stall_limit_seconds: int) -> dict[str, object]:
    csv_path = run_dir / "csv" / "full_results.csv"
    exit_path = run_dir / "runner_exit_code.txt"
    if not csv_path.exists() or not exit_path.exists():
        return {
            "status": "infrastructure_error",
            "stable": False,
            "reasons": ["result CSV or runner exit marker missing"],
            "rounds": 0,
        }

    try:
        runner_exit = int(exit_path.read_text(encoding="utf-8").strip())
        with csv_path.open(newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
    except (OSError, ValueError, csv.Error) as exc:
        return {
            "status": "infrastructure_error",
            "stable": False,
            "reasons": [f"cannot parse result: {exc}"],
            "rounds": 0,
        }
    if not rows:
        return {
            "status": "infrastructure_error",
            "stable": False,
            "reasons": ["result CSV has no rows"],
            "rounds": 0,
        }

    reasons: list[str] = []
    if runner_exit != 0:
        return {"status": "infrastructure_error", "stable": False,
                "reasons": [f"runner_exit_code={runner_exit}"], "rounds": len(rows)}
    for index, row in enumerate(rows, 1):
        prefix = f"round{index}"
        if not as_bool(row.get("valid", "")):
            detail = row.get("invalid_reasons", "") or row.get("run_state", "invalid")
            reasons.append(f"{prefix}: valid=false ({detail})")
        if not as_bool(row.get("drain_completed", "")):
            reasons.append(f"{prefix}: drain_completed=false")
        fields = ZERO_FIELDS
        if row.get("use_consumer", "True").lower() == "false":
            fields = tuple(field for field in fields if field not in {
                "consumer_record_delta", "malformed_headers", "payload_crc_errors",
                "sequence_gaps", "duplicate_records", "out_of_order_records",
            })
        elif "consumer_drain_completed" in row and not as_bool(row["consumer_drain_completed"]):
            reasons.append(f"{prefix}: consumer_drain_completed=false")
        for field in fields:
            value = as_int(row, field)
            if value != 0:
                reasons.append(f"{prefix}: {field}={value}")
        achieved_pct = as_float(row, "achieved_pct")
        if achieved_pct < minimum_achieved_pct:
            reasons.append(
                f"{prefix}: achieved_pct={achieved_pct:.2f}<{minimum_achieved_pct:.2f}"
            )
        stall = as_int(row, "ack_stall_max_sec")
        if stall >= stall_limit_seconds:
            reasons.append(f"{prefix}: ack_stall_max_sec={stall}>={stall_limit_seconds}")

    errors = kernel_errors(run_dir / "env" / "performance-kernel-window.log")
    if errors:
        reasons.append(f"kernel_storage_errors={len(errors)}")

    return {
        "status": "stable" if not reasons else "saturated",
        "stable": not reasons,
        "reasons": reasons,
        "rounds": len(rows),
        "achieved_pct_min": min(as_float(row, "achieved_pct") for row in rows),
        "achieved_ops_mean": sum(as_float(row, "achieved_ops") for row in rows) / len(rows),
        "ack_stall_max_sec": max(as_int(row, "ack_stall_max_sec") for row in rows),
        "runner_exit_code": runner_exit,
        "kernel_storage_errors": len(errors),
    }


def configure_result_paths(cfg, run_dir: Path) -> None:
    cfg.RESULT_DIR = str(run_dir)
    cfg.ROUND_DIR = str(run_dir / "rounds")
    cfg.RAW_DIR = str(run_dir / "raw")
    cfg.MONITOR_DIR = str(run_dir / "monitor")
    cfg.CSV_DIR = str(run_dir / "csv")
    cfg.ENV_DIR = str(run_dir / "env")
    cfg.EXPERIMENT_KRAFT_CONFIG = str(run_dir / "env" / "server-femu.properties")
    if os.environ.get("BENCH_LONG_GC") == "1":
        import long_gc
        long_gc.install(cfg)


def capture_kernel_window(run_dir: Path, since_epoch: int) -> None:
    destination = run_dir / "env" / "performance-kernel-window.log"
    destination.parent.mkdir(parents=True, exist_ok=True)
    command = ["sudo", "journalctl", "-k", "--since", f"@{since_epoch}", "--no-pager"]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    content = result.stdout
    if result.returncode != 0:
        content += f"\n[journalctl exit={result.returncode}]\n{result.stderr}"
    destination.write_text(content, encoding="utf-8")


def run_single(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).resolve()
    if run_dir.exists() and any(run_dir.iterdir()):
        print(f"[Error] result directory is not empty: {run_dir}", file=sys.stderr)
        return 2

    os.environ["BENCH_STORAGE_BACKEND"] = args.backend
    os.environ["BENCH_FILESYSTEMS"] = args.filesystem
    os.environ["BENCH_RECORD_SIZES"] = "1024"
    os.environ["BENCH_OCCUPANCY_POINTS"] = str(args.occupancy)
    os.environ["BENCH_LONG_RECORD_SIZE"] = "1024"
    scenario_key, scenario_group = SCENARIOS[args.scenario]
    os.environ["BENCH_LONG_SCENARIO"] = scenario_key
    os.environ["BENCH_LONG_WARMUP_SECONDS"] = str(args.warmup)
    os.environ["BENCH_STEADY_WARMUP_SECONDS"] = str(args.warmup)
    os.environ["BENCH_STEADY_RETENTION_TOTAL_BYTES"] = str(
        args.kafka_retention_gib * 1024**3
    )
    os.environ["BENCH_LONG_DURATION_SECONDS"] = str(args.measure)
    os.environ["BENCH_FAIL_FAST_STALL_SECONDS"] = str(args.stall_limit)

    # Import only after the child-specific environment has been installed.
    import settings as cfg

    configure_result_paths(cfg, run_dir)
    cfg.LATENCY_OPS_BY_RECORD_SIZE[1024] = args.rate
    cfg.PROFILES["latency"]["ops_by_record_size"][1024] = args.rate
    if args.scenario == "read-write-topics":
        cfg.JAVA_BENCH_CMD = cfg.JAVA_BENCH_CMD.replace(
            "java ", "java -Dbenchmark.topicDeletion=true ", 1
        )

    since_epoch = int(dt.datetime.now().timestamp())
    exit_code = 1
    try:
        import bench_runner

        # Reject incomplete prefill even if fio exits zero after SIGTERM.
        original_prefill = bench_runner.fill_filesystem_to

        def checked_prefill(target):
            usage = original_prefill(target)
            if abs(usage["used_percent"] - target) > cfg.OCCUPANCY_TOLERANCE_PERCENT:
                raise RuntimeError("prefill did not reach requested occupancy")
            return usage

        bench_runner.fill_filesystem_to = checked_prefill

        exit_code = bench_runner.run([
            "performance_evaluation.py",
            "1",
            str(args.rounds),
            "latency",
            scenario_group,
            "steady" if args.workload == "kafka-only" else "endurance",
        ])
    except BaseException:  # Preserve a partial result for post-mortem analysis.
        traceback.print_exc()
        exit_code = 1
    finally:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "runner_exit_code.txt").write_text(
            f"{exit_code}\n", encoding="utf-8"
        )
        capture_kernel_window(run_dir, since_epoch)
    return exit_code


def midpoint(previous: int, saturated: int) -> int | None:
    if saturated - previous <= 1:
        return None
    value = (previous + saturated) // 2
    if value >= 1000:
        value = (value // 1000) * 1000
    return value if previous < value < saturated else None


def write_summary(parent: Path, manifest: dict[str, object],
                  records: list[dict[str, object]]) -> None:
    payload = dict(manifest)
    payload["results"] = records
    stable_rates = [int(record["rate"]) for record in records if record["stable"]]
    saturated_rates = [int(record["rate"]) for record in records
                       if record["status"] == "saturated"]
    payload["highest_stable_rate"] = max(stable_rates) if stable_rates else None
    payload["first_saturated_rate"] = min(saturated_rates) if saturated_rates else None
    (parent / "sweep_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    columns = (
        "rate", "kind", "status", "stable", "rounds", "achieved_ops_mean",
        "achieved_pct_min", "ack_stall_max_sec", "kernel_storage_errors", "reasons",
        "result_dir",
    )
    with (parent / "sweep_summary.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for record in records:
            row = {key: record.get(key, "") for key in columns}
            row["reasons"] = " | ".join(record.get("reasons", []))
            writer.writerow(row)


def child_command(script: Path, args: argparse.Namespace, rate: int,
                  run_dir: Path) -> list[str]:
    return [
        sys.executable, str(script), "--single-rate",
        "--backend", args.backend,
        "--filesystem", args.filesystem,
        "--scenario", args.scenario,
        "--workload", args.workload,
        "--kafka-retention-gib", str(args.kafka_retention_gib),
        "--rate", str(rate),
        "--rounds", str(args.rounds),
        "--warmup", str(args.warmup),
        "--measure", str(args.measure),
        "--occupancy", str(args.occupancy),
        "--stall-limit", str(args.stall_limit),
        "--run-dir", str(run_dir),
    ]


def run_sweep(args: argparse.Namespace) -> int:
    if (args.workload == "kafka-only" and args.output_dir and
            (Path(args.output_dir).resolve().parent / "SKIP_KAFKA_ONLY").exists()):
        print("[Skip] Kafka-only runs disabled for this suite pending disk-aging support.")
        return 0
    if args.scenario == "all":
        base = Path(args.output_dir).resolve() if args.output_dir else None
        for name in SCENARIOS:
            child = copy(args)
            child.scenario = name
            child.output_dir = str(base / name) if base else None
            result = run_sweep(child)
            if result != 0:
                return result
        return 0
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parent = Path(args.output_dir or (
        Path(__file__).resolve().parents[1] / "results" / "performance-evaluation" /
        f"{args.backend}-{args.filesystem}" /
        f"performance_eval_{args.workload}_{args.scenario}_{timestamp}"
    )).resolve()
    manifest = {
        "started_at": dt.datetime.now().astimezone().isoformat(),
        "backend": args.backend,
        "filesystem": args.filesystem,
        "workload": args.workload,
        "scenario": args.scenario,
        "topic_create_delete": args.scenario == "read-write-topics",
        "topic_rate_per_second": 5 if args.scenario == "read-write-topics" else 0,
        "fio_prefill": args.workload == "prefill",
        "kafka_retention_total_bytes": (
            args.kafka_retention_gib * 1024**3 if args.workload == "kafka-only"
            else int(os.environ.get("BENCH_ENDURANCE_RETENTION_TOTAL_BYTES", 2 * 1024**3))
        ),
        "rates": list(args.rates),
        "rounds_per_rate": args.rounds,
        "warmup_seconds": args.warmup,
        "measurement_seconds": args.measure,
        "occupancy_percent": args.occupancy if args.workload == "prefill" else None,
        "minimum_achieved_percent": args.minimum_achieved_pct,
        "ack_stall_limit_seconds": args.stall_limit,
        "midpoint_refinement": not args.no_refine,
    }
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print(f"[Plan] Results: {parent}")
    if args.dry_run:
        print("[Dry-run] No device or filesystem was modified.")
        return 0

    parent.mkdir(parents=True, exist_ok=False)
    (parent / "sweep_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    records: list[dict[str, object]] = []
    last_stable: int | None = None
    first_saturated: int | None = None
    script = Path(__file__).resolve()

    def execute(rate: int, kind: str) -> dict[str, object]:
        index = len(records) + 1
        run_dir = parent / f"{index:02d}_{rate}ops_{kind}"
        print(f"\n[Rate] {rate} ops/s ({kind})")
        completed = subprocess.run(child_command(script, args, rate, run_dir), check=False)
        record = classify_run(run_dir, args.minimum_achieved_pct, args.stall_limit)
        record.update({
            "rate": rate,
            "kind": kind,
            "result_dir": str(run_dir),
            "child_exit_code": completed.returncode,
        })
        records.append(record)
        write_summary(parent, manifest, records)
        print(f"[Rate] {rate} ops/s -> {record['status']}")
        for reason in record.get("reasons", []):
            print(f"  - {reason}")
        return record

    for rate in args.rates:
        record = execute(rate, "planned")
        if record["status"] == "infrastructure_error":
            print("[Stop] Incomplete result; higher rates were not attempted.")
            return 2
        if record["stable"]:
            last_stable = rate
            continue
        first_saturated = rate
        break

    if first_saturated is not None and last_stable is not None and not args.no_refine:
        refine_rate = midpoint(last_stable, first_saturated)
        if refine_rate is not None and refine_rate not in args.rates:
            record = execute(refine_rate, "refinement")
            if record["status"] == "infrastructure_error":
                return 2

    write_summary(parent, manifest, records)
    stable_rates = [int(record["rate"]) for record in records if record["stable"]]
    print("\n[Complete] Adaptive sweep finished.")
    print(f"[Complete] Highest stable rate: {max(stable_rates) if stable_rates else 'none'}")
    if first_saturated is not None:
        print(f"[Complete] First saturated planned rate: {first_saturated}")
    print(f"[Complete] Summary: {parent / 'sweep_summary.csv'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an adaptive FEMU Kafka performance sweep without changing benchmark 4."
    )
    parser.add_argument("--backend", choices=("dm-zns", "cns"), default="cns")
    parser.add_argument("--filesystem", choices=("ext4", "f2fs"), required=True)
    parser.add_argument("--scenario", choices=(*SCENARIOS, "all"), default="all",
                        help="independent write/read-write/read-write-topics sweeps")
    parser.add_argument("--workload", choices=("prefill", "kafka-only"), default="prefill",
                        help="prefill: fio aging; kafka-only: Kafka fills a fresh filesystem")
    parser.add_argument("--kafka-retention-gib", type=int, default=16,
                        help="total topic retention for kafka-only (default: 16 GiB)")
    parser.add_argument("--rates", type=parse_rates, default=DEFAULT_RATES)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=300)
    parser.add_argument("--measure", type=int, default=600)
    parser.add_argument("--occupancy", type=int, default=80)
    parser.add_argument("--minimum-achieved-pct", type=float, default=95.0)
    parser.add_argument("--stall-limit", type=int, default=60)
    parser.add_argument("--no-refine", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--dry-run", action="store_true")

    # Internal mode used to isolate settings.py globals for each rate.
    parser.add_argument("--single-rate", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--rate", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--run-dir", help=argparse.SUPPRESS)
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.kafka_retention_gib < 1:
        parser.error("--kafka-retention-gib must be >= 1")
    for field in ("rounds", "warmup", "measure", "occupancy", "stall_limit"):
        value = getattr(args, field)
        minimum = 0 if field == "warmup" else 1
        if value < minimum:
            parser.error(f"--{field.replace('_', '-')} must be >= {minimum}")
    if args.occupancy > 80:
        parser.error("--occupancy must be <= 80")
    if not 0 < args.minimum_achieved_pct <= 100:
        parser.error("--minimum-achieved-pct must be in (0, 100]")
    if args.single_rate:
        if args.scenario == "all":
            parser.error("single-rate requires one scenario")
        if not args.rate or args.rate <= 0 or not args.run_dir:
            parser.error("internal single-rate mode requires --rate and --run-dir")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    return run_single(args) if args.single_rate else run_sweep(args)


if __name__ == "__main__":
    raise SystemExit(main())

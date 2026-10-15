"""Top-level experiment orchestration."""

import sys
from copy import deepcopy

import settings as cfg
from bench_logging import TeeLogger
from bench_utils import run_cmd_quiet
from performance import run_benchmark_once
from reporting import save_csv_reports, save_json_snapshot, save_summary_report
from system_setup import (
    capture_environment, control_kafka, count_kafka_topics, prepare_experiment_kraft_config,
    fill_filesystem_to, filesystem_usage, recreate_main_topic, remove_dm_stack, setup_filesystem,
    unmount_log_device, validate_kafka_environment, wait_for_filesystem_usage,
)


def init_result_structure():
    return {
        fs: {size: {scenario: {"rounds": []} for scenario in cfg.SCENARIO_KEYS}
             for size in cfg.RECORD_SIZES}
        for fs in cfg.FILESYSTEMS
    }


def parse_arguments(argv):
    if len(argv) < 2 or len(argv) > 6:
        raise ValueError(
            "Usage: python3 bench_final.py <0=fixed|1=dynamic> [rounds] "
            "[saturation|latency] [baseline|dynamic|all] [fresh|occupancy|long]"
        )
    cfg.configure_dm_implementation(argv[1])
    rounds = int(argv[2]) if len(argv) >= 3 else cfg.DEFAULT_ROUNDS
    profile = argv[3] if len(argv) >= 4 else cfg.DEFAULT_PROFILE
    cfg.configure_profile(profile)
    scenario_group = argv[4] if len(argv) >= 5 else cfg.DEFAULT_SCENARIO_GROUP
    cfg.configure_scenario_group(scenario_group)
    workload_mode = argv[5] if len(argv) == 6 else cfg.DEFAULT_WORKLOAD_MODE
    cfg.configure_workload_mode(workload_mode)
    if rounds < 1:
        raise ValueError("rounds must be positive")
    return rounds


def persist_results(results, round_number, final=False):
    save_json_snapshot(results)
    save_csv_reports(results)
    save_summary_report(results, round_number, final=final)


def benchmark_config(record_size, scenario, measure_sec=None, warmup_sec=None):
    profile = cfg.active_profile_config()
    return {
        "record_size": record_size,
        "target_ops": cfg.FIXED_OPS_BY_RECORD_SIZE[record_size],
        "producers": scenario["producers"],
        "use_consumer": scenario["use_consumer"],
        "dynamic_topics": scenario["dynamic_topics"],
        "dynamic_topic_rate": cfg.TOPIC_RATE,
        "warmup_sec": cfg.WARMUP_SECONDS if warmup_sec is None else warmup_sec,
        "measure_sec": cfg.MEASURE_DURATION if measure_sec is None else measure_sec,
        "drain_timeout_sec": cfg.DRAIN_TIMEOUT_SECONDS,
        "fail_fast_stall_sec": cfg.FAIL_FAST_STALL_SECONDS,
        "profile": cfg.ACTIVE_PROFILE,
        "max_in_flight_records": profile["max_in_flight_records"],
        "max_catch_up_records": profile["max_catch_up_records"],
        "max_schedule_lag_ms": profile["max_schedule_lag_ms"],
    }


def measure_one(results, fs_type, record_size, scenario_key, round_number,
                occupancy_target=None, phase="measure", measure_sec=None, warmup_sec=None,
                retention_total_bytes=None):
    scenario = deepcopy(cfg.SCENARIO_TEMPLATES[scenario_key])
    config = benchmark_config(record_size, scenario, measure_sec, warmup_sec)
    config["phase"] = phase
    config["occupancy_target_pct"] = occupancy_target
    config["topic_retention_total_bytes"] = retention_total_bytes
    control_kafka("start")
    recreate_main_topic(retention_total_bytes=retention_total_bytes)
    if occupancy_target is not None:
        wait_for_filesystem_usage(occupancy_target)
    # Capture after deleting the previous topic so the value describes the
    # actual occupancy seen by this workload, not stale Kafka log segments.
    config["fs_usage_before"] = filesystem_usage()
    print(
        f"[{cfg.ACTIVE_PROFILE}/{phase}] {fs_type.upper()} / {scenario_key} / "
        f"{record_size}B -> target_ops={config['target_ops']}, "
        f"fs_used={config['fs_usage_before']['used_percent']:.2f}%"
    )
    phase_tag = phase if occupancy_target is None else f"{phase}_used{occupancy_target}pct"
    measured = run_benchmark_once(
        fs_type, scenario_key, config, round_number, phase_tag=phase_tag,
    )
    measured["metrics"]["final_topic_count"] = count_kafka_topics()
    control_kafka("stop")
    config["fs_usage_after"] = filesystem_usage()
    measured["config"] = deepcopy(config)
    measured["round"] = round_number
    measured["calibration_reference"] = {
        "mode": cfg.ACTIVE_PROFILE, "chosen_ops": config["target_ops"],
    }
    results[fs_type][record_size][scenario_key]["rounds"].append(measured)
    persist_results(results, round_number)


def run_fresh(results, rounds):
    """Preserve the original reset-before-every-scenario experiment."""
    for record_size in cfg.RECORD_SIZES:
        for round_number in range(1, rounds + 1):
            for fs_type in cfg.FILESYSTEMS:
                for scenario_key in cfg.SCENARIO_KEYS:
                    control_kafka("stop")
                    setup_filesystem(fs_type)
                    measure_one(results, fs_type, record_size, scenario_key, round_number)


def run_occupancy(results, rounds):
    """Run an independent occupancy progression for each record size.

    Resetting when the record size changes prevents workloads with other record
    sizes from contaminating the 20 -> 40 -> 60 -> 80% aging curve.
    """
    for fs_type in cfg.FILESYSTEMS:
        for round_number in range(1, rounds + 1):
            for record_size in cfg.RECORD_SIZES:
                print(
                    f"\n{'#' * 70}\n# FILESYSTEM = {fs_type.upper()} / "
                    f"RECORD SIZE = {record_size}B / ROUND {round_number}\n{'#' * 70}"
                )
                control_kafka("stop")
                setup_filesystem(fs_type)
                for occupancy in cfg.OCCUPANCY_POINTS:
                    # Remove the preceding workload's topic before calculating
                    # the next prefill delta.  The prefill file remains, so the
                    # same record size ages progressively through all points.
                    control_kafka("start")
                    recreate_main_topic()
                    wait_for_filesystem_usage()
                    control_kafka("stop")
                    fill_filesystem_to(occupancy)
                    for scenario_key in cfg.SCENARIO_KEYS:
                        measure_one(
                            results, fs_type, record_size, scenario_key, round_number,
                            occupancy_target=occupancy, phase="occupancy",
                        )


def validate_long_configuration():
    if cfg.LONG_RECORD_SIZE not in cfg.RECORD_SIZES:
        raise ValueError("BENCH_LONG_RECORD_SIZE must be one of RECORD_SIZES")
    if cfg.LONG_SCENARIO not in cfg.SCENARIO_KEYS:
        raise ValueError("BENCH_LONG_SCENARIO must be enabled by the scenario group")


def run_endurance(results, rounds):
    """Cycle a bounded Kafka log above 80% permanently-live prefill data."""
    validate_long_configuration()
    occupancy = cfg.OCCUPANCY_POINTS[-1]
    for fs_type in cfg.FILESYSTEMS:
        for round_number in range(1, rounds + 1):
            print(
                f"\n{'#' * 70}\n# 80% ENDURANCE = {fs_type.upper()} / "
                f"{cfg.LONG_RECORD_SIZE}B / {occupancy}% / ROUND {round_number}\n{'#' * 70}"
            )
            control_kafka("stop")
            setup_filesystem(fs_type)
            control_kafka("start")
            recreate_main_topic()
            wait_for_filesystem_usage()
            control_kafka("stop")
            fill_filesystem_to(occupancy)
            measure_one(
                results, fs_type, cfg.LONG_RECORD_SIZE, cfg.LONG_SCENARIO,
                round_number, occupancy_target=occupancy, phase="endurance",
                measure_sec=cfg.LONG_DURATION_SECONDS,
                warmup_sec=cfg.LONG_WARMUP_SECONDS,
                retention_total_bytes=cfg.ENDURANCE_RETENTION_TOTAL_BYTES,
            )


def run_steady(results, rounds):
    """Let Kafka data itself reach retention equilibrium, without fio prefill."""
    validate_long_configuration()
    for fs_type in cfg.FILESYSTEMS:
        for round_number in range(1, rounds + 1):
            print(
                f"\n{'#' * 70}\n# KAFKA STEADY-STATE = {fs_type.upper()} / "
                f"{cfg.LONG_RECORD_SIZE}B / ROUND {round_number}\n{'#' * 70}"
            )
            control_kafka("stop")
            setup_filesystem(fs_type)
            measure_one(
                results, fs_type, cfg.LONG_RECORD_SIZE, cfg.LONG_SCENARIO,
                round_number, phase="steady",
                measure_sec=cfg.LONG_DURATION_SECONDS,
                warmup_sec=cfg.STEADY_WARMUP_SECONDS,
                retention_total_bytes=cfg.STEADY_RETENTION_TOTAL_BYTES,
            )


def run(argv=None):
    argv = sys.argv if argv is None else argv
    try:
        rounds = parse_arguments(argv)
    except (ValueError, TypeError) as exc:
        print(f"[CRITICAL ERROR] {exc}")
        return 2

    cfg.initialize_result_directories()
    print(f"[Config] Storage backend: {cfg.STORAGE_BACKEND}")
    if cfg.STORAGE_BACKEND == "dm-zns":
        print(f"[Config] DM implementation: {cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]}")
        print(f"[Config] DM logical capacity: at most {cfg.LOGICAL_CAPACITY_PERCENT}% of physical")
    print(f"[Config] Benchmark profile: {cfg.ACTIVE_PROFILE}")
    print(f"[Config] Scenario group: {cfg.ACTIVE_SCENARIO_GROUP}")
    print(f"[Config] Workload mode: {cfg.ACTIVE_WORKLOAD_MODE}")
    if cfg.ACTIVE_WORKLOAD_MODE != "fresh":
        print(f"[Config] Occupancy points: {cfg.OCCUPANCY_POINTS}%")
    validate_kafka_environment()
    prepare_experiment_kraft_config()
    capture_environment()
    results = init_result_structure()
    completed = False
    try:
        log_path = f"{cfg.ROUND_DIR}/{cfg.ACTIVE_WORKLOAD_MODE}_run.txt"
        logger, original_stdout = TeeLogger(log_path), sys.stdout
        sys.stdout = logger
        try:
            if cfg.ACTIVE_WORKLOAD_MODE == "fresh":
                run_fresh(results, rounds)
            elif cfg.ACTIVE_WORKLOAD_MODE == "occupancy":
                run_occupancy(results, rounds)
            elif cfg.ACTIVE_WORKLOAD_MODE == "endurance":
                run_endurance(results, rounds)
            else:
                run_steady(results, rounds)
        except Exception as exc:
            original_stdout.write(f"\n[CRITICAL ERROR] benchmark failed: {exc}\n")
            raise
        finally:
            sys.stdout = original_stdout
            logger.close()
            print(f"[Done] Run log saved: {log_path}")
        persist_results(results, rounds, final=True)
        completed = True
        return 0
    finally:
        control_kafka("stop")
        unmount_log_device(strict=False)
        if cfg.STORAGE_BACKEND == "dm-zns":
            remove_dm_stack(strict=False)
        run_cmd_quiet("sudo sync")
        if completed:
            print(f"\n{'=' * 80}\n[Finish] All {rounds} rounds completed.")
            print(f"Check '{cfg.RESULT_DIR}' for reports.\n{'=' * 80}")
        else:
            print("[Finish] Benchmark stopped before completion; partial results were retained.")

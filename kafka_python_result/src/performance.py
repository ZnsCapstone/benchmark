"""Benchmark execution, host monitoring, and performance metric parsing."""

import os
import re
import signal
import subprocess
import time
from copy import deepcopy

import settings as cfg
from bench_utils import gzip_file, run_cmd_streaming, safe_float, safe_mean, write_text


def start_monitors(prefix):
    paths = {"iostat": f"{prefix}_iostat.txt", "vmstat": f"{prefix}_vmstat.txt"}
    files = {name: open(path, "w", encoding="utf-8") for name, path in paths.items()}
    mapper_kernel_name = os.path.basename(os.path.realpath(cfg.FS_DEVICE))
    processes = {
        "iostat": subprocess.Popen(
            f"iostat -N -y -dxm 1 {cfg.RAW_DEVICE_BASENAME} {mapper_kernel_name}", shell=True,
            stdout=files["iostat"], stderr=subprocess.STDOUT, text=True,
            preexec_fn=os.setsid,
        ),
        "vmstat": subprocess.Popen(
            "vmstat 1", shell=True, stdout=files["vmstat"],
            stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid,
        ),
    }
    time.sleep(cfg.MONITOR_LEAD_SECONDS)
    return {"paths": paths, "files": files, "processes": processes}


def stop_monitors(monitors):
    for process in monitors["processes"].values():
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass
    time.sleep(1)
    for file in monitors["files"].values():
        file.close()


def parse_iostat_file(path, device_name, skip_samples=0, max_samples=None):
    if not os.path.exists(path):
        return {}
    rows, headers = [], None
    device_names = {device_name} if isinstance(device_name, str) else set(device_name)
    with open(path, "r", encoding="utf-8", errors="ignore") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("Device"):
                headers = re.split(r"\s+", line)
            elif headers and line.split(maxsplit=1)[0] in device_names:
                parts = re.split(r"\s+", line)
                if len(parts) == len(headers):
                    rows.append(dict(zip(headers, parts)))
    rows = rows[skip_samples:]
    if max_samples is not None:
        rows = rows[:max_samples]
    if not rows:
        return {}

    def columns(*names):
        for name in names:
            if name in rows[0]:
                return [safe_float(row.get(name)) for row in rows]
        return []

    util = columns("%util", "util")
    await_values = columns("await")
    read_await, write_await = columns("r_await"), columns("w_await")
    reads, writes = columns("r/s"), columns("w/s")
    if not await_values and (read_await or write_await):
        await_values = []
        for index in range(max(len(read_await), len(write_await))):
            r_await = read_await[index] if index < len(read_await) else 0.0
            w_await = write_await[index] if index < len(write_await) else 0.0
            rps = reads[index] if index < len(reads) else 0.0
            wps = writes[index] if index < len(writes) else 0.0
            if rps + wps > 0:
                await_values.append((r_await * rps + w_await * wps) / (rps + wps))
            else:
                nonzero = [value for value in (r_await, w_await) if value > 0]
                await_values.append(safe_mean(nonzero))

    read_kb, write_kb = columns("rMB/s"), columns("wMB/s")
    read_kb = [value * 1024 for value in read_kb] if read_kb else columns("rkB/s")
    write_kb = [value * 1024 for value in write_kb] if write_kb else columns("wkB/s")
    return {
        "samples": len(rows),
        "util_avg": safe_mean(util), "util_max": max(util) if util else 0.0,
        "await_avg": safe_mean(await_values),
        "await_max": max(await_values) if await_values else 0.0,
        "r_await_avg": safe_mean(read_await), "w_await_avg": safe_mean(write_await),
        "rkB_s_avg": safe_mean(read_kb), "wkB_s_avg": safe_mean(write_kb),
        "rps_avg": safe_mean(reads), "wps_avg": safe_mean(writes),
        "aqu_sz_avg": safe_mean(columns("aqu-sz", "avgqu-sz")),
        "rrqm_avg": safe_mean(columns("%rrqm", "%rrqm/s")),
        "wrqm_avg": safe_mean(columns("%wrqm", "%wrqm/s")),
    }


def parse_vmstat_file(path, skip_samples=0, max_samples=None):
    if not os.path.exists(path):
        return {}
    rows, header = [], None
    with open(path, "r", encoding="utf-8", errors="ignore") as file:
        for raw_line in file:
            line = raw_line.strip()
            if re.match(r"^r\s+b\s+swpd\s+", line):
                header = re.split(r"\s+", line)
            elif header and re.match(r"^\d+", line):
                parts = re.split(r"\s+", line)
                if len(parts) == len(header):
                    rows.append(dict(zip(header, parts)))
    rows = rows[skip_samples:]
    if max_samples is not None:
        rows = rows[:max_samples]
    if not rows:
        return {}

    def mean(name):
        return safe_mean([safe_float(row.get(name)) for row in rows if name in row])

    return {
        "samples": len(rows), "cpu_us_avg": mean("us"), "cpu_sy_avg": mean("sy"),
        "cpu_id_avg": mean("id"), "cpu_wa_avg": mean("wa"), "cpu_st_avg": mean("st"),
        "bi_avg": mean("bi"), "bo_avg": mean("bo"), "cs_avg": mean("cs"),
    }


def build_java_cmd(config):
    return (
        f"{cfg.JAVA_BENCH_CMD} --record-size {config['record_size']} "
        f"--target-ops {config['target_ops']} --producers {config['producers']} "
        f"--use-consumer {str(config['use_consumer']).lower()} "
        f"--dynamic-topics {str(config['dynamic_topics']).lower()} "
        f"--dynamic-topic-rate {config['dynamic_topic_rate']} "
        f"--warmup-sec {config['warmup_sec']} "
        f"--measure-sec {config['measure_sec']} "
        f"--drain-timeout-sec {config['drain_timeout_sec']} "
        f"--fail-fast-stall-sec {config['fail_fast_stall_sec']} "
        f"--bootstrap-servers {cfg.BOOTSTRAP} --topic {cfg.TOPIC_NAME} "
        f"--max-in-flight-records {config['max_in_flight_records']} "
        f"--max-catch-up-records {config['max_catch_up_records']} "
        f"--max-schedule-lag-ms {config['max_schedule_lag_ms']}"
    )


def parse_java_metrics(output):
    metrics = {
        "total_requests": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0,
        "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0, "achieved_ops": 0.0,
        "p95_ms": 0.0,
        "achieved_pct": 0.0, "total_sent": 0, "send_errors": 0,
        "drain_time_sec": 0.0, "drain_completed": False,
        "sent_requests": 0, "sent_ops": 0.0,
        "ack_window_requests": 0, "ack_window_ops": 0.0,
        "eventual_ack_requests": 0, "eventual_ack_ops": 0.0,
        "outstanding_at_end": 0, "failed_requests": 0,
        "unresolved_after_drain": 0, "latency_dropped_samples": 0,
        "backpressure_wait_count": 0, "backpressure_wait_ms": 0.0,
        "max_observed_outstanding": 0, "catch_up_resets": 0,
        "catch_up_records_skipped": 0, "max_schedule_lag_ms": 0.0,
        "ack_stall_count": 0, "ack_stall_total_sec": 0,
        "ack_stall_max_sec": 0,
        "consumer_records": -1, "producer_acked_records": -1,
        "consumer_record_delta": 0, "consumer_drain_completed": True,
        "malformed_headers": 0, "payload_crc_errors": 0,
        "sequence_gaps": 0, "duplicate_records": 0,
        "out_of_order_records": 0,
        "topics_created": 0, "topics_failed": 0,
        "final_topic_count": -1,
        "topic_first_failure_elapsed_ms": -1, "topic_first_failure": "none",
    }
    patterns = {
        "total_requests": (r"Total Requests\s*:\s*(\d+)", int),
        "avg_ms": (r"Average\s*:\s*([\d.]+)\s*ms", float),
        "p50_ms": (r"p50.*?:\s*([\d.]+)\s*ms", float),
        "p90_ms": (r"p90\s*:\s*([\d.]+)\s*ms", float),
        "p95_ms": (r"p95\s*:\s*([\d.]+)\s*ms", float),
        "p99_ms": (r"p99\s*:\s*([\d.]+)\s*ms", float),
        "p999_ms": (r"p999\s*:\s*([\d.]+)\s*ms", float),
        "max_ms": (r"Max\s*:\s*([\d.]+)\s*ms", float),
        "achieved_ops": (r"Achieved OP/s\s*:\s*([\d.]+)", float),
        "achieved_pct": (r"Achieved/Target.*?:\s*([\d.]+)", float),
        "total_sent": (r"Total Sent.*?:\s*(\d+)", int),
        "send_errors": (r"Send Errors\s*:\s*(\d+)", int),
        "drain_time_sec": (r"Drain Time\s*:\s*([\d.]+)\s*sec", float),
        "drain_completed": (
            r"Drain Completed\s*:\s*(true|false)",
            lambda value: value == "true",
        ),
        "sent_requests": (r"Sent Requests\s*:\s*(\d+)", int),
        "sent_ops": (r"Sent OP/s\s*:\s*([\d.]+)", float),
        "ack_window_requests": (r"ACK Window Requests\s*:\s*(\d+)", int),
        "ack_window_ops": (r"ACK Window OP/s\s*:\s*([\d.]+)", float),
        "eventual_ack_requests": (r"Eventual ACK Requests\s*:\s*(\d+)", int),
        "eventual_ack_ops": (r"Eventual ACK OP/s\s*:\s*([\d.]+)", float),
        "outstanding_at_end": (r"Outstanding at End\s*:\s*(\d+)", int),
        "failed_requests": (r"Failed Requests\s*:\s*(\d+)", int),
        "unresolved_after_drain": (r"Unresolved After Drain\s*:\s*(\d+)", int),
        "latency_dropped_samples": (r"Latency Dropped Samples\s*:\s*(\d+)", int),
        "backpressure_wait_count": (r"Backpressure Wait Count\s*:\s*(\d+)", int),
        "backpressure_wait_ms": (r"Backpressure Wait Time\s*:\s*([\d.]+)\s*ms", float),
        "max_observed_outstanding": (r"Max Observed Outstanding\s*:\s*(\d+)", int),
        "catch_up_resets": (r"Catch-up Resets\s*:\s*(\d+)", int),
        "catch_up_records_skipped": (r"Catch-up Records Skipped\s*:\s*(\d+)", int),
        "max_schedule_lag_ms": (r"Max Schedule Lag\s*:\s*([\d.]+)\s*ms", float),
        "ack_stall_count": (r"ACK Stall Count\s*:\s*(\d+)", int),
        "ack_stall_total_sec": (r"ACK Stall Total\s*:\s*(\d+)\s*sec", int),
        "ack_stall_max_sec": (r"ACK Stall Max\s*:\s*(\d+)\s*sec", int),
        "consumer_records": (r"Consumer Records\s*:\s*(\d+)", int),
        "producer_acked_records": (r"Producer ACKed Records\s*:\s*(\d+)", int),
        "consumer_record_delta": (r"Consumer Record Delta\s*:\s*(-?\d+)", int),
        "consumer_drain_completed": (
            r"Consumer Drain Completed\s*:\s*(true|false)",
            lambda value: value == "true",
        ),
        "malformed_headers": (r"Malformed Headers\s*:\s*(\d+)", int),
        "payload_crc_errors": (r"Payload CRC Errors\s*:\s*(\d+)", int),
        "sequence_gaps": (r"Sequence Gaps\s*:\s*(\d+)", int),
        "duplicate_records": (r"Duplicate Records\s*:\s*(\d+)", int),
        "out_of_order_records": (r"Out-of-order Records\s*:\s*(\d+)", int),
        "topics_created": (r"\[TopicCreator\] created=(\d+)", int),
        "topics_failed": (r"\[TopicCreator\] created=\d+ failed=(\d+)", int),
        "topic_first_failure_elapsed_ms": (
            r"\[TopicCreator\] first_failure_elapsed_ms=(-?\d+)", int,
        ),
    }
    for key, (pattern, converter) in patterns.items():
        match = re.search(pattern, output)
        if match:
            metrics[key] = converter(match.group(1))
    failure_match = re.search(
        r"\[TopicCreator\] first_failure_elapsed_ms=-?\d+ first_failure=(.*)", output
    )
    if failure_match:
        metrics["topic_first_failure"] = failure_match.group(1).strip()
    if metrics["total_requests"] == 0:
        print("[WARN] total_requests=0 — Java 출력 파싱 실패 가능성. raw output 확인 필요.")
    return metrics


def evaluate_run(metrics, return_code):
    """Classify validity first, then describe the dominant runtime state."""
    invalid_reasons = []
    if return_code != 0:
        invalid_reasons.append(f"java_exit_code={return_code}")
    if metrics.get("eventual_ack_requests", 0) == 0:
        invalid_reasons.append("eventual_ack_requests=0")
    if metrics.get("failed_requests", 0) > 0:
        invalid_reasons.append(f"failed_requests={metrics['failed_requests']}")
    if metrics.get("send_errors", 0) > 0:
        invalid_reasons.append(f"send_errors={metrics['send_errors']}")
    if metrics.get("topics_failed", 0) > 0:
        invalid_reasons.append(f"topics_failed={metrics['topics_failed']}")
    if not metrics.get("drain_completed", False):
        invalid_reasons.append("drain_completed=false")
    if metrics.get("unresolved_after_drain", 0) > 0:
        invalid_reasons.append(
            f"unresolved_after_drain={metrics['unresolved_after_drain']}"
        )
    if metrics.get("latency_dropped_samples", 0) > 0:
        invalid_reasons.append(
            f"latency_dropped_samples={metrics['latency_dropped_samples']}"
        )
    if metrics.get("consumer_records", -1) >= 0:
        integrity_fields = (
            "malformed_headers", "payload_crc_errors", "sequence_gaps",
            "duplicate_records", "out_of_order_records",
        )
        if not metrics.get("consumer_drain_completed", False):
            invalid_reasons.append("consumer_drain_completed=false")
        if metrics.get("consumer_record_delta", 0) != 0:
            invalid_reasons.append(
                f"consumer_record_delta={metrics['consumer_record_delta']}"
            )
        for key in integrity_fields:
            if metrics.get(key, 0) > 0:
                invalid_reasons.append(f"{key}={metrics[key]}")

    valid = not invalid_reasons
    if not valid:
        integrity_failed = any(
            metrics.get(key, 0) > 0 for key in (
                "malformed_headers", "payload_crc_errors", "sequence_gaps",
                "duplicate_records", "out_of_order_records",
            )
        ) or metrics.get("consumer_record_delta", 0) != 0
        failed = (
            return_code != 0
            or metrics.get("eventual_ack_requests", 0) == 0
            or metrics.get("failed_requests", 0) > 0
            or metrics.get("send_errors", 0) > 0
            or metrics.get("topics_failed", 0) > 0
            or integrity_failed
        )
        state = "FAILED" if failed else "INCOMPLETE"
    else:
        rules = cfg.BOTTLENECK_RULES
        util = metrics.get("mapper_util_avg", metrics.get("raw_util_avg", 0.0))
        await_avg = metrics.get("mapper_await_avg", metrics.get("raw_await_avg", 0.0))
        iowait = metrics.get("cpu_wa_avg", 0.0)
        saturated = util >= rules["min_disk_util_pct"] and (
            await_avg >= rules["min_await_ms"]
            or iowait >= rules["min_cpu_iowait_pct"]
        )
        backpressured = (
            metrics.get("outstanding_at_end", 0) > 0
            or metrics.get("backpressure_wait_count", 0) > 0
            or metrics.get("ack_window_ops", 0.0)
            < metrics.get("sent_ops", 0.0) * 0.98
        )
        if metrics.get("ack_stall_max_sec", 0) > cfg.MAX_ACCEPTABLE_ACK_STALL_SECONDS:
            state = "STALLED"
        else:
            state = "SATURATED" if saturated else (
                "BACKPRESSURED" if backpressured else "NOT_SATURATED"
            )
    return {"valid": valid, "state": state, "invalid_reasons": invalid_reasons}


def detect_bottleneck(metrics):
    util, iowait, await_avg = (
        metrics.get("mapper_util_avg", metrics.get("raw_util_avg", 0.0)),
        metrics.get("cpu_wa_avg", 0.0),
        metrics.get("mapper_await_avg", metrics.get("raw_await_avg", 0.0)),
    )
    rules = cfg.BOTTLENECK_RULES
    util_hit = util >= rules["min_disk_util_pct"]
    iowait_hit = iowait >= rules["min_cpu_iowait_pct"]
    await_hit = await_avg >= rules["min_await_ms"]
    reasons = []
    if util_hit:
        reasons.append(f"disk util avg {util:.2f}% >= {rules['min_disk_util_pct']:.2f}%")
    if iowait_hit:
        reasons.append(f"cpu iowait avg {iowait:.2f}% >= {rules['min_cpu_iowait_pct']:.2f}%")
    if await_hit:
        reasons.append(f"disk await avg {await_avg:.2f} ms >= {rules['min_await_ms']:.2f} ms")
    return {"is_bottleneck": util_hit and (await_hit or iowait_hit), "reasons": reasons}


def run_benchmark_once(fs_type, scenario_key, config, round_idx, phase_tag="measure"):
    print(
        f"\n--- [{phase_tag.upper()}] {fs_type.upper()} / {scenario_key} / "
        f"{config['record_size']}B / {config['target_ops']} ops ---"
    )
    prefix = (
        f"{cfg.MONITOR_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}"
    )
    monitors = start_monitors(prefix)
    try:
        output, return_code = run_cmd_streaming(build_java_cmd(config))
    finally:
        stop_monitors(monitors)
    skip = config.get("warmup_sec", 0) + cfg.MONITOR_LEAD_SECONDS
    metrics = parse_java_metrics(output)
    measure_samples = config["measure_sec"]
    mapper_kernel_name = os.path.basename(os.path.realpath(cfg.FS_DEVICE))
    for prefix_name, device_name in (
        ("raw", cfg.RAW_DEVICE_BASENAME),
        ("mapper", (cfg.DM_NAME, mapper_kernel_name)),
    ):
        device_metrics = parse_iostat_file(
            monitors["paths"]["iostat"], device_name, skip,
            max_samples=measure_samples,
        )
        metrics.update({f"{prefix_name}_{key}": value
                        for key, value in device_metrics.items()})
    metrics.update(parse_vmstat_file(
        monitors["paths"]["vmstat"], skip, max_samples=measure_samples
    ))
    bottleneck = detect_bottleneck(metrics)
    validity = evaluate_run(metrics, return_code)
    raw_path = (
        f"{cfg.RAW_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}.txt"
    )
    write_text(raw_path, output)
    print(
        f"  > Req={metrics.get('total_requests', 0)} | "
        f"Sent/ACKwin/Eventual={metrics.get('sent_ops', 0.0):.1f}/"
        f"{metrics.get('ack_window_ops', 0.0):.1f}/"
        f"{metrics.get('eventual_ack_ops', 0.0):.1f} OP/s | "
        f"App-Avg={metrics.get('avg_ms', 0.0):.2f}ms | "
        f"App-P99={metrics.get('p99_ms', 0.0):.2f}ms | "
        f"MapperUtil={metrics.get('mapper_util_avg', 0.0):.2f}% | "
        f"RawUtil={metrics.get('raw_util_avg', 0.0):.2f}% | "
        f"iowait={metrics.get('cpu_wa_avg', 0.0):.2f}% | "
        f"State={validity['state']} | Valid={validity['valid']}"
    )
    paths = monitors["paths"]
    if cfg.COMPRESS_AFTER_RUN:
        raw_path = gzip_file(raw_path)
        paths = {name: gzip_file(path) for name, path in paths.items()}
    return {
        "config": deepcopy(config), "metrics": metrics, "bottleneck": bottleneck,
        "validity": validity, "java_exit_code": return_code,
        "raw_output_path": raw_path, "monitor_files": paths,
    }

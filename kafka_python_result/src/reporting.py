"""JSON, CSV, and human-readable benchmark result output."""

import csv
import datetime
import json

import settings as cfg
from bench_utils import safe_cv, safe_mean, safe_stdev, write_text


def flatten_result_rows(all_results):
    rows = []
    for fs_type, fs_payload in all_results.items():
        for record_size, size_payload in fs_payload.items():
            for scenario, scenario_payload in size_payload.items():
                for entry in scenario_payload["rounds"]:
                    metrics, bottleneck = entry["metrics"], entry["bottleneck"]
                    validity = entry.get("validity", {})
                    rows.append({
                        "filesystem": fs_type, "record_size": record_size,
                        "scenario": scenario, "round": entry["round"],
                        "profile": entry["config"].get("profile", "unknown"),
                        "phase": entry["config"].get("phase", "measure"),
                        "occupancy_target_pct": entry["config"].get("occupancy_target_pct"),
                        "fs_used_pct_before": entry["config"].get("fs_usage_before", {}).get("used_percent"),
                        "fs_used_pct_after": entry["config"].get("fs_usage_after", {}).get("used_percent"),
                        "valid": validity.get("valid", False),
                        "run_state": validity.get("state", "INCOMPLETE"),
                        "invalid_reasons": " | ".join(validity.get("invalid_reasons", [])),
                        "java_exit_code": entry.get("java_exit_code", -1),
                        "target_ops": entry["config"]["target_ops"],
                        "measure_sec": entry["config"]["measure_sec"],
                        "drain_timeout_sec": entry["config"]["drain_timeout_sec"],
                        "warmup_sec": entry["config"]["warmup_sec"],
                        "producers": entry["config"]["producers"],
                        "use_consumer": entry["config"]["use_consumer"],
                        "dynamic_topics": entry["config"]["dynamic_topics"],
                        "dynamic_topic_rate": entry["config"]["dynamic_topic_rate"],
                        "max_in_flight_records": entry["config"].get("max_in_flight_records", 0),
                        "max_catch_up_records": entry["config"].get("max_catch_up_records", 0),
                        "max_schedule_lag_limit_ms": entry["config"].get("max_schedule_lag_ms", 0),
                        "sent_requests": metrics.get("sent_requests", 0),
                        "sent_ops": metrics.get("sent_ops", 0.0),
                        "ack_window_requests": metrics.get("ack_window_requests", 0),
                        "ack_window_ops": metrics.get("ack_window_ops", 0.0),
                        "eventual_ack_requests": metrics.get("eventual_ack_requests", 0),
                        "eventual_ack_ops": metrics.get("eventual_ack_ops", 0.0),
                        "outstanding_at_end": metrics.get("outstanding_at_end", 0),
                        "failed_requests": metrics.get("failed_requests", 0),
                        "unresolved_after_drain": metrics.get("unresolved_after_drain", 0),
                        "latency_dropped_samples": metrics.get("latency_dropped_samples", 0),
                        "backpressure_wait_count": metrics.get("backpressure_wait_count", 0),
                        "backpressure_wait_ms": metrics.get("backpressure_wait_ms", 0.0),
                        "max_observed_outstanding": metrics.get("max_observed_outstanding", 0),
                        "catch_up_resets": metrics.get("catch_up_resets", 0),
                        "catch_up_records_skipped": metrics.get("catch_up_records_skipped", 0),
                        "max_schedule_lag_ms": metrics.get("max_schedule_lag_ms", 0.0),
                        "ack_stall_count": metrics.get("ack_stall_count", 0),
                        "ack_stall_total_sec": metrics.get("ack_stall_total_sec", 0),
                        "ack_stall_max_sec": metrics.get("ack_stall_max_sec", 0),
                        "consumer_records": metrics.get("consumer_records", -1),
                        "producer_acked_records": metrics.get("producer_acked_records", -1),
                        "consumer_record_delta": metrics.get("consumer_record_delta", 0),
                        "consumer_drain_completed": metrics.get("consumer_drain_completed", True),
                        "malformed_headers": metrics.get("malformed_headers", 0),
                        "payload_crc_errors": metrics.get("payload_crc_errors", 0),
                        "sequence_gaps": metrics.get("sequence_gaps", 0),
                        "duplicate_records": metrics.get("duplicate_records", 0),
                        "out_of_order_records": metrics.get("out_of_order_records", 0),
                        "total_requests": metrics.get("total_requests", 0),
                        "achieved_ops": metrics.get("achieved_ops", 0.0),
                        "achieved_pct": metrics.get("achieved_pct", 0.0),
                        "total_sent": metrics.get("total_sent", 0),
                        "send_errors": metrics.get("send_errors", 0),
                        "drain_time_sec": metrics.get("drain_time_sec", 0.0),
                        "drain_completed": metrics.get("drain_completed", False),
                        "app_avg_ms": metrics.get("avg_ms", 0.0),
                        "app_p50_ms": metrics.get("p50_ms", 0.0),
                        "app_p90_ms": metrics.get("p90_ms", 0.0),
                        "app_p95_ms": metrics.get("p95_ms", 0.0),
                        "app_p99_ms": metrics.get("p99_ms", 0.0),
                        "app_p999_ms": metrics.get("p999_ms", 0.0),
                        "app_max_ms": metrics.get("max_ms", 0.0),
                        "mapper_util_avg": metrics.get("mapper_util_avg", 0.0),
                        "mapper_await_avg": metrics.get("mapper_await_avg", 0.0),
                        "mapper_wkB_s_avg": metrics.get("mapper_wkB_s_avg", 0.0),
                        "raw_util_avg": metrics.get("raw_util_avg", 0.0),
                        "raw_await_avg": metrics.get("raw_await_avg", 0.0),
                        "raw_wkB_s_avg": metrics.get("raw_wkB_s_avg", 0.0),
                        "cpu_us_avg": metrics.get("cpu_us_avg", 0.0),
                        "cpu_sy_avg": metrics.get("cpu_sy_avg", 0.0),
                        "cpu_id_avg": metrics.get("cpu_id_avg", 0.0),
                        "cpu_wa_avg": metrics.get("cpu_wa_avg", 0.0),
                        "bottleneck": bottleneck.get("is_bottleneck", False),
                        "bottleneck_reasons": " | ".join(bottleneck.get("reasons", [])),
                        "topics_created": metrics.get("topics_created", 0),
                        "topics_failed": metrics.get("topics_failed", 0),
                        "final_topic_count": metrics.get("final_topic_count", -1),
                        "topic_first_failure_elapsed_ms": metrics.get("topic_first_failure_elapsed_ms", -1),
                        "topic_first_failure": metrics.get("topic_first_failure", "none"),
                        "raw_output_path": entry["raw_output_path"],
                        "iostat_path": entry["monitor_files"]["iostat"],
                        "vmstat_path": entry["monitor_files"]["vmstat"],
                    })
    return rows


def save_csv_reports(all_results):
    rows = flatten_result_rows(all_results)
    full_path = f"{cfg.CSV_DIR}/full_results.csv"
    if rows:
        with open(full_path, "w", newline="", encoding="utf-8") as file:
            fieldnames = list(dict.fromkeys(key for row in rows for key in row))
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    grouped = {}
    for row in rows:
        grouped.setdefault(
            (row["profile"], row["phase"], row["occupancy_target_pct"], row["filesystem"], row["record_size"], row["scenario"]), []
        ).append(row)
    summaries = []
    for (profile, phase, occupancy, fs_type, size, scenario), all_items in grouped.items():
        items = [item for item in all_items if item["valid"]]
        if not items:
            summaries.append({
                "profile": profile, "phase": phase, "occupancy_target_pct": occupancy,
                "filesystem": fs_type, "record_size": size,
                "scenario": scenario, "total_rounds": len(all_items), "valid_rounds": 0,
                "invalid_rounds": len(all_items), "run_states": " | ".join(
                    item["run_state"] for item in all_items
                ),
            })
            continue
        values = lambda key: [item[key] for item in items]
        summaries.append({
            "profile": profile, "phase": phase, "occupancy_target_pct": occupancy,
            "filesystem": fs_type, "record_size": size,
            "scenario": scenario, "total_rounds": len(all_items),
            "valid_rounds": len(items), "invalid_rounds": len(all_items) - len(items),
            "run_states": " | ".join(item["run_state"] for item in all_items),
            "target_ops_mean": safe_mean(values("target_ops")),
            "sent_ops_mean": safe_mean(values("sent_ops")),
            "ack_window_ops_mean": safe_mean(values("ack_window_ops")),
            "eventual_ack_ops_mean": safe_mean(values("eventual_ack_ops")),
            "outstanding_at_end_mean": safe_mean(values("outstanding_at_end")),
            "drain_time_sec_mean": safe_mean(values("drain_time_sec")),
            "drain_completed_rounds": sum(item["drain_completed"] for item in items),
            "requests_mean": safe_mean(values("total_requests")),
            "app_avg_mean": safe_mean(values("app_avg_ms")),
            "app_avg_stdev": safe_stdev(values("app_avg_ms")),
            "app_avg_cv_pct": safe_cv(values("app_avg_ms")),
            "app_p50_mean": safe_mean(values("app_p50_ms")),
            "app_p90_mean": safe_mean(values("app_p90_ms")),
            "app_p95_mean": safe_mean(values("app_p95_ms")),
            "app_p99_mean": safe_mean(values("app_p99_ms")),
            "app_p99_stdev": safe_stdev(values("app_p99_ms")),
            "app_p99_cv_pct": safe_cv(values("app_p99_ms")),
            "app_max_mean": safe_mean(values("app_max_ms")),
            "mapper_util_mean": safe_mean(values("mapper_util_avg")),
            "mapper_await_mean": safe_mean(values("mapper_await_avg")),
            "raw_util_mean": safe_mean(values("raw_util_avg")),
            "raw_await_mean": safe_mean(values("raw_await_avg")),
            "cpu_iowait_mean": safe_mean(values("cpu_wa_avg")),
            "bottleneck_rounds": sum(item["bottleneck"] for item in items),
        })
    summary_path = f"{cfg.CSV_DIR}/summary_by_fs_recordsize_scenario.csv"
    if summaries:
        with open(summary_path, "w", newline="", encoding="utf-8") as file:
            fieldnames = list(dict.fromkeys(key for row in summaries for key in row))
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summaries)
    print(f"[Success] CSV saved: {full_path}")
    print(f"[Success] CSV saved: {summary_path}")


def save_json_snapshot(all_results):
    path = f"{cfg.RESULT_DIR}/results_snapshot.json"
    with open(path, "w", encoding="utf-8") as file:
        json.dump(all_results, file, indent=2, ensure_ascii=False)
    print(f"[Success] JSON snapshot saved: {path}")


def save_summary_report(all_results, current_round, final=False):
    lines = [
        "============================================================",
        "  KAFKA FILESYSTEM PERFORMANCE EXPERIMENT REPORT (M1-#1)",
        "============================================================",
        f"Start Time : {cfg.PROGRAM_START_TIME}",
        f"Current Round Completed : {current_round}",
    ]
    if final:
        lines.append(f"End Time   : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    lines.extend([
        "", "Latency Layers",
        "  - App-level   : producer.send → broker ACK (Kafka client latency)",
        "  - Block-level : iostat await (block queue entry → completion)", "",
        "Experiment Scope",
        "- Kafka 4.2 (KRaft single broker, replication=1)",
        f"- metadata.log.dir separated: {cfg.SEPARATE_METADATA_DIR}",
        f"- Filesystems: {', '.join(cfg.FILESYSTEMS)} on {cfg.FS_DEVICE}",
        f"- DM implementation: {cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]}",
        f"- Benchmark profile: {cfg.ACTIVE_PROFILE}",
        f"- Scenario group: {cfg.ACTIVE_SCENARIO_GROUP}",
        f"- Raw ZNS: {cfg.RAW_ZNS_DEVICE}",
        f"- Record sizes: {cfg.RECORD_SIZES}",
        f"- Dynamic topic rate (/sec): {cfg.TOPIC_RATE}",
        f"- Measure: {cfg.MEASURE_DURATION}s (warmup {cfg.WARMUP_SECONDS}s)",
        f"- Drain timeout: {cfg.DRAIN_TIMEOUT_SECONDS}s",
        f"- Fail-fast ACK stall: {cfg.FAIL_FAST_STALL_SECONDS}s (0 disables)",
        f"- FIXED_OPS_BY_RECORD_SIZE: {cfg.FIXED_OPS_BY_RECORD_SIZE}", "",
    ])
    for fs_type in cfg.FILESYSTEMS:
        lines.append(f"################ FILESYSTEM: {fs_type.upper()} ################")
        for size in cfg.RECORD_SIZES:
            lines.append(f"\n[Record Size: {size} Bytes]")
            for scenario in cfg.SCENARIO_KEYS:
                rounds = all_results.get(fs_type, {}).get(size, {}).get(scenario, {}).get("rounds", [])
                if not rounds:
                    lines.append(f"  - {scenario}: no data")
                    continue
                valid_rounds = [entry for entry in rounds
                                if entry.get("validity", {}).get("valid", False)]
                states = [entry.get("validity", {}).get("state", "INCOMPLETE")
                          for entry in rounds]
                if not valid_rounds:
                    lines.append(
                        f"  [{scenario}] valid: 0/{len(rounds)}; states: {', '.join(states)}"
                    )
                    continue
                metric = lambda key: [entry["metrics"].get(key, 0.0)
                                      for entry in valid_rounds]
                lines.extend([
                    f"  [{scenario}] valid: {len(valid_rounds)}/{len(rounds)}; states: {', '.join(states)}",
                    f"    target/sent ops     : {safe_mean([e['config']['target_ops'] for e in valid_rounds]):.2f} / {safe_mean(metric('sent_ops')):.2f}",
                    f"    ACK window/eventual : {safe_mean(metric('ack_window_ops')):.2f} / {safe_mean(metric('eventual_ack_ops')):.2f} OP/s",
                    f"    outstanding at end : {safe_mean(metric('outstanding_at_end')):.2f}",
                    f"    drain time/completed: {safe_mean(metric('drain_time_sec')):.2f} sec / {sum(metric('drain_completed'))}/{len(valid_rounds)}",
                    f"    app avg / stdev     : {safe_mean(metric('avg_ms')):.2f} / {safe_stdev(metric('avg_ms')):.2f} ms (CV={safe_cv(metric('avg_ms')):.1f}%)",
                    f"    app p50 / p90       : {safe_mean(metric('p50_ms')):.2f} / {safe_mean(metric('p90_ms')):.2f} ms",
                    f"    app p99 / stdev     : {safe_mean(metric('p99_ms')):.2f} / {safe_stdev(metric('p99_ms')):.2f} ms (CV={safe_cv(metric('p99_ms')):.1f}%)",
                    f"    app max             : {safe_mean(metric('max_ms')):.2f} ms",
                    f"    mapper util / await : {safe_mean(metric('mapper_util_avg')):.2f} % / {safe_mean(metric('mapper_await_avg')):.2f} ms",
                    f"    raw util / await    : {safe_mean(metric('raw_util_avg')):.2f} % / {safe_mean(metric('raw_await_avg')):.2f} ms",
                    f"    cpu iowait          : {safe_mean(metric('cpu_wa_avg')):.2f} %",
                ])
        lines.append("")
    path = f"{cfg.RESULT_DIR}/summary_report.txt"
    write_text(path, "\n".join(lines))
    print(f"[Success] Summary report saved: {path}")

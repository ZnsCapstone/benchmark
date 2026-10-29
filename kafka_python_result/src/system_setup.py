"""FEMU ZNS, filesystem, Kafka KRaft, and reproducibility setup."""

import datetime
import os
import subprocess
import sys
import time

import settings as cfg
from bench_utils import run_cmd_full, run_cmd_quiet, wait_for_port, write_text


def capture_environment():
    sections = [
        ("Date", datetime.datetime.now().isoformat()),
        ("uname -a", run_cmd_quiet("uname -a")),
        ("/etc/os-release", run_cmd_quiet("cat /etc/os-release")),
        ("CPU info (lscpu)", run_cmd_quiet("lscpu")),
        ("Memory (free -h)", run_cmd_quiet("free -h")),
        ("Disk (lsblk)", run_cmd_quiet("lsblk")),
        ("Disk (df -h)", run_cmd_quiet("df -h")),
        ("Kernel cmdline", run_cmd_quiet("cat /proc/cmdline")),
        ("NVMe I/O timeout", run_cmd_quiet(
            "cat /sys/module/nvme_core/parameters/io_timeout"
        )),
        ("FEMU commit (launcher)", os.environ.get("BENCH_FEMU_COMMIT", "unknown")),
        ("FEMU binary SHA-256 (launcher)", os.environ.get(
            "BENCH_FEMU_BINARY_SHA256", "unknown"
        )),
        ("vm.dirty_ratio", run_cmd_quiet("sysctl vm.dirty_ratio")),
        ("vm.dirty_background_ratio", run_cmd_quiet("sysctl vm.dirty_background_ratio")),
        ("vm.swappiness", run_cmd_quiet("sysctl vm.swappiness")),
        ("iostat version", run_cmd_quiet("iostat -V | head -1")),
        ("vmstat version", run_cmd_quiet("vmstat -V")),
        ("mkfs.ext4 version", run_cmd_quiet("mkfs.ext4 -V 2>&1 | head -1")),
        ("mkfs.f2fs version", run_cmd_quiet("mkfs.f2fs -V 2>&1 | head -1")),
        ("Java version", run_cmd_quiet("java -version 2>&1")),
        ("Java benchmark JAR", cfg.JAVA_BENCH_JAR),
        ("Java benchmark SHA-256", run_cmd_quiet(f"sha256sum {cfg.JAVA_BENCH_JAR}")),
        ("Benchmark profile", cfg.ACTIVE_PROFILE),
        ("Profile config", str(cfg.active_profile_config())),
        ("Scenario group", cfg.ACTIVE_SCENARIO_GROUP),
        ("Workload mode", cfg.ACTIVE_WORKLOAD_MODE),
        ("Occupancy points", str(cfg.OCCUPANCY_POINTS)),
        ("Kafka path", cfg.KAFKA_PATH),
        ("Kafka config", run_cmd_quiet(f"cat {cfg.EXPERIMENT_KRAFT_CONFIG} | head -50")),
        ("Storage backend", cfg.STORAGE_BACKEND),
        ("Raw FEMU device", cfg.RAW_ZNS_DEVICE),
        ("Filesystem device", cfg.FS_DEVICE),
    ]
    if cfg.STORAGE_BACKEND == "dm-zns":
        sections.extend([
            ("DM module", cfg.DM_MODULE_PATH),
            ("DM implementation", cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]),
            ("DM logical capacity percent", str(cfg.LOGICAL_CAPACITY_PERCENT)),
            ("DM GC reserved zones", str(cfg.GC_RESERVE_ZONES)),
            ("DM GC low watermark", str(cfg.GC_LOW_WATERMARK)),
            ("DM GC high watermark", str(cfg.GC_HIGH_WATERMARK)),
            ("DM table", run_cmd_quiet(
                f"sudo dmsetup table {cfg.DM_NAME} 2>&1 || true"
            )),
        ])
    else:
        sections.extend([
            ("CNS expected model", cfg.CNS_EXPECTED_MODEL),
            ("CNS expected sectors", str(cfg.CNS_EXPECTED_SECTORS)),
            ("CNS actual model", run_cmd_quiet(
                f"cat /sys/block/{cfg.RAW_DEVICE_BASENAME}/device/model"
            )),
            ("CNS actual sectors", run_cmd_quiet(
                f"cat /sys/block/{cfg.RAW_DEVICE_BASENAME}/size"
            )),
        ])
    content = "\n".join(f"### {name}\n{value}\n" for name, value in sections)
    path = f"{cfg.ENV_DIR}/system_info.txt"
    write_text(path, content)
    print(f"[Env] System info saved: {path}")


def dm_module_name():
    return os.path.basename(cfg.DM_MODULE_PATH).removesuffix(".ko").replace("-", "_")


def unmount_log_device(strict=True):
    last_result = None
    for attempt in range(1, 6):
        result = subprocess.run(
            f"sudo umount {cfg.MOUNT_POINT}", shell=True, capture_output=True, text=True
        )
        if result.returncode == 0 or "not mounted" in result.stderr.lower():
            return
        last_result = result
        if attempt < 5:
            print(f"[Setup] Mount still busy; retrying unmount ({attempt}/5) ...")
            run_cmd_quiet("sudo sync")
            time.sleep(2)
    busy = run_cmd_full(f"sudo fuser -vm {cfg.MOUNT_POINT} 2>&1 || true")
    message = f"umount failed: {last_result.stderr.strip()}\n{busy.stdout.strip()}"
    if strict:
        raise RuntimeError(message)
    print(f"[Warn] {message}")


def remove_dm_stack(strict=True):
    result = run_cmd_full(f"sudo dmsetup remove {cfg.DM_NAME}")
    if result.returncode != 0 and not any(
        text in result.stderr.lower() for text in ("no such device", "not found")
    ):
        message = f"dmsetup remove failed:\n{result.stdout}\n{result.stderr}"
        if strict:
            raise RuntimeError(message)
        print(f"[Warn] {message}")

    result = run_cmd_full(f"sudo rmmod {dm_module_name()}")
    if result.returncode != 0 and not any(
        text in result.stderr.lower() for text in ("not currently loaded", "no such file")
    ):
        message = f"rmmod failed:\n{result.stdout}\n{result.stderr}"
        if strict:
            raise RuntimeError(message)
        print(f"[Warn] {message}")


def validate_cns_device():
    """Fail closed before destroying the conventional FEMU namespace."""
    if cfg.FS_DEVICE != cfg.RAW_ZNS_DEVICE or cfg.RAW_ZNS_DEVICE != "/dev/nvme0n1":
        raise RuntimeError("CNS backend is restricted to /dev/nvme0n1")
    sysfs = f"/sys/block/{cfg.RAW_DEVICE_BASENAME}"
    try:
        with open(f"{sysfs}/queue/zoned", encoding="utf-8") as file:
            zoned = file.read().strip()
        with open(f"{sysfs}/device/model", encoding="utf-8") as file:
            model = file.read().strip()
        with open(f"{sysfs}/size", encoding="utf-8") as file:
            sectors = int(file.read().strip())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot identify CNS device {cfg.RAW_ZNS_DEVICE}: {exc}") from exc
    if zoned != "none":
        raise RuntimeError(f"CNS device must be non-zoned (zoned={zoned!r})")
    if model != cfg.CNS_EXPECTED_MODEL:
        raise RuntimeError(
            f"unexpected CNS model {model!r}; expected {cfg.CNS_EXPECTED_MODEL!r}"
        )
    if sectors != cfg.CNS_EXPECTED_SECTORS:
        raise RuntimeError(
            f"unexpected CNS size {sectors} sectors; expected {cfg.CNS_EXPECTED_SECTORS}"
        )
    print(
        f"[CNS] Verified {cfg.RAW_ZNS_DEVICE}: {model}, "
        f"{sectors * 512 / 1024**3:.2f} GiB, zoned={zoned}"
    )

def zns_logical_sectors():
    zoned = run_cmd_quiet(f"cat /sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/zoned")
    if zoned != "host-managed":
        raise RuntimeError(
            f"{cfg.RAW_ZNS_DEVICE} is not a host-managed ZNS device (zoned={zoned!r})"
        )
    try:
        with open(
            f"/sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/chunk_sectors", encoding="utf-8"
        ) as file:
            zone_sectors = int(file.read().strip())
        with open(
            f"/sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/nr_zones", encoding="utf-8"
        ) as file:
            nr_zones = int(file.read().strip())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read ZNS geometry for {cfg.RAW_ZNS_DEVICE}: {exc}") from exc

    if not 1 <= cfg.LOGICAL_CAPACITY_PERCENT < 100:
        raise RuntimeError("LOGICAL_CAPACITY_PERCENT must be between 1 and 99")

    if cfg.DM_IMPLEMENTATION == "dynamic":
        reserved = cfg.GC_RESERVE_ZONES
    else:
        reserved = cfg.METADATA_ZONES + cfg.GC_RESERVE_ZONES
    if nr_zones <= reserved:
        raise RuntimeError(
            f"ZNS has {nr_zones} zones, but {cfg.DM_IMPLEMENTATION} dm-zns-base "
            f"needs {reserved} reserved zones"
        )
    # 논리 주소 범위는 zone 경계에 맞추고, 구현별 고정 reserve와 비율 기반
    # over-provisioning 중 더 보수적인 쪽을 사용한다. allocator는 underlying의
    # 모든 zone을 계속 관리하므로 줄어든 부분은 GC/WAL/SSTable 물리 여유다.
    percent_zones = nr_zones * cfg.LOGICAL_CAPACITY_PERCENT // 100
    logical_zones = min(nr_zones - reserved, percent_zones)
    if logical_zones < 1:
        raise RuntimeError("logical capacity leaves no host-visible zone")
    return logical_zones * zone_sectors


def create_dm_target():
    if not os.path.exists(cfg.DM_MODULE_PATH):
        raise RuntimeError(
            f"dm-zns-base module not found: {cfg.DM_MODULE_PATH}\n"
            "Build dm-zns-base.c into dm-zns-base.ko first, or set DM_ZNS_MODULE_PATH."
        )
    if cfg.GC_LOW_WATERMARK <= cfg.GC_RESERVE_ZONES:
        raise RuntimeError("DM_GC_LOW_WATERMARK must be greater than DM_GC_RESERVED_ZONES")
    if cfg.GC_HIGH_WATERMARK <= cfg.GC_LOW_WATERMARK:
        raise RuntimeError("DM_GC_HIGH_WATERMARK must be greater than DM_GC_LOW_WATERMARK")
    module_args = [
        "sudo", "insmod", cfg.DM_MODULE_PATH,
        f"gc_low_watermark={cfg.GC_LOW_WATERMARK}",
    ]
    if getattr(cfg, "DM_METRICS", False):
        module_args.append("metrics_enabled=1")
    # Older settings.py versions do not expose the dynamic-only diagnostic knob.
    diag_budget = getattr(cfg, "GC_DIAG_BUDGET", 0)
    if cfg.DM_IMPLEMENTATION == "fixed":
        if cfg.GC_RESERVE_ZONES != 2:
            raise RuntimeError("fixed implementation requires DM_GC_RESERVED_ZONES=2")
        if diag_budget:
            raise RuntimeError("fixed implementation does not support gc_diag_budget")
        module_args.append(f"gc_target_free_zones={cfg.GC_HIGH_WATERMARK}")
    elif cfg.DM_IMPLEMENTATION == "dynamic":
        module_args.extend([
            f"gc_reserved_zones={cfg.GC_RESERVE_ZONES}",
            f"gc_high_watermark={cfg.GC_HIGH_WATERMARK}",
        ])
        if diag_budget:
            module_args.append(f"gc_diag_budget={diag_budget}")
    else:
        raise RuntimeError(f"Unsupported DM implementation: {cfg.DM_IMPLEMENTATION}")
    if getattr(cfg, "WAL_DIAGNOSTIC_DELAY_MS", None) is not None:
        module_args.extend(["wal_timing_enabled=1",
                            f"wal_batch_delay_ms={cfg.WAL_DIAGNOSTIC_DELAY_MS}"])
    result = subprocess.run(
        module_args,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"insmod failed:\n{result.stdout}\n{result.stderr}")
    logical_sectors = zns_logical_sectors()
    result = subprocess.run(
        ["sudo", "dmsetup", "create", cfg.DM_NAME],
        input=f"0 {logical_sectors} zns-base {cfg.RAW_ZNS_DEVICE}\n",
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        run_cmd_quiet(f"sudo rmmod {dm_module_name()} || true")
        raise RuntimeError(f"dmsetup create failed:\n{result.stdout}\n{result.stderr}")
    print(
        f"[DM] Created {cfg.FS_DEVICE}: {logical_sectors} sectors "
        f"({logical_sectors * 512 / 1024**3:.2f} GiB, "
        f"logical={cfg.LOGICAL_CAPACITY_PERCENT}% max, "
        f"GC reserve/low/high={cfg.GC_RESERVE_ZONES}/"
        f"{cfg.GC_LOW_WATERMARK}/{cfg.GC_HIGH_WATERMARK})"
    )


def stop_stale_kafka_processes():
    """Stop only known Kafka JVM main classes before replacing the log device.

    Do not use a broad pattern such as ``pkill -f kafka`` here.  The benchmark
    runner itself normally lives below a ``kafka_python_result`` directory, so
    that pattern also matches and kills the Python process when an absolute
    script path is used.
    """
    for process_class in ("kafka.Kafka", "QuorumPeerMain"):
        run_cmd_quiet(f"pkill -9 -f '{process_class}' || true")


def setup_filesystem(fs_type):
    if fs_type not in cfg.FILESYSTEMS:
        raise ValueError(f"Unsupported filesystem: {fs_type}")
    print(f"\n[Setup] Reinitializing {cfg.STORAGE_BACKEND} and mounting {fs_type} ...")
    stop_stale_kafka_processes()
    time.sleep(2)
    unmount_log_device()
    if cfg.STORAGE_BACKEND == "cns":
        validate_cns_device()
    else:
        remove_dm_stack()
        result = run_cmd_full(f"sudo blkzone reset {cfg.RAW_ZNS_DEVICE}")
        if result.returncode != 0:
            raise RuntimeError(f"zone reset failed:\n{result.stdout}\n{result.stderr}")
        create_dm_target()
    result = run_cmd_full(f"sudo wipefs -a {cfg.FS_DEVICE}")
    if result.returncode != 0:
        raise RuntimeError(f"wipefs failed:\n{result.stdout}\n{result.stderr}")

    if fs_type == "ext4":
        result = run_cmd_full(f"sudo mkfs.ext4 -F -E nodiscard -L kafka_ext4 {cfg.FS_DEVICE}")
    else:
        result = run_cmd_full(f"sudo mkfs.f2fs -f -t 0 -l kafka_f2fs {cfg.FS_DEVICE}")
    if result.returncode != 0:
        raise RuntimeError(f"mkfs failed:\n{result.stdout}\n{result.stderr}")

    run_cmd_quiet(f"sudo mkdir -p {cfg.MOUNT_POINT}")
    discard_enabled = (
        (fs_type == "ext4" and cfg.EXT4_DISCARD)
        or (fs_type == "f2fs" and cfg.F2FS_DISCARD)
    )
    discard_option = "discard" if discard_enabled else "nodiscard"
    result = run_cmd_full(
        f"sudo mount -o noatime,nodiratime,{discard_option} {cfg.FS_DEVICE} {cfg.MOUNT_POINT}"
    )
    if result.returncode != 0:
        raise RuntimeError(f"mount failed:\n{result.stdout}\n{result.stderr}")
    run_cmd_quiet(f"sudo rm -rf {cfg.MOUNT_POINT}/lost+found")
    run_cmd_quiet(f"sudo chmod 777 {cfg.MOUNT_POINT}")
    print(run_cmd_quiet(f"lsblk {cfg.FS_DEVICE}"))
    print(run_cmd_quiet(f"findmnt -no SOURCE,FSTYPE,OPTIONS {cfg.MOUNT_POINT}"))
    run_cmd_quiet("sudo sync")
    run_cmd_quiet("echo 3 | sudo tee /proc/sys/vm/drop_caches")
    if cfg.SEPARATE_METADATA_DIR:
        run_cmd_quiet(f"sudo mkdir -p {cfg.METADATA_DIR}")
        run_cmd_quiet(f"sudo rm -rf {cfg.METADATA_DIR}/*")
        run_cmd_quiet(f"sudo chmod 777 {cfg.METADATA_DIR}")


def filesystem_usage():
    """Return logical usage of the mounted benchmark filesystem.

    ``df /`` is intentionally not used: the guest root disk is unrelated to
    the dm-zns-base device being evaluated.  ``f_bavail`` is used for free
    space so reserved filesystem blocks are treated as unavailable.
    """
    stats = os.statvfs(cfg.MOUNT_POINT)
    total = stats.f_blocks * stats.f_frsize
    free = stats.f_bfree * stats.f_frsize
    available = stats.f_bavail * stats.f_frsize
    used = total - free
    usable = used + available
    return {
        "total_bytes": total,
        "usable_bytes": usable,
        "used_bytes": used,
        "available_bytes": available,
        "used_percent": (used * 100.0 / usable) if usable else 0.0,
    }


def _run_with_refresh(command):
    """Render carriage-return progress by replacing one terminal line."""
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0
    )
    pending = bytearray()
    progress_active = False
    # During a run sys.stdout is TeeLogger.  Progress belongs on the terminal
    # only; newline-delimited fio summaries still pass through the run log.
    terminal = getattr(sys.stdout, "terminal", sys.stdout)

    while True:
        char = process.stdout.read(1)
        if not char:
            break
        if char == b"\r":
            terminal.write("\r\033[K" + pending.decode(errors="replace"))
            terminal.flush()
            pending.clear()
            progress_active = True
        elif char == b"\n":
            if progress_active:
                terminal.write("\r\033[K")
                terminal.flush()
                progress_active = False
            print(pending.decode(errors="replace"))
            pending.clear()
        else:
            pending.extend(char)

    if pending:
        if progress_active:
            terminal.write("\r\033[K")
            terminal.flush()
        print(pending.decode(errors="replace"))
        progress_active = False
    if progress_active:
        terminal.write("\n")
        terminal.flush()
    return process.wait()


def fill_filesystem_to(target_percent):
    """Append real data until the benchmark filesystem reaches a target.

    fio performs actual direct writes; fallocate is unsuitable because merely
    allocating extents would not age the filesystem or dm-zns-base.  The file
    remains present for subsequent measurements so occupancy increases
    monotonically within one filesystem run.
    """
    if target_percent <= 0 or target_percent > cfg.MAX_OCCUPANCY_PERCENT:
        raise ValueError(
            f"target occupancy must be in 1..{cfg.MAX_OCCUPANCY_PERCENT}%"
        )
    before = filesystem_usage()
    target_bytes = int(before["usable_bytes"] * target_percent / 100.0)
    delta = target_bytes - before["used_bytes"]
    # Direct I/O requires aligned lengths.  A sub-MiB difference is immaterial
    # at a 32-GiB scale and is reported through the observed percentage.
    delta = delta // (1024 * 1024) * (1024 * 1024)
    if delta > 0:
        offset = os.path.getsize(cfg.PREFILL_FILE) if os.path.exists(cfg.PREFILL_FILE) else 0
        print(
            f"[Prefill] {before['used_percent']:.2f}% -> {target_percent}% "
            f"(writing {delta / 1024**3:.2f} GiB)"
        )
        returncode = _run_with_refresh([
            "sudo", "fio", "--name=benchmark-prefill",
            f"--filename={cfg.PREFILL_FILE}", "--rw=write", "--bs=1M",
            "--ioengine=sync", "--direct=1", f"--offset={offset}",
            f"--size={delta}", "--end_fsync=1", "--eta=always",
        ])
        if returncode != 0:
            raise RuntimeError(f"filesystem prefill failed with exit code {returncode}")
        run_cmd_quiet("sudo sync")
    after = filesystem_usage()
    if after["used_percent"] > cfg.MAX_OCCUPANCY_PERCENT + 1.0:
        raise RuntimeError(
            f"benchmark filesystem exceeded safety limit: {after['used_percent']:.2f}%"
        )
    print(
        f"[Prefill] observed usage: {after['used_percent']:.2f}% "
        f"({after['available_bytes'] / 1024**3:.2f} GiB available)"
    )
    return after


def wait_for_filesystem_usage(expected_percent=None, stable_samples=3):
    """Wait for asynchronous topic deletion to release filesystem space.

    Kafka topic deletion and filesystem block reclamation can continue after
    the Admin API returns.  Starting the next run immediately would mix that
    cleanup I/O into the measurement and make a nominal 20% run begin at 25%.
    Usage must remain nearly unchanged for consecutive samples; when an
    occupancy target is supplied it must also return within the configured
    tolerance.
    """
    deadline = time.monotonic() + cfg.OCCUPANCY_STABILIZE_TIMEOUT_SECONDS
    previous = None
    stable = 0
    while time.monotonic() < deadline:
        usage = filesystem_usage()
        current = usage["used_percent"]
        if previous is not None and abs(current - previous) <= 0.05:
            stable += 1
        else:
            stable = 0
        within_target = expected_percent is None or (
            current <= expected_percent + cfg.OCCUPANCY_TOLERANCE_PERCENT
        )
        if stable >= stable_samples and within_target:
            print(f"[Occupancy] filesystem usage stabilized at {current:.2f}%")
            return usage
        previous = current
        time.sleep(2)
    target_text = "any stable value" if expected_percent is None else (
        f"<= {expected_percent + cfg.OCCUPANCY_TOLERANCE_PERCENT:.2f}%"
    )
    raise RuntimeError(
        f"filesystem usage did not stabilize at {target_text} within "
        f"{cfg.OCCUPANCY_STABILIZE_TIMEOUT_SECONDS}s; last={previous:.2f}%"
    )


def validate_kafka_environment():
    print("[Check] Validating Kafka environment ...")
    if "4.2" not in cfg.KAFKA_PATH:
        print(f"[WARN] KAFKA_PATH에 4.2 문자열이 없습니다: {cfg.KAFKA_PATH}")
    required = [
        f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh",
        f"{cfg.KAFKA_PATH}/bin/kafka-server-start.sh",
        f"{cfg.KAFKA_PATH}/bin/kafka-server-stop.sh",
        cfg.KRAFT_CONFIG,
        cfg.JAVA_BENCH_JAR,
    ]
    missing = [path for path in required if not os.path.exists(path)]
    if missing:
        raise RuntimeError("Required benchmark files not found:\n" + "\n".join(missing))


def prepare_experiment_kraft_config():
    with open(cfg.KRAFT_CONFIG, "r", encoding="utf-8") as source:
        lines = source.readlines()
    managed_keys = ("log.dirs", "metadata.log.dir")
    lines = [
        line for line in lines
        if not any(line.strip().startswith(f"{key}=") for key in managed_keys)
    ]
    lines.append(f"\nlog.dirs={cfg.MOUNT_POINT}\n")
    if cfg.SEPARATE_METADATA_DIR:
        lines.append(f"metadata.log.dir={cfg.METADATA_DIR}\n")
    write_text(cfg.EXPERIMENT_KRAFT_CONFIG, "".join(lines))
    print(f"[Env] Experiment Kafka config saved: {cfg.EXPERIMENT_KRAFT_CONFIG}")


def control_kafka(action):
    if action == "stop":
        print("[Service] Stopping Kafka KRaft broker ...")
        run_cmd_quiet(f"{cfg.KAFKA_PATH}/bin/kafka-server-stop.sh")
        if not wait_for_port(9092, timeout=15, closed=True):
            run_cmd_quiet("sudo fuser -k 9092/tcp || true")
            run_cmd_quiet("pkill -TERM -f 'kafka.Kafka' || true")
            if not wait_for_port(9092, timeout=10, closed=True):
                run_cmd_quiet("pkill -KILL -f 'kafka.Kafka' || true")
                wait_for_port(9092, timeout=5, closed=True)
        run_cmd_quiet("sudo sync")
        time.sleep(2)
        return
    if action != "start":
        raise ValueError(f"Unknown action: {action}")

    print("[Service] Starting Kafka in KRaft mode ...")
    cluster_id = existing_kraft_cluster_id()
    if cluster_id:
        print(f"[Service] Reusing KRaft cluster id: {cluster_id}")
    else:
        cluster_id = run_cmd_quiet(f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh random-uuid")
    if not cluster_id:
        raise RuntimeError("Failed to generate KRaft cluster id")
    result = run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh format -t {cluster_id} "
        f"-c {cfg.EXPERIMENT_KRAFT_CONFIG} --standalone --ignore-formatted"
    )
    if result.returncode != 0:
        raise RuntimeError(f"KRaft storage format failed:\n{result.stdout}\n{result.stderr}")
    overrides = " ".join([
        "--override num.partitions=8",
        "--override offsets.topic.replication.factor=1",
        "--override transaction.state.log.replication.factor=1",
        "--override transaction.state.log.min.isr=1",
        "--override default.replication.factor=1",
        "--override log.retention.check.interval.ms=1000",
        "--override log.segment.delete.delay.ms=0",
        "--override auto.create.topics.enable=false",
        "--override message.max.bytes=10485760",
        "--override replica.fetch.max.bytes=10485760",
    ])
    run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-server-start.sh -daemon "
        f"{cfg.EXPERIMENT_KRAFT_CONFIG} {overrides}"
    )
    if not wait_for_port(9092, timeout=180):
        raise RuntimeError("Kafka KRaft broker start timeout")
    time.sleep(8)


def existing_kraft_cluster_id():
    """Return the cluster ID already formatted on the current filesystem.

    KRaft writes ``meta.properties`` to both the data log directory and the
    optional metadata directory.  Occupancy and long-running modes preserve
    those directories across broker restarts, so generating a new UUID at
    every start would make Kafka reject the existing storage.  A disagreement
    between the two files indicates genuine storage corruption or stale state
    and must not be hidden by choosing either value.
    """
    paths = [f"{cfg.MOUNT_POINT}/meta.properties"]
    if cfg.SEPARATE_METADATA_DIR:
        paths.append(f"{cfg.METADATA_DIR}/meta.properties")
    found = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as file:
                for raw_line in file:
                    key, separator, value = raw_line.strip().partition("=")
                    if separator and key == "cluster.id" and value:
                        found[path] = value
                        break
        except OSError as exc:
            raise RuntimeError(f"cannot read KRaft metadata {path}: {exc}") from exc
    unique_ids = set(found.values())
    if len(unique_ids) > 1:
        details = ", ".join(f"{path}={value}" for path, value in found.items())
        raise RuntimeError(f"KRaft cluster id mismatch: {details}")
    return next(iter(unique_ids), "")


def recreate_main_topic(retention_total_bytes=None):
    print("[Service] Recreating benchmark topic ...")
    run_cmd_quiet(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} "
        f"--delete --topic {cfg.TOPIC_NAME} --if-exists"
    )
    time.sleep(3)
    topic_configs = ""
    if retention_total_bytes is not None:
        if retention_total_bytes <= 0:
            raise ValueError("retention_total_bytes must be positive")
        retention_per_partition = retention_total_bytes // cfg.TOPIC_PARTITIONS
        if retention_per_partition < cfg.RETENTION_SEGMENT_BYTES * 2:
            raise ValueError(
                "topic retention must hold at least two segments per partition"
            )
        topic_configs = " ".join([
            "--config cleanup.policy=delete",
            f"--config retention.bytes={retention_per_partition}",
            "--config retention.ms=-1",
            f"--config segment.bytes={cfg.RETENTION_SEGMENT_BYTES}",
            f"--config segment.ms={cfg.RETENTION_SEGMENT_MS}",
        ])
        print(
            f"[Service] Topic retention: {retention_total_bytes / 1024**3:.2f} GiB total "
            f"({retention_per_partition / 1024**2:.0f} MiB/partition)"
        )
    run_cmd_quiet(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} "
        f"--create --topic {cfg.TOPIC_NAME} --partitions {cfg.TOPIC_PARTITIONS} "
        f"--replication-factor 1 --if-not-exists {topic_configs}"
    )
    time.sleep(3)


def count_kafka_topics():
    """Return the broker's current topic count, or -1 when the query fails."""
    result = run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} --list"
    )
    if result.returncode != 0:
        return -1
    return len([line for line in result.stdout.splitlines() if line.strip()])

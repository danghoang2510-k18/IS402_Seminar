#!/usr/bin/env python3
"""Full-dataset IoT telemetry replay for FreeManufacturingQC100M.

- Streams ALL source Parquet rows in bounded Arrow batches; no sampling.
- Preserves source reading/machine/line, label and original sensor values.
- Adds 5 (configurable) virtual machines, current UTC EventTimestamp and stable replay IDs.
- Supports one IoT Hub device identity or multiple DIFFERENT identities for throughput.
- One async sending worker / SDK client per identity (no concurrent calls on one client).
- Global rate limit, bounded asyncio queue, retries with exponential backoff.
- Atomically checkpoints contiguous *acknowledged* row positions; resume is
  AT-LEAST-ONCE, NOT EXACTLY ONCE. A crash may cause duplicate messages.
- Produces streaming operational stats and approximate p50/p95 latency buckets.

Compatible with SAquerry_calibrated.sql. SQL ignores extra ReplayRunId,
ReplayRowIndex and original provenance fields, so downstream outputs are NOT
automatically deduplicated. Unlike the original reservoir demo, the full replay
reads source rows in file order (network arrival may reorder when parallel)
and preserves natural anomaly prevalence; it does NOT force
Normal -> Anomaly -> Recovery phases.

Examples:
  python simulation_iot_full_100m.py --dry-run --limit 1000 --rate 0
  export IOTHUB_DEVICE_CONNECTION_STRING='YOUR_ROTATED_CONNECTION_STRING'
  python simulation_iot_full_100m.py --limit 1000 --rate 30
  python simulation_iot_full_100m.py --resume --rate 30  # finish remaining rows
  python simulation_iot_full_100m.py --connections-file devices.txt --rate 500

NOTE: Each line in devices.txt is a secret connection string for a DIFFERENT
registered device on the SAME IoT Hub. chmod 600 devices.txt; do NOT commit it.
This sends billable traffic. Verify IoT Hub tier/quota, ASA units, Blob throughput
and Azure Monitor metrics before moving beyond small tests.
"""

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import stat
import statistics
import time
import uuid

LOG = logging.getLogger("iot_100m")
DEFAULT_SOURCE = (
    "hf://datasets/ziadatalabs/FreeManufacturingQC100M/"
    "manufacturing_qc_readings_100M.parquet"
)
SOURCE_COLUMNS = (
    "reading_id", "machine_id", "production_line", "sensor_type",
    "sensor_value", "unit", "is_anomaly",
)


def positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return result


def nonnegative_float(value):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("must be finite and >= 0")
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=os.getenv("PARQUET_SOURCE", DEFAULT_SOURCE))
    parser.add_argument("--machines", type=positive_int, default=5,
                        help="number of logical/virtual machines (default: 5)")
    parser.add_argument("--batch-size", type=positive_int, default=4096,
                        help="Arrow batch row count; does not load full dataset")
    parser.add_argument("--queue-size", type=positive_int, default=2000,
                        help="maximum queued records, i.e. RAM/backpressure bound")
    parser.add_argument("--rate", type=nonnegative_float, default=50.0,
                        help="GLOBAL SDK send attempts per second; 0 allowed only with --dry-run")
    parser.add_argument("--limit", type=positive_int, default=None,
                        help="optional absolute source-row upper limit (exclusive); default ALL")
    parser.add_argument("--retries", type=positive_int, default=5)
    parser.add_argument("--retry-max-delay", type=positive_int, default=30)
    parser.add_argument("--connections-file", type=Path, default=None,
                        help="one distinct DeviceId connection string per line")
    parser.add_argument("--checkpoint", type=Path, default=Path("iot_100m_checkpoint.json"))
    parser.add_argument("--summary", type=Path, default=Path("iot_100m_summary.json"))
    parser.add_argument("--resume", action="store_true",
                        help="resume existing checkpoint; may replay recent successful messages")
    parser.add_argument("--checkpoint-every", type=positive_int, default=1000)
    parser.add_argument("--checkpoint-seconds", type=positive_int, default=15)
    parser.add_argument("--progress-every", type=positive_int, default=10000)
    parser.add_argument("--dry-run", action="store_true",
                        help="no Azure connection, processes input and builds payloads locally")
    parser.add_argument("--preview", type=positive_int, default=3,
                        help="print this many sample JSON messages in dry run")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default="INFO")
    args = parser.parse_args(argv)
    if args.rate == 0 and not args.dry_run:
        parser.error("--rate 0 (unlimited) is only permitted for --dry-run")
    if args.resume and not args.checkpoint.exists():
        parser.error("--resume requires an existing checkpoint file")
    if args.checkpoint.exists() and not args.resume:
        parser.error(f"checkpoint already exists: {args.checkpoint}; use --resume or a new --checkpoint")
    return args


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def atomic_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class ParquetBatchReader:
    """Read selected columns, skipping completed row groups on resume."""

    def __init__(self, source, batch_size, start_row=0):
        import fsspec
        import pyarrow.parquet as pq

        self.source = source
        # Session-only Parquet read/decode measurements (not physical disk/network bytes).
        self.batch_read_seconds = 0.0
        self.batch_logical_bytes = 0
        self.batch_rows = 0
        self.stream = fsspec.open(source, "rb").open()
        try:
            self.pf = pq.ParquetFile(self.stream)
            self.total_rows = self.pf.metadata.num_rows
            names = set(self.pf.schema_arrow.names)
            missing = sorted(set(SOURCE_COLUMNS).difference(names))
            if missing:
                raise ValueError(f"Missing Parquet columns: {missing}")
            if start_row < 0 or start_row > self.total_rows:
                raise ValueError(f"start_row {start_row} beyond dataset size {self.total_rows}")
            self.schema_hash = hashlib.sha256(
                str(self.pf.schema_arrow).encode("utf-8")
            ).hexdigest()
            cumulative = 0
            start_rg = self.pf.num_row_groups
            skip_in_rg = 0
            for i in range(self.pf.num_row_groups):
                count = self.pf.metadata.row_group(i).num_rows
                if start_row < cumulative + count:
                    start_rg, skip_in_rg = i, start_row - cumulative
                    break
                cumulative += count
            self._pos = start_row
            self._skip = skip_in_rg
            groups = range(start_rg, self.pf.num_row_groups)
            self._iterator = self.pf.iter_batches(
                batch_size=batch_size, columns=list(SOURCE_COLUMNS),
                row_groups=list(groups), use_threads=True,
            )
        except Exception:
            self.stream.close()
            raise

    def next_batch(self):
        while True:
            read_started = time.perf_counter()
            try:
                batch = next(self._iterator)
            except StopIteration:
                return None
            if self._skip:
                take = min(self._skip, batch.num_rows)
                self._skip -= take
                if take == batch.num_rows:
                    continue
                batch = batch.slice(take)
            start = self._pos
            self._pos += batch.num_rows
            logical_bytes = batch.nbytes  # Arrow uncompressed in-memory bytes, not file bytes.
            columns = batch.to_pydict()
            self.batch_read_seconds += time.perf_counter() - read_started
            self.batch_logical_bytes += logical_bytes
            self.batch_rows += batch.num_rows
            return (start, columns, batch.num_rows)

    def close(self):
        self.stream.close()


def safe_str(x):
    return None if x is None else str(x)


def normalize_value(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def source_flag(value):
    if value is None:
        return None
    if value is True or value == 1:
        return 1
    if value is False or value == 0:
        return 0
    return None


def encode_record(raw, row_index, machines, run_id):
    machine_no = row_index % machines
    return {
        "ReplayRunId": run_id,
        "ReplayRowIndex": row_index,
        "MachineId": f"SIM-{machine_no + 1:02d}",
        "ProductionLine": f"Demo-Line-{machine_no + 1:02d}",
        "SourceReadingId": safe_str(raw["reading_id"]),
        "SourceMachineId": safe_str(raw["machine_id"]),
        "SourceProductionLine": safe_str(raw["production_line"]),
        "SensorType": safe_str(raw["sensor_type"]).lower() if raw["sensor_type"] is not None else None,
        "SensorValue": normalize_value(raw["sensor_value"]),
        "unit": safe_str(raw["unit"]),
        "SourceAnomalyFlag": source_flag(raw["is_anomaly"]),
        "EventTimestamp": utcnow(),
    }


class GlobalPacer:
    """Pace all sending attempts globally (including retries), without bursts."""

    def __init__(self, rate):
        self.step = 0 if rate == 0 else 1 / rate
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        if not self.step:
            return
        async with self.lock:
            now = time.monotonic()
            send_at = max(now, self.next_at)
            self.next_at = send_at + self.step
        delay = send_at - now
        if delay > 0:
            await asyncio.sleep(delay)


class LatencyStats:
    """Logarithmic histogram for approximate quantiles in bounded memory."""

    def __init__(self, prior=None):
        prior = prior or {}
        self.count = int(prior.get("count", 0))
        self.sum_ms = float(prior.get("sum_ms", 0.0))
        self.bins = Counter({int(k): int(v) for k, v in prior.get("bins", {}).items()})

    def add(self, ms):
        ms = max(0.0, float(ms))
        self.count += 1
        self.sum_ms += ms
        self.bins[int(math.log1p(ms) * 100)] += 1

    def quantile(self, fraction):
        if not self.count:
            return None
        target = max(1, math.ceil(self.count * fraction))
        so_far = 0
        for idx, n in sorted(self.bins.items()):
            so_far += n
            if so_far >= target:
                return round(math.expm1((idx + 0.5) / 100), 2)
        return None

    def json(self):
        return {"count": self.count, "sum_ms": round(self.sum_ms, 4),
                "bins": {str(k): v for k, v in sorted(self.bins.items())}}


class Progress:
    def __init__(self, source, total_rows, schema_hash, run_id, start_row, prior, args):
        self.source = source
        self.total_rows = total_rows
        self.schema_hash = schema_hash
        self.run_id = run_id
        self.next_row = start_row  # earliest source row not durably acknowledged
        self.out_of_order = set()
        self.success = int(prior.get("success", 0))
        self.failed_attempts = int(prior.get("failed_attempts", 0))
        self.payload_bytes = int(prior.get("payload_bytes", 0))
        self.invalid_values = int(prior.get("invalid_values", 0))
        self.by_machine = Counter(prior.get("by_machine", {}))
        self.by_sensor = Counter(prior.get("by_sensor", {}))
        self.latencies = LatencyStats(prior.get("latency"))
        self.elapsed_seconds = float(prior.get("elapsed_seconds", 0.0))
        self.persisted_next_row = start_row
        self.last_save = time.monotonic()
        self.started = time.monotonic()
        self.session_started_utc = utcnow()
        self.session_start_success = self.success
        self.args = args
        self.preview_count = 0
        self.read_count = 0

    def acknowledge(self, index, payload):
        self.success += 1
        self.payload_bytes += len(payload)
        if index == self.next_row:
            self.next_row += 1
            while self.next_row in self.out_of_order:
                self.out_of_order.remove(self.next_row)
                self.next_row += 1
        elif index > self.next_row:
            self.out_of_order.add(index)
        else:
            raise RuntimeError(f"duplicate acknowledged index {index}")

    def maybe_save(self, force=False):
        now = time.monotonic()
        if (force or
            self.next_row - self.persisted_next_row >= self.args.checkpoint_every or
            now - self.last_save >= self.args.checkpoint_seconds):
            data = {
                "version": 1, "source": self.source,
                "source_rows": self.total_rows, "source_schema_sha256": self.schema_hash,
                "machines": self.args.machines, "dry_run": self.args.dry_run,
                "run_id": self.run_id, "next_row": self.next_row,
                "success": self.success,
                "failed_attempts": self.failed_attempts,
                "payload_bytes": self.payload_bytes,
                "invalid_values": self.invalid_values,
                "by_machine": dict(self.by_machine), "by_sensor": dict(self.by_sensor),
                "latency": self.latencies.json(),
                "elapsed_seconds": round(self.elapsed_seconds + (now - self.started), 4),
                "updated_utc": utcnow(),
            }
            atomic_json(self.args.checkpoint, data)
            self.last_save = now
            self.persisted_next_row = self.next_row

    def summary(self, status, error=None, reader=None):
        session_elapsed = time.monotonic() - self.started
        elapsed = self.elapsed_seconds + session_elapsed
        read_seconds = getattr(reader, "batch_read_seconds", None)
        logical_bytes = getattr(reader, "batch_logical_bytes", None)
        return {
            "status": status, "error_type": type(error).__name__ if error else None,
            "source": self.source, "source_rows": self.total_rows,
            "run_id": self.run_id, "next_row_acknowledged": self.next_row,
            "percent_source_acked": round(100 * self.next_row / self.total_rows, 5) if self.total_rows else 100,
            "dry_run": self.args.dry_run,
            "virtual_machines": self.args.machines,
            "configured_rate_attempts_per_second": self.args.rate,
            "session_started_at_utc": self.session_started_utc,
            "session_finished_at_utc": utcnow(),
            "session_elapsed_seconds": round(session_elapsed, 3),
            "session_success_messages": self.success - self.session_start_success,
            "session_success_messages_per_second": round(
                (self.success - self.session_start_success) / session_elapsed, 3
            ) if session_elapsed else None,
            "parquet_batch_read_decode_seconds_session": round(read_seconds, 4) if read_seconds is not None else None,
            "parquet_batch_logical_bytes_session": logical_bytes,
            "parquet_batch_logical_mib_per_second_session": round(
                logical_bytes / (1024 ** 2) / read_seconds, 3
            ) if logical_bytes is not None and read_seconds else None,
            "parquet_batch_rows_read_session": getattr(reader, "batch_rows", None),
            "parquet_read_metric_note": (
                "Arrow uncompressed logical bytes per batch / batch read+decode time; "
                "NOT physical Parquet file I/O, and excludes sender/queue wait. "
                "The final batch may extend beyond --limit."
            ),
            "session_read_rows": self.read_count,
            "success_total": self.success,
            "failed_attempts_total": self.failed_attempts,
            "payload_bytes_total": self.payload_bytes,
            "payload_gib_total": round(self.payload_bytes / 1024**3, 4),
            "invalid_sensor_value_count": self.invalid_values,
            "by_machine": dict(self.by_machine), "by_sensor": dict(self.by_sensor),
            "elapsed_seconds_total": round(elapsed, 3),
            "average_success_messages_per_second": round(self.success / elapsed, 3) if elapsed else None,
            "sdk_send_latency_ms_mean": (
                round(self.latencies.sum_ms / self.latencies.count, 2)
                if self.latencies.count else None
            ),
            "sdk_send_latency_ms_p50_approx": self.latencies.quantile(.5),
            "sdk_send_latency_ms_p95_approx": self.latencies.quantile(.95),
            "sdk_latency_observations": self.latencies.count,
            "checkpoint_file": str(self.args.checkpoint),
            "warning": "AT-LEAST-ONCE; checkpoint after SDK acknowledgments may repeat rows after crash. SQL outputs are not deduplicated.",
        }


def connection_strings(args):
    if args.connections_file:
        path = args.connections_file
        if not path.is_file():
            raise ValueError(f"Cannot find connections file: {path}")
        if os.name == "posix" and stat.S_IMODE(path.stat().st_mode) & 0o077:
            LOG.warning("Secret file %s is readable by other users; run chmod 600", path)
        strings = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
                   if line.strip() and not line.lstrip().startswith("#")]
    else:
        conn = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
        strings = [conn] if conn else []
    if not strings:
        raise ValueError("No device connection strings. Set IOTHUB_DEVICE_CONNECTION_STRING or --connections-file")
    ids = set()
    host = None
    for s in strings:
        d = dict(part.split("=", 1) for part in s.split(";") if "=" in part)
        identity = d.get("DeviceId")
        hub = d.get("HostName")
        if not identity or not hub:
            raise ValueError("Each connection string needs HostName and DeviceId")
        if identity in ids:
            raise ValueError("Duplicate DeviceId in connections: use different registered identities")
        if host is not None and host != hub:
            raise ValueError("All device identities must belong to the same IoT Hub")
        host = hub
        ids.add(identity)
    LOG.info("Configured %d DISTINCT IoT Hub device identities; hub=%s", len(ids), host)
    return strings


async def send_worker(name, queue, client, progress, pacer, args):
    if not args.dry_run:
        from azure.iot.device import Message
    while True:
        item = await queue.get()
        if item is None:
            return
        index, raw = item
        payload_obj = encode_record(raw, index, args.machines, progress.run_id)
        raw_json = json.dumps(payload_obj, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        payload = raw_json.encode("utf-8")
        if payload_obj["SensorValue"] is None:
            progress.invalid_values += 1
        if args.dry_run:
            if progress.preview_count < args.preview:
                LOG.info("DRY RUN payload: %s", raw_json)
                progress.preview_count += 1
        else:
            message = Message(raw_json)
            message.content_encoding = "utf-8"
            message.content_type = "application/json"
            last_error = None
            for retry in range(args.retries):
                await pacer.wait()
                started = time.perf_counter()
                try:
                    await client.send_message(message)
                    progress.latencies.add((time.perf_counter() - started) * 1000)
                    last_error = None
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    progress.failed_attempts += 1
                    last_error = exc
                    LOG.warning("Worker %s row %d attempt %d/%d failed (%s)",
                                name, index, retry + 1, args.retries, type(exc).__name__)
                    if retry < args.retries - 1:
                        await asyncio.sleep(min(args.retry_max_delay, 2 ** retry) + random.random() * 0.25)
            if last_error is not None:
                raise RuntimeError(f"Unable to send row {index} after {args.retries} attempts") from last_error
        progress.by_machine[payload_obj["MachineId"]] += 1
        progress.by_sensor[str(payload_obj["SensorType"])] += 1
        progress.acknowledge(index, payload)
        progress.maybe_save()
        if progress.success % args.progress_every == 0:
            elapsed = time.monotonic() - progress.started
            LOG.info("Progress: %d cumulative SDK successes; acked next=%d/%d (%.2f%%); %.1f source rows/s this session",
                     progress.success, progress.next_row, progress.total_rows,
                     100 * progress.next_row / max(1, progress.total_rows),
                     progress.read_count / max(.001, elapsed))


async def producer(reader, queue, workers, stop_row, progress):
    while True:
        batch = await asyncio.to_thread(reader.next_batch)
        if batch is None:
            break
        start, columns, count = batch
        hit_limit = False
        for j in range(count):
            index = start + j
            if index >= stop_row:
                hit_limit = True
                break
            row = {field: columns[field][j] for field in SOURCE_COLUMNS}
            await queue.put((index, row))
            progress.read_count += 1
        if hit_limit:
            break
    # Only issue sentinels on successful producer completion. On failure,
    # TaskGroup cancels sibling workers and a full queue cannot deadlock us.
    for _ in range(workers):
        await queue.put(None)


async def run(args):
    prior = {}
    if args.resume:
        prior = json.loads(args.checkpoint.read_text(encoding="utf-8"))
        if prior.get("version") != 1:
            raise RuntimeError("Unsupported checkpoint version")
        if prior.get("source") != args.source:
            raise RuntimeError("Source differs from checkpoint; do not change source on resume")

    start_row = int(prior.get("next_row", 0))
    reader = await asyncio.to_thread(ParquetBatchReader, args.source, args.batch_size, start_row)
    progress = None
    clients = []
    status = "failed"
    error = None
    try:
        if prior:
            if (reader.total_rows != prior.get("source_rows") or
                reader.schema_hash != prior.get("source_schema_sha256")):
                raise RuntimeError("Parquet metadata/schema changed since checkpoint")
            if prior.get("machines") != args.machines:
                raise RuntimeError("--machines differs from checkpoint; keep virtual machine IDs stable")
            if prior.get("dry_run") != args.dry_run:
                raise RuntimeError("Cannot mix --dry-run and real sends in one checkpoint")
        stop_row = min(reader.total_rows, args.limit if args.limit is not None else reader.total_rows)
        if start_row > stop_row:
            raise RuntimeError(f"Checkpoint next_row={start_row} exceeds --limit={stop_row}")
        run_id = prior.get("run_id") or str(uuid.uuid4())
        progress = Progress(args.source, reader.total_rows, reader.schema_hash,
                            run_id, start_row, prior, args)
        LOG.info("Dataset metadata=%s rows; start=%s; stop=%s; batch=%s; rate=%.1f/s",
                 reader.total_rows, start_row, stop_row, args.batch_size, args.rate)
        if args.dry_run:
            identities = [None]
        else:
            from azure.iot.device.aio import IoTHubDeviceClient
            for conn in connection_strings(args):
                client = IoTHubDeviceClient.create_from_connection_string(conn)
                clients.append(client)
            for client in clients:
                await client.connect()
            identities = clients

        queue = asyncio.Queue(maxsize=args.queue_size)
        pacer = GlobalPacer(args.rate)
        async with asyncio.TaskGroup() as tg:
            for idx, client in enumerate(identities):
                tg.create_task(send_worker(idx, queue, client, progress, pacer, args))
            tg.create_task(producer(reader, queue, len(identities), stop_row, progress))
        if progress.next_row != stop_row:
            raise RuntimeError(f"Incomplete: acknowledged up to {progress.next_row}, expected {stop_row}")
        status = "complete_source" if stop_row >= reader.total_rows else "stopped_at_limit"
        LOG.info("Finished with status=%s; contiguous acknowledged rows=%d", status, progress.next_row)
    except BaseException as exc:
        error = exc
        LOG.error("Replay failed/interrupted: %s", type(exc).__name__)
        raise
    finally:
        if progress:
            progress.maybe_save(force=True)
            atomic_json(args.summary, progress.summary(status, error, reader))
        for client in clients:
            try:
                await client.shutdown()
            except Exception:
                LOG.exception("Error while closing an IoT Hub client")
        await asyncio.to_thread(reader.close)


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        LOG.error("Interrupted. Continue with --resume; recent messages may be duplicated.")
        raise SystemExit(130)
    except Exception as exc:
        LOG.error("Stopped: %s", type(exc).__name__)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

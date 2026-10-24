#!/usr/bin/env python3
"""Reproducible bounded-memory IoT demo replay from FreeManufacturingQC100M.

- Scans a bounded number of Parquet rows by batch (no full DataFrame).
- Builds a seeded, stratified RESERVOIR of real source readings per sensor and
  source anomaly direction (normal / low / high), for a deliberately balanced demo.
- Replays those sensor values in real time through five VIRTUAL machines, in phases:
  normal -> anomaly -> recovery. Preserves SourceMachineId and SourceReadingId.
- All 5 virtual machines use ONE Azure IoT Hub registered DEVICE identity.
- Emits SourceAnomalyFlag only for validating the synthetic dataset labels;
  this is ground truth metadata, NOT an AI prediction.
- Uses the calibrated thresholds in SAquerry_calibrated.sql. This sampling
  intentionally changes natural class prevalence; NOT suitable for estimating
  production anomaly prevalence or production precision/recall.

Examples:
  python3 simulate_iot.py --dry-run --max-messages 60 --max-scan-rows 300000
  export IOTHUB_DEVICE_CONNECTION_STRING='NEW_ROTATED_DEVICE_CONNECTION_STRING'
  python3 simulate_iot.py --max-messages 1500 --machines 5 --interval 1
"""

import argparse
import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import random
import statistics
import time

DEFAULT_SOURCE = (
    'hf://datasets/ziadatalabs/FreeManufacturingQC100M/'
    'manufacturing_qc_readings_100M.parquet'
)
# Sensor names/units and exploratory boundaries match SQL exactly.
THRESHOLDS = {
    'temperature': ('F', 70.0, 80.0),
    'vibration': ('mm/s', 0.46, 0.54),
    'pressure': ('psi', 93.0, 107.0),
    'current_draw': ('A', 13.85, 16.15),
    'humidity': ('%', 41.5, 48.5),
}
COLUMNS = [
    'reading_id', 'machine_id', 'production_line',
    'sensor_type', 'sensor_value', 'unit', 'is_anomaly'
]
LOG = logging.getLogger('iot_simulator')


def positive_int(value):
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError('Must be >= 1')
    return n


def nonnegative_float(value):
    n = float(value)
    if not math.isfinite(n) or n < 0:
        raise argparse.ArgumentTypeError('Must be finite and >= 0')
    return n


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--source', default=os.getenv('PARQUET_SOURCE', DEFAULT_SOURCE),
                        help='hf:// Parquet URL or local file path')
    parser.add_argument('--machines', type=positive_int, default=5,
                        help='Number of virtual load-generator machines (default: 5)')
    parser.add_argument('--max-messages', type=positive_int, default=1500,
                        help='Maximum successful source readings selected (default: 1500)')
    parser.add_argument('--max-scan-rows', type=positive_int, default=2_000_000,
                        help='Maximum Parquet records examined in reservoir selection')
    parser.add_argument('--batch-size', type=positive_int, default=4096,
                        help='Bounded Parquet record-batch size')
    parser.add_argument('--interval', type=nonnegative_float, default=1.0,
                        help='Minimum time between messages per virtual machine, seconds')
    parser.add_argument('--retries', type=positive_int, default=3,
                        help='Send retries (includes first attempt)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Reproducible reservoir and ordering random seed')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show sample JSON; DO NOT send to IoT Hub')
    parser.add_argument('--allow-partial', action='store_true',
                        help='Proceed if dataset lacks one of the targeted strata')
    parser.add_argument('--summary', default='iot_run_summary.json',
                        help='Where to write JSON summary')
    parser.add_argument('--log-level', choices=['DEBUG','INFO','WARNING','ERROR'],
                        default='INFO')
    args = parser.parse_args(argv)
    if args.machines > 26:
        parser.error('--machines must be <= 26 for unique demo line letters')
    return args


def quotas(total):
    """Largest-remainder allocation: each sensor 1/5, each class normal/low/high 40/30/30."""
    weighted = [(sensor, cls, w / len(THRESHOLDS))
                for sensor in THRESHOLDS
                for cls, w in [('normal', 0.4), ('low', 0.3), ('high', 0.3)]]
    q = {(s, c): int(total * w) for s, c, w in weighted}
    spare = total - sum(q.values())
    ordered = sorted(weighted, key=lambda x: -(total*x[2] - int(total*x[2])))
    for s, c, _ in ordered[:spare]:
        q[(s, c)] += 1
    return q


def classify(sensor, unit, value, is_anomaly):
    """Keep genuine labelled normal values in-range, labelled anomalies outside bounds."""
    specification = THRESHOLDS.get(str(sensor).strip().lower())
    if specification is None or str(unit) != specification[0]:
        return None
    _, lower, upper = specification
    if value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    if not math.isfinite(value):
        return None
    # Null ground truth values never become healthy measurements.
    if is_anomaly is None:
        return None
    if bool(is_anomaly):
        return 'low' if value < lower else ('high' if value > upper else None)
    return 'normal' if lower <= value <= upper else None


def reservoir_sample(iter_rows, target_quotas, seed):
    """Pure-Python reservoir sampler; iterable can be synthetic test data or Parquet rows."""
    rng = random.Random(seed)
    chosen = {key: [] for key in target_quotas}
    eligible_seen = Counter()
    scanned = 0
    for record in iter_rows:
        scanned += 1
        sensor = str(record['sensor_type']).strip().lower()
        bucket = classify(sensor, record['unit'], record['sensor_value'], record['is_anomaly'])
        key = (sensor, bucket)
        limit = target_quotas.get(key, 0)
        if not limit:
            continue
        eligible_seen[key] += 1
        pool = chosen[key]
        item = {
            'SourceReadingId': str(record['reading_id']),
            'SourceMachineId': str(record['machine_id']),
            'SourceProductionLine': str(record['production_line']),
            'SensorType': sensor,
            'SensorValue': float(record['sensor_value']),
            'unit': str(record['unit']),
            'SourceAnomalyFlag': int(bool(record['is_anomaly'])),
            '_class': bucket,
        }
        if len(pool) < limit:
            pool.append(item)
        else:
            idx = rng.randrange(eligible_seen[key])
            if idx < limit:
                pool[idx] = item
    return chosen, eligible_seen, scanned


def parquet_rows(source, max_scan_rows, batch_size):
    """Yield bounded Arrow batches from local/Hugging Face Parquet; may range-read remotely."""
    import fsspec
    import pyarrow.parquet as pq
    scanned = 0
    with fsspec.open(source, 'rb').open() as stream:
        pf = pq.ParquetFile(stream)
        missing = sorted(set(COLUMNS).difference(pf.schema_arrow.names))
        if missing:
            raise ValueError(f'Missing Parquet columns: {missing}')
        LOG.info('Parquet total rows (metadata): %s', pf.metadata.num_rows)
        for batch in pf.iter_batches(batch_size=batch_size, columns=COLUMNS):
            cols = batch.to_pydict()
            for i in range(batch.num_rows):
                if scanned >= max_scan_rows:
                    return
                scanned += 1
                yield {name: cols[name][i] for name in COLUMNS}


def build_schedule(pools, machine_count, seed):
    """Distribute sampled real readings across virtual machines with 3 demo phases."""
    rng = random.Random(seed + 1)
    phases = [defaultdict(list) for _ in range(machine_count)]
    # phase 0=normal baseline, 1=anomaly, 2=normal recovery.
    # Distribute each group across VMs round-robin, then shuffle within phase.
    for sensor in THRESHOLDS:
        normals = list(pools[(sensor, 'normal')])
        rng.shuffle(normals)
        split = len(normals) // 2
        normal_pairs = [(0, r) for r in normals[:split]] + [(2, r) for r in normals[split:]]
        abnormal = [(1, r) for cls in ('low', 'high') for r in pools[(sensor, cls)]]
        rng.shuffle(abnormal)
        for phase, records in ((0, [r for p, r in normal_pairs if p == 0]),
                               (1, [r for p, r in abnormal]),
                               (2, [r for p, r in normal_pairs if p == 2])):
            for j, record in enumerate(records):
                slot = j % machine_count
                phases[slot][phase].append(record)
    schedules = []
    for slot in range(machine_count):
        output = []
        for phase in (0, 1, 2):
            values = phases[slot][phase]
            rng.shuffle(values)
            for record in values:
                entry = {k: v for k, v in record.items() if k != '_class'}
                entry['MachineId'] = f'SIM-{slot+1:02d}'
                entry['ProductionLine'] = f'Demo-Line-{chr(65+slot)}'
                entry['DemoPhase'] = ('normal', 'anomaly', 'recovery')[phase]
                output.append(entry)
        schedules.append(output)
    return schedules


def percentile(values, fraction):
    if not values:
        return None
    vals = sorted(values)
    x = (len(vals) - 1) * fraction
    low, high = math.floor(x), math.ceil(x)
    return round(vals[low] + (vals[high] - vals[low]) * (x - low), 2)


async def sender(slot, records, client, send_lock, args, stats):
    """Virtual machines share one SDK client/device identity but produce distinct MachineId."""
    for record in records:
        successful = False
        for attempt in range(1, args.retries+1):
            try:
                # With one IoT Hub identity and one SDK client, serialize SDK send for safety.
                # Timestamp is captured inside lock to keep event times consistent with send order.
                async with send_lock:
                    payload = dict(record)
                    payload['EventTimestamp'] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                    if args.dry_run:
                        if stats['printed'] < 5:
                            LOG.info('DRY RUN message: %s', json.dumps(payload, ensure_ascii=False))
                            stats['printed'] += 1
                        duration = 0.0
                    else:
                        from azure.iot.device import Message
                        wire = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
                        msg = Message(wire)
                        msg.content_encoding = 'utf-8'
                        msg.content_type = 'application/json'
                        t0 = time.perf_counter()
                        await client.send_message(msg)
                        duration = (time.perf_counter() - t0) * 1000
                stats['success'] += 1
                stats['latencies_ms'].append(duration)
                stats['by_machine'][record['MachineId']] += 1
                stats['by_sensor'][record['SensorType']] += 1
                stats['by_phase'][record['DemoPhase']] += 1
                if stats['success'] % 100 == 0:
                    LOG.info('Sent %s messages', stats['success'])
                successful = True
                break
            except Exception as ex:
                LOG.warning('Send failed for %s attempt %d/%d: %s',
                            record['MachineId'], attempt, args.retries, type(ex).__name__)
                if attempt < args.retries:
                    await asyncio.sleep(min(2 ** (attempt - 1), 8))
        if not successful:
            stats['failed'] += 1
        if args.interval:
            await asyncio.sleep(args.interval)


async def main(args):
    selection_start = time.perf_counter()
    q = quotas(args.max_messages)
    LOG.info('Sampling %s messages from <= %s rows: stratified by source anomaly and threshold direction',
             args.max_messages, args.max_scan_rows)
    # Sampling BEFORE connecting to IoT Hub avoids consuming paid connection uptime while reading.
    pools, eligible, scanned = await asyncio.to_thread(
        reservoir_sample, parquet_rows(args.source, args.max_scan_rows, args.batch_size),
        q, args.seed)
    shortfalls = {f'{s}/{c}': q[(s,c)] - len(pools[(s,c)])
                  for s,c in q if len(pools[(s,c)]) < q[(s,c)]}
    picked = sum(map(len, pools.values()))
    if shortfalls:
        LOG.warning('Selection shortages: %s', shortfalls)
        if not args.allow_partial:
            raise RuntimeError('Insufficient records for a balanced demo. Increase --max-scan-rows or add --allow-partial.')
    if picked == 0:
        raise RuntimeError('No readings match the sensor/unit/label thresholds')
    schedules = build_schedule(pools, args.machines, args.seed)
    LOG.info('Selected %d genuine dataset readings from %d examined in %.1fs; virtual machines: %d',
             picked, scanned, time.perf_counter() - selection_start, args.machines)
    stats = {
        'success': 0, 'failed': 0, 'latencies_ms': [],
        'by_machine': Counter(), 'by_sensor': Counter(), 'by_phase': Counter(), 'printed': 0,
    }
    client = None
    try:
        if not args.dry_run:
            connection = os.getenv('IOTHUB_DEVICE_CONNECTION_STRING')
            if not connection:
                raise RuntimeError('IOTHUB_DEVICE_CONNECTION_STRING is required (regenerate any previously exposed key).')
            from azure.iot.device.aio import IoTHubDeviceClient
            client = IoTHubDeviceClient.create_from_connection_string(connection)
            await client.connect()
            LOG.info('Connected to IoT Hub as ONE device identity; payload has %d virtual machines', args.machines)
        else:
            LOG.info('DRY RUN; no connection to Azure IoT Hub')
        t0 = time.perf_counter()
        lock = asyncio.Lock()
        await asyncio.gather(*[
            sender(i, rows, client, lock, args, stats)
            for i, rows in enumerate(schedules)
        ])
        sending_seconds = time.perf_counter() - t0
        summary = {
            'source': args.source,
            'dry_run': args.dry_run,
            'note': 'STRATIFIED DEMO, NOT NATURAL DATASET ANOMALY PREVALENCE',
            'scanned_rows': scanned,
            'selected_readings': picked,
            'selection_seconds': round(t0-selection_start,2),
            'send_seconds': round(sending_seconds,2),
            'success': stats['success'], 'failed': stats['failed'],
            'send_rate_messages_per_second': round(stats['success']/sending_seconds,2) if sending_seconds else None,
            'sdk_send_latency_ms_p50': None if args.dry_run else percentile(stats['latencies_ms'],0.50),
            'sdk_send_latency_ms_p95': None if args.dry_run else percentile(stats['latencies_ms'],0.95),
            'sdk_send_latency_ms_mean': None if args.dry_run or not stats['latencies_ms'] else round(statistics.mean(stats['latencies_ms']),2),
            'virtual_machines': args.machines,
            'sent_by_machine': dict(stats['by_machine']),
            'sent_by_sensor': dict(stats['by_sensor']),
            'sent_by_phase': dict(stats['by_phase']),
            'selected_by_sensor_and_class': {f'{s}/{c}': len(pools[(s,c)]) for s,c in q},
            'eligible_seen_by_sensor_and_class': {f'{s}/{c}': eligible[(s,c)] for s,c in q},
            'selection_shortfalls': shortfalls,
            'thresholds': {k: {'unit':x[0],'lower':x[1],'upper':x[2]} for k,x in THRESHOLDS.items()},
        }
        Path(args.summary).write_text(json.dumps(summary,indent=2,ensure_ascii=False), encoding='utf-8')
        LOG.info('Finished: %s', json.dumps({k:summary[k] for k in ('success','failed','send_seconds','sent_by_phase')}, ensure_ascii=False))
        LOG.info('Saved summary: %s', args.summary)
    finally:
        if client is not None:
            await client.shutdown()


if __name__ == '__main__':
    args = get_args()
    logging.basicConfig(level=getattr(logging,args.log_level), format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main(args))
    except (KeyboardInterrupt, Exception) as exc:
        LOG.error('Simulation stopped: %s', exc)
        raise SystemExit(1)

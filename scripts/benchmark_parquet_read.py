#!/usr/bin/env python3
"""Independent Parquet read + Arrow decode benchmark (NO Azure sends).

Measures in-memory Arrow logical bytes, NOT compressed file/network bytes.
The script reads and decodes a fixed number of rows per run.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import time

DEFAULT_SOURCE = (
    'hf://datasets/ziadatalabs/FreeManufacturingQC100M/'
    'manufacturing_qc_readings_100M.parquet'
)
SOURCE_COLUMNS = [
    'reading_id', 'machine_id', 'production_line', 'sensor_type',
    'sensor_value', 'unit', 'is_anomaly'
]


def run_once(source: str, limit: int, batch_size: int) -> dict:
    import fsspec
    import pyarrow.parquet as pq

    started = time.perf_counter()
    records = logical_bytes = batches = 0
    with fsspec.open(source, 'rb') as f:
        parquet = pq.ParquetFile(f)
        missing = set(SOURCE_COLUMNS) - set(parquet.schema_arrow.names)
        if missing:
            raise ValueError(f'Missing expected columns: {sorted(missing)}')
        for batch in parquet.iter_batches(batch_size=batch_size, columns=SOURCE_COLUMNS, use_threads=True):
            if records + batch.num_rows > limit:
                batch = batch.slice(0, limit - records)
            if not batch.num_rows:
                break
            # Match simulator's batch.to_pydict() decode step.
            decoded = batch.to_pydict()
            if not decoded:
                raise RuntimeError('No columns decoded')
            records += batch.num_rows
            logical_bytes += batch.nbytes
            batches += 1
            if records >= limit:
                break
    elapsed = time.perf_counter() - started
    return {
        'rows': records, 'batches': batches,
        'logical_arrow_bytes': logical_bytes,
        'elapsed_seconds': round(elapsed, 5),
        'rows_per_second': round(records / elapsed, 3) if elapsed else None,
        'logical_arrow_mib_per_second': round(logical_bytes / 1048576 / elapsed, 3) if elapsed else None,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', default=DEFAULT_SOURCE)
    p.add_argument('--limit', type=int, default=1000000)
    p.add_argument('--batch-size', type=int, default=4096)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--region', required=True, help='Label to associate with VM region, e.g. EastAsia')
    p.add_argument('--output', type=Path, default=Path('benchmark_parquet_result.json'))
    a = p.parse_args()
    if min(a.limit, a.batch_size, a.repeats) <= 0:
        p.error('--limit, --batch-size and --repeats must be positive')
    measurements = []
    for i in range(a.repeats):
        result = run_once(a.source, a.limit, a.batch_size)
        result['repeat'] = i + 1
        measurements.append(result)
        print(f"Run {i+1}/{a.repeats}: {result['rows']} rows, {result['logical_arrow_mib_per_second']} logical MiB/s")
    result = {
        'benchmark': 'parquet_arrow_decode',
        'region_label': a.region,
        'utc_finished': datetime.now(timezone.utc).isoformat(),
        'source': a.source, 'requested_rows': a.limit, 'batch_size': a.batch_size,
        'measurements': measurements,
        'mean_logical_arrow_mib_per_second': round(statistics.mean(x['logical_arrow_mib_per_second'] for x in measurements), 3),
        'mean_rows_per_second': round(statistics.mean(x['rows_per_second'] for x in measurements), 3),
        'metric_note': 'Arrow in-memory uncompressed bytes after selecting columns / read+decode elapsed; NOT physical Parquet or network throughput. Repeats may benefit from cache.',
    }
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Saved {a.output}')


if __name__ == '__main__':
    main()

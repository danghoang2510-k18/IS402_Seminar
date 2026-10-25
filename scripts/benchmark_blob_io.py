#!/usr/bin/env python3
"""Upload/download performance benchmark for Azure Blob Storage.

Uses a fresh uniquely named test blob per repeat, never overwrites other output.
Times only SDK upload and SDK download into a temporary local file.
Requires azure-storage-blob and optionally azure-identity; no IoT Hub used.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import tempfile
import time
import uuid


def create_local_file(path, size_bytes):
    # Generate bytes outside the timed network transfer; bounded RAM use.
    with open(path, 'wb') as f:
        chunk = b'cloud-iot-benchmark\x00' * 65536
        while f.tell() < size_bytes:
            remaining = size_bytes - f.tell()
            f.write(chunk[:min(remaining, len(chunk))])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--container', default='iot-alerts-data')
    p.add_argument('--region', required=True, help='Label only: e.g. EastAsia')
    p.add_argument('--mib', type=int, default=100)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--max-concurrency', type=int, default=2)
    p.add_argument('--account-url', help='Use DefaultAzureCredential if no connection-string env var')
    p.add_argument('--prefix', default='benchmarks/blob-io')
    p.add_argument('--cleanup', action='store_true', help='Delete ONLY blobs created by this benchmark')
    p.add_argument('--output', type=Path, default=Path('benchmark_blob_result.json'))
    args = p.parse_args()
    if min(args.mib, args.repeats, args.max_concurrency) < 1:
        p.error('--mib, --repeats, and --max-concurrency must be positive')
    from azure.storage.blob import BlobServiceClient
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if connection_string:
        service = BlobServiceClient.from_connection_string(connection_string)
        auth_type = 'AZURE_STORAGE_CONNECTION_STRING'
    elif args.account_url:
        from azure.identity import DefaultAzureCredential
        service = BlobServiceClient(account_url=args.account_url, credential=DefaultAzureCredential())
        auth_type = 'DefaultAzureCredential'
    else:
        p.error('Set AZURE_STORAGE_CONNECTION_STRING or supply --account-url with Azure login/identity')
    container = service.get_container_client(args.container)
    # Avoid creating containers implicitly. Existing private container expected.
    if not container.exists():
        raise RuntimeError(f'Container does not exist: {args.container}')
    byte_count = args.mib * 1048576
    run_id = uuid.uuid4().hex[:12]
    measurements = []
    with tempfile.TemporaryDirectory(prefix='iot_blob_bench_') as d:
        test_file = Path(d) / 'payload.bin'
        create_local_file(test_file, byte_count)
        for repeat in range(1, args.repeats + 1):
            blob_name = f'{args.prefix.rstrip("/")}/{args.region}/{run_id}/trial_{repeat}.bin'
            blob = container.get_blob_client(blob_name)
            with test_file.open('rb') as f:
                t0 = time.perf_counter()
                blob.upload_blob(f, overwrite=False, max_concurrency=args.max_concurrency)
                upload_seconds = time.perf_counter() - t0
            with tempfile.TemporaryFile(mode='w+b') as dst:
                t0 = time.perf_counter()
                blob.download_blob(max_concurrency=args.max_concurrency).readinto(dst)
                download_seconds = time.perf_counter() - t0
                downloaded = dst.tell()
            if downloaded != byte_count:
                raise RuntimeError(f'Downloaded bytes {downloaded}, expected {byte_count}')
            measurements.append({
                'repeat': repeat, 'bytes': byte_count,
                'blob_path': blob_name,
                'upload_seconds': round(upload_seconds, 5),
                'upload_mib_per_second': round(args.mib/upload_seconds, 3),
                'download_seconds': round(download_seconds, 5),
                'download_mib_per_second': round(args.mib/download_seconds, 3),
                'downloaded_bytes_verified': downloaded,
            })
            if args.cleanup:
                blob.delete_blob()
            print(f'Round {repeat}: upload {args.mib/upload_seconds:.2f} MiB/s | download {args.mib/download_seconds:.2f} MiB/s')
    result = {
        'benchmark': 'azure_blob_upload_download',
        'region_label': args.region,
        'utc_finished': datetime.now(timezone.utc).isoformat(),
        'container': args.container,
        'authentication_mode': auth_type,
        'blob_prefix': args.prefix,
        'bytes_per_run': byte_count,
        'max_concurrency': args.max_concurrency,
        'cleanup_test_blobs': args.cleanup,
        'measurements': measurements,
        'mean_upload_mib_per_second': round(statistics.mean(x['upload_mib_per_second'] for x in measurements), 3),
        'mean_download_mib_per_second': round(statistics.mean(x['download_mib_per_second'] for x in measurements), 3),
        'metric_note': 'SDK client-perspective throughput; includes network and service handling; excludes local file generation and optional deletion. Cache/network can affect repeats.',
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Saved {args.output}')


if __name__ == '__main__':
    main()

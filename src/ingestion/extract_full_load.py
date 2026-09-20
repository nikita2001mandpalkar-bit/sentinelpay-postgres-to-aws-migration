"""Extract an immutable full-load snapshot from the local legacy PostgreSQL source."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import psycopg
from dotenv import load_dotenv


TABLE_KEYS = {
    "merchants": "merchant_id",
    "customers": "customer_id",
    "payments": "source_record_id",
    "settlements": "source_record_id",
}


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    temporary_path.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv_batch(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    temporary_path = path.with_suffix(".part")
    with temporary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)
    temporary_path.replace(path)


def upload_batch(local_path: Path, bucket: str, object_key: str) -> str:
    boto3.client("s3").upload_file(str(local_path), bucket, object_key)
    return f"s3://{bucket}/{object_key}"


def extract_table(
    connection: psycopg.Connection,
    table: str,
    batch_size: int,
    run_directory: Path,
    run_id: str,
    extract_date: str,
    bucket: str | None,
    manifest: dict[str, Any],
) -> None:
    key_column = TABLE_KEYS[table]
    last_key: Any | None = None
    batch_number = 0
    total_rows = 0

    while True:
        where_clause = "" if last_key is None else f"WHERE {key_column} > %s"
        parameters = (batch_size,) if last_key is None else (last_key, batch_size)
        query = f"SELECT * FROM {table} {where_clause} ORDER BY {key_column} LIMIT %s"

        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]

        if not rows:
            break

        batch_number += 1
        total_rows += len(rows)
        last_key = rows[-1][columns.index(key_column)]
        batch_id = f"{table}-{batch_number:05d}"
        batch_directory = run_directory / table / f"extract_date={extract_date}" / f"batch_id={batch_id}"
        batch_directory.mkdir(parents=True, exist_ok=False)
        local_path = batch_directory / "records.csv"
        write_csv_batch(local_path, columns, rows)

        object_key = (
            f"bronze/legacy_postgres/{table}/extract_date={extract_date}/"
            f"run_id={run_id}/batch_id={batch_id}/records.csv"
        )
        s3_uri = upload_batch(local_path, bucket, object_key) if bucket else None
        manifest["batches"].append(
            {
                "batch_id": batch_id,
                "source_table": table,
                "record_count": len(rows),
                "last_source_key": str(last_key),
                "local_path": str(local_path),
                "s3_uri": s3_uri,
                "sha256": sha256_file(local_path),
                "status": "COMPLETED",
                "extracted_at": utc_now().isoformat(),
            }
        )
        manifest["totals"][table] = total_rows
        write_json_atomic(run_directory / "manifest.json", manifest)
        print(f"{table}: batch {batch_number} extracted ({total_rows:,} records total)")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Create a PostgreSQL-to-Bronze full-load snapshot.")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--run-id", default=None, help="Use a stable ID to make reruns fail safely instead of duplicating output.")
    parser.add_argument("--s3-bucket", default=None, help="Optional target bucket for Bronze uploads.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/output/bronze"))
    parser.add_argument("--tables", nargs="+", choices=sorted(TABLE_KEYS), default=sorted(TABLE_KEYS))
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("batch size must be positive")

    run_id = args.run_id or f"full-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run_directory = args.output_dir / f"run_id={run_id}"
    if run_directory.exists():
        raise FileExistsError(f"Run directory already exists: {run_directory}. Use a new run ID to avoid duplicate output.")

    run_directory.mkdir(parents=True)
    extract_date = utc_now().date().isoformat()
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "load_type": "FULL",
        "status": "RUNNING",
        "started_at": utc_now().isoformat(),
        "source": "local_postgresql",
        "s3_bucket": args.s3_bucket,
        "batch_size": args.batch_size,
        "batches": [],
        "totals": {},
    }
    write_json_atomic(run_directory / "manifest.json", manifest)

    dsn = os.getenv("SENTINELPAY_DB_DSN")
    if not dsn:
        raise ValueError("SENTINELPAY_DB_DSN is required. Copy .env.example to .env first.")
    try:
        with psycopg.connect(dsn) as connection:
            for table in args.tables:
                extract_table(
                    connection,
                    table,
                    args.batch_size,
                    run_directory,
                    run_id,
                    extract_date,
                    args.s3_bucket,
                    manifest,
                )
        manifest["status"] = "COMPLETED"
        manifest["completed_at"] = utc_now().isoformat()
        write_json_atomic(run_directory / "manifest.json", manifest)
        print(f"Full load completed: {run_directory}")
    except Exception:
        manifest["status"] = "FAILED"
        manifest["failed_at"] = utc_now().isoformat()
        write_json_atomic(run_directory / "manifest.json", manifest)
        raise


if __name__ == "__main__":
    main()

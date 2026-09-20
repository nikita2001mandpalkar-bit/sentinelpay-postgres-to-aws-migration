"""Extract only PostgreSQL rows newer than a persisted per-table watermark."""

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

import psycopg
from dotenv import load_dotenv


TABLE_KEYS = {
    "merchants": ("merchant_id", str),
    "customers": ("customer_id", str),
    "payments": ("source_record_id", int),
    "settlements": ("source_record_id", int),
}


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(path)


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    temporary_path = path.with_suffix(".part")
    with temporary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)
    temporary_path.replace(path)
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    return checksum


def latest_watermark(connection: psycopg.Connection, table: str) -> dict[str, str] | None:
    key_column, _ = TABLE_KEYS[table]
    query = f"""
        SELECT updated_at, {key_column}
        FROM {table}
        WHERE updated_at IS NOT NULL
        ORDER BY updated_at DESC, {key_column} DESC
        LIMIT 1
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    if row is None:
        return None
    return {"updated_at": row[0].isoformat(), "source_key": str(row[1])}


def initialize_state(connection: psycopg.Connection, state_path: Path, tables: list[str]) -> None:
    state = {
        "initialized_at": utc_now().isoformat(),
        "tables": {table: latest_watermark(connection, table) for table in tables},
    }
    write_json_atomic(state_path, state)
    print(f"Initialized incremental watermarks: {state_path}")


def extract_table(
    connection: psycopg.Connection,
    table: str,
    watermark: dict[str, str] | None,
    cutoff: datetime,
    batch_size: int,
    run_directory: Path,
    manifest: dict[str, Any],
) -> dict[str, str] | None:
    if watermark is None:
        return None

    key_column, key_type = TABLE_KEYS[table]
    last_updated_at = datetime.fromisoformat(watermark["updated_at"])
    last_key = key_type(watermark["source_key"])
    batch_number = 0
    total_rows = 0
    new_watermark = watermark

    while True:
        query = f"""
            SELECT *
            FROM {table}
            WHERE updated_at <= %s
              AND (updated_at > %s OR (updated_at = %s AND {key_column} > %s))
            ORDER BY updated_at, {key_column}
            LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(query, (cutoff, last_updated_at, last_updated_at, last_key, batch_size))
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]
        if not rows:
            break

        batch_number += 1
        total_rows += len(rows)
        last_updated_at = rows[-1][columns.index("updated_at")]
        last_key = rows[-1][columns.index(key_column)]
        new_watermark = {"updated_at": last_updated_at.isoformat(), "source_key": str(last_key)}

        batch_id = f"{table}-{batch_number:05d}"
        batch_directory = run_directory / table / f"batch_id={batch_id}"
        batch_directory.mkdir(parents=True, exist_ok=False)
        local_path = batch_directory / "records.csv"
        checksum = write_csv(local_path, columns, rows)
        manifest["batches"].append(
            {
                "batch_id": batch_id,
                "source_table": table,
                "record_count": len(rows),
                "watermark_after_batch": new_watermark,
                "local_path": str(local_path),
                "sha256": checksum,
                "status": "COMPLETED",
            }
        )
        manifest["totals"][table] = total_rows
        write_json_atomic(run_directory / "manifest.json", manifest)
        print(f"{table}: batch {batch_number} extracted ({total_rows:,} changed rows)")

    return new_watermark


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Create an incremental PostgreSQL-to-Bronze extract.")
    parser.add_argument("--initialize", action="store_true", help="Save current source watermarks without extracting rows.")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--state-file", type=Path, default=Path("data/state/incremental_watermarks.json"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/output/bronze"))
    parser.add_argument("--tables", nargs="+", choices=sorted(TABLE_KEYS), default=sorted(TABLE_KEYS))
    args = parser.parse_args()

    dsn = os.getenv("SENTINELPAY_DB_DSN")
    if not dsn:
        raise ValueError("SENTINELPAY_DB_DSN is required. Copy .env.example to .env first.")
    if args.batch_size <= 0:
        raise ValueError("batch size must be positive")

    args.state_file.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(dsn) as connection:
        if args.initialize:
            initialize_state(connection, args.state_file, args.tables)
            return

        if not args.state_file.exists():
            raise FileNotFoundError("Initialize watermarks before extracting changes: use --initialize.")
        state = json.loads(args.state_file.read_text(encoding="utf-8"))
        run_id = args.run_id or f"incremental-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
        run_directory = args.bronze_dir / f"run_id={run_id}"
        if run_directory.exists():
            raise FileExistsError(f"Run directory already exists: {run_directory}")
        run_directory.mkdir(parents=True)
        cutoff = utc_now()
        manifest: dict[str, Any] = {
            "run_id": run_id,
            "load_type": "INCREMENTAL",
            "status": "RUNNING",
            "started_at": cutoff.isoformat(),
            "cutoff_at": cutoff.isoformat(),
            "batches": [],
            "totals": {},
        }
        write_json_atomic(run_directory / "manifest.json", manifest)

        for table in args.tables:
            prior_watermark = state["tables"].get(table)
            new_watermark = extract_table(
                connection, table, prior_watermark, cutoff, args.batch_size, run_directory, manifest
            )
            if new_watermark:
                state["tables"][table] = new_watermark

        state["updated_at"] = utc_now().isoformat()
        write_json_atomic(args.state_file, state)
        manifest["status"] = "COMPLETED"
        manifest["completed_at"] = utc_now().isoformat()
        write_json_atomic(run_directory / "manifest.json", manifest)
        print(f"Incremental load completed: {run_directory}")


if __name__ == "__main__":
    main()

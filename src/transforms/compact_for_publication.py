"""Compact local Spark outputs into an Athena-friendly publication layout."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Dataset:
    source: Path
    target: str
    date_column: str | None = None
    category_column: str | None = None


def parquet_file_count(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))


def write_compacted(dataset: Dataset, destination_root: Path, spark: SparkSession) -> dict[str, object]:
    target_path = destination_root / dataset.target
    if target_path.exists():
        raise FileExistsError(f"Publication target already exists: {target_path}")

    dataframe = spark.read.parquet(str(dataset.source))
    input_files = parquet_file_count(dataset.source)
    record_count = dataframe.count()

    writer = dataframe.write.mode("errorifexists").option("compression", "snappy")
    if dataset.date_column:
        partitioned = (
            dataframe.withColumn("partition_year", F.year(dataset.date_column))
            .withColumn("partition_month", F.month(dataset.date_column))
            .repartition("partition_year", "partition_month")
        )
        writer = partitioned.write.mode("errorifexists").option("compression", "snappy").partitionBy(
            "partition_year", "partition_month"
        )
    elif dataset.category_column:
        writer = dataframe.repartition(dataset.category_column).write.mode("errorifexists").option(
            "compression", "snappy"
        ).partitionBy(dataset.category_column)
    else:
        writer = dataframe.coalesce(1).write.mode("errorifexists").option("compression", "snappy")

    writer.parquet(str(target_path))
    output_files = parquet_file_count(target_path)
    print(f"{dataset.target}: {record_count:,} rows, {input_files:,} files -> {output_files:,} files")
    return {
        "dataset": dataset.target,
        "records": record_count,
        "input_parquet_files": input_files,
        "output_parquet_files": output_files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Create compact, Athena-ready SentinelPay publication artifacts.")
    parser.add_argument("--silver-run-id", default="silver-incremental-001")
    parser.add_argument("--gold-run-id", default="gold-incremental-001")
    parser.add_argument("--scd2-run-id", default="gold-scd2-incremental-001")
    parser.add_argument("--publication-id", default="incremental-001")
    parser.add_argument("--output-dir", type=Path, default=Path("data/published"))
    args = parser.parse_args()

    silver_root = Path("data/output/silver") / f"run_id={args.silver_run_id}"
    gold_root = Path("data/output/gold") / f"run_id={args.gold_run_id}"
    scd2_root = Path("data/output/gold") / f"run_id={args.scd2_run_id}"
    publication_root = args.output_dir / f"publication_id={args.publication_id}"
    if publication_root.exists():
        raise FileExistsError(f"Publication already exists: {publication_root}. Use a new publication ID.")

    datasets = [
        Dataset(silver_root / "merchants", "silver/merchants"),
        Dataset(silver_root / "customers", "silver/customers"),
        Dataset(silver_root / "payments", "silver/payments", date_column="payment_date"),
        Dataset(silver_root / "settlements", "silver/settlements", date_column="settlement_date"),
        Dataset(gold_root / "payment_reconciliation", "gold/payment_reconciliation", category_column="reconciliation_status"),
        Dataset(gold_root / "merchant_daily_metrics", "gold/merchant_daily_metrics", date_column="business_date"),
        Dataset(gold_root / "merchant_scd2", "gold/merchant_scd2"),
        Dataset(gold_root / "reconciliation_summary", "gold/reconciliation_summary"),
        Dataset(scd2_root / "merchant_scd2", "gold/merchant_scd2_history"),
    ]
    missing_sources = [str(dataset.source) for dataset in datasets if not dataset.source.exists()]
    if missing_sources:
        raise FileNotFoundError("Missing source outputs:\n" + "\n".join(missing_sources))

    publication_root.mkdir(parents=True)
    spark = (
        SparkSession.builder.appName("sentinelpay-publication-compaction")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    try:
        report = [write_compacted(dataset, publication_root, spark) for dataset in datasets]
    finally:
        spark.stop()

    payload = {
        "publication_id": args.publication_id,
        "created_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "datasets": report,
    }
    (publication_root / "publication_report.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Publication compaction completed: {publication_root}")


if __name__ == "__main__":
    main()

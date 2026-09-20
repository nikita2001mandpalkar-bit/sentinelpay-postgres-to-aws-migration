"""Create a new merchant SCD Type 2 dimension from a Gold base and Bronze changes."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TRACKED_COLUMNS = ["merchant_name", "merchant_category", "city", "state", "risk_tier"]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply merchant changes as SCD Type 2 history.")
    parser.add_argument("--base-gold-run-id", required=True)
    parser.add_argument("--incremental-bronze-run-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--gold-dir", type=Path, default=Path("data/output/gold"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/output/bronze"))
    args = parser.parse_args()

    base_path = args.gold_dir / f"run_id={args.base_gold_run_id}" / "merchant_scd2"
    change_path = args.bronze_dir / f"run_id={args.incremental_bronze_run_id}" / "merchants" / "batch_id=*" / "records.csv"
    output_root = args.gold_dir / f"run_id={args.run_id}"
    output_path = output_root / "merchant_scd2"
    if not base_path.exists() or not change_path.parent.parent.exists():
        raise FileNotFoundError("The base Gold SCD2 table or incremental Bronze merchant batch does not exist.")
    if output_root.exists():
        raise FileExistsError("Output already exists. Use a new run ID to keep the SCD2 merge idempotent.")

    spark = SparkSession.builder.appName("SentinelPayMerchantSCD2").master("local[*]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        base = spark.read.parquet(str(base_path))
        changes = (
            spark.read.option("header", True).option("inferSchema", True).csv(str(change_path))
            .dropDuplicates(["merchant_id"])
            .alias("changes")
        )
        current = base.filter(F.col("is_current") == True).alias("current")

        difference_condition = None
        for column in TRACKED_COLUMNS:
            comparison = ~F.col(f"current.{column}").eqNullSafe(F.col(f"changes.{column}"))
            difference_condition = comparison if difference_condition is None else difference_condition | comparison

        changed = (
            current.join(changes, F.col("current.merchant_id") == F.col("changes.merchant_id"), "inner")
            .filter(difference_condition)
            .select(
                F.col("current.merchant_id").alias("merchant_id"),
                F.col("changes.updated_at").alias("new_effective_from"),
                *[F.col(f"changes.{column}").alias(column) for column in TRACKED_COLUMNS],
                F.col("changes.created_at").alias("created_at"),
                F.col("changes.updated_at").alias("updated_at"),
                F.col("changes.source_batch_id").alias("source_batch_id"),
                F.col("current.scd_version").alias("prior_scd_version"),
            )
        )

        close_joined = (
            base.alias("base")
            .join(changed.select("merchant_id", "new_effective_from").alias("changed"), "merchant_id", "left")
        )
        closed_base = close_joined.select(
            *[
                (
                    F.when(F.col("base.is_current") & F.col("new_effective_from").isNotNull(), F.col("new_effective_from"))
                    .otherwise(F.col("base.effective_to"))
                    .alias("effective_to")
                    if column == "effective_to"
                    else F.when(F.col("base.is_current") & F.col("new_effective_from").isNotNull(), F.lit(False))
                    .otherwise(F.col("base.is_current"))
                    .alias("is_current")
                    if column == "is_current"
                    else F.col(f"base.{column}").alias(column)
                )
                for column in base.columns
            ]
        )

        new_versions = changed.select(
            "merchant_id",
            *TRACKED_COLUMNS,
            "created_at",
            "updated_at",
            "source_batch_id",
            F.col("new_effective_from").alias("effective_from"),
            F.lit(None).cast("timestamp").alias("effective_to"),
            F.lit(True).alias("is_current"),
            (F.col("prior_scd_version") + 1).alias("scd_version"),
        )
        result = closed_base.unionByName(new_versions)

        report = {
            "run_id": args.run_id,
            "base_gold_run_id": args.base_gold_run_id,
            "incremental_bronze_run_id": args.incremental_bronze_run_id,
            "status": "COMPLETED",
            "processed_at": utc_now(),
            "changed_merchants": changed.count(),
            "total_scd2_rows": result.count(),
            "current_scd2_rows": result.filter(F.col("is_current") == True).count(),
            "closed_scd2_rows": result.filter(F.col("is_current") == False).count(),
        }

        result.write.mode("errorifexists").parquet(str(output_path))
        (output_root / "scd2_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

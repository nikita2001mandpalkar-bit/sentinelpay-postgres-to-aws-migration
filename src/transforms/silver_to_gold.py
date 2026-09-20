"""Create Gold reconciliation and merchant-history datasets from Silver Parquet."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def read_silver(spark: SparkSession, root: Path, table: str) -> DataFrame:
    return spark.read.parquet(str(root / table))


def write_parquet(dataframe: DataFrame, path: Path, partition_column: str | None = None) -> None:
    writer = dataframe.write.mode("errorifexists")
    if partition_column:
        writer.partitionBy(partition_column).parquet(str(path))
    else:
        writer.parquet(str(path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform Silver Parquet into Gold payment analytics.")
    parser.add_argument("--input-run-id", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--silver-dir", type=Path, default=Path("data/output/silver"))
    parser.add_argument("--gold-dir", type=Path, default=Path("data/output/gold"))
    args = parser.parse_args()

    run_id = args.run_id or f"gold-{args.input_run_id}"
    silver_root = args.silver_dir / f"run_id={args.input_run_id}"
    gold_root = args.gold_dir / f"run_id={run_id}"
    if not silver_root.exists():
        raise FileNotFoundError(f"Silver run does not exist: {silver_root}")
    if gold_root.exists():
        raise FileExistsError(f"Gold run already exists: {gold_root}")

    spark = (
        SparkSession.builder.appName("SentinelPaySilverToGold")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        merchants = read_silver(spark, silver_root, "merchants")
        payments = read_silver(spark, silver_root, "payments")
        settlements = read_silver(spark, silver_root, "settlements")

        settlement_totals = settlements.groupBy("payment_id").agg(
            F.round(F.sum("settlement_amount"), 2).alias("settled_amount"),
            F.count("settlement_id").alias("settlement_record_count"),
            F.max("settlement_timestamp").alias("latest_settlement_timestamp"),
        )
        payment_reconciliation = (
            payments.join(settlement_totals, "payment_id", "left")
            .withColumn("settled_amount", F.coalesce(F.col("settled_amount"), F.lit(0.0)))
            .withColumn("settlement_record_count", F.coalesce(F.col("settlement_record_count"), F.lit(0)))
            .withColumn("reconciliation_difference", F.round(F.col("payment_amount") - F.col("settled_amount"), 2))
            .withColumn(
                "reconciliation_status",
                F.when(F.col("settlement_record_count") == 0, "MISSING_SETTLEMENT")
                .when(F.abs(F.col("reconciliation_difference")) <= F.lit(0.01), "MATCHED")
                .when(F.col("reconciliation_difference") > 0, "UNDER_SETTLED")
                .otherwise("OVER_SETTLED"),
            )
            .withColumn("reconciled_at", F.lit(utc_now()))
        )

        merchant_daily_metrics = (
            payment_reconciliation.withColumn("business_date", F.to_date("payment_timestamp"))
            .groupBy("business_date", "merchant_id", "currency")
            .agg(
                F.count("payment_id").alias("payment_count"),
                F.round(F.sum("payment_amount"), 2).alias("gross_payment_amount"),
                F.round(F.sum("settled_amount"), 2).alias("settled_amount"),
                F.sum(F.when(F.col("reconciliation_status") == "MISSING_SETTLEMENT", 1).otherwise(0)).alias("missing_settlement_count"),
                F.sum(F.when(F.col("reconciliation_status") == "UNDER_SETTLED", 1).otherwise(0)).alias("under_settled_count"),
            )
            .withColumn("unsettled_amount", F.round(F.col("gross_payment_amount") - F.col("settled_amount"), 2))
        )

        # The initial snapshot creates the first version of each merchant dimension.
        merchant_scd2 = (
            merchants.withColumn("effective_from", F.coalesce(F.col("updated_at"), F.col("created_at")))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("scd_version", F.lit(1))
        )

        reconciliation_summary = (
            payment_reconciliation.groupBy("reconciliation_status")
            .agg(
                F.count("payment_id").alias("payment_count"),
                F.round(F.sum("payment_amount"), 2).alias("payment_amount"),
                F.round(F.sum("settled_amount"), 2).alias("settled_amount"),
                F.round(F.sum("reconciliation_difference"), 2).alias("net_difference"),
            )
            .withColumn("run_id", F.lit(run_id))
            .withColumn("generated_at", F.lit(utc_now()))
        )

        report = {
            "run_id": run_id,
            "input_run_id": args.input_run_id,
            "status": "COMPLETED",
            "processed_at": utc_now(),
            "gold_rows": {
                "payment_reconciliation": payment_reconciliation.count(),
                "merchant_daily_metrics": merchant_daily_metrics.count(),
                "merchant_scd2": merchant_scd2.count(),
            },
            "reconciliation_status_counts": {
                row["reconciliation_status"]: row["payment_count"]
                for row in reconciliation_summary.select("reconciliation_status", "payment_count").collect()
            },
        }

        write_parquet(payment_reconciliation, gold_root / "payment_reconciliation", "reconciliation_status")
        write_parquet(merchant_daily_metrics, gold_root / "merchant_daily_metrics", "business_date")
        write_parquet(merchant_scd2, gold_root / "merchant_scd2")
        write_parquet(reconciliation_summary, gold_root / "reconciliation_summary")

        (gold_root / "gold_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

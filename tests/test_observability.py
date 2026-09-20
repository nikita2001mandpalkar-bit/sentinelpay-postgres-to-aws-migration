from src.observability.publish_cloudwatch import build_metrics


def test_build_metrics_uses_aggregate_report_values() -> None:
    metrics = build_metrics(
        {
            "status": "COMPLETED",
            "valid_rows": {"merchants": 2, "payments": 8},
            "quarantined_rows": {"payments_invalid": 1, "payments_duplicates": 2},
        },
        {
            "status": "PASSED",
            "target": {"valid_payment_count": 8},
        },
    )

    values = {metric["MetricName"]: metric["Value"] for metric in metrics}

    assert values == {
        "PipelineSuccess": 1,
        "RecordsProcessed": 10,
        "ValidPayments": 8,
        "QuarantinedRecords": 3,
        "ReconciliationPassed": 1,
        "QualityChecksCompleted": 1,
    }


def test_build_metrics_marks_failed_reconciliation() -> None:
    metrics = build_metrics(
        {
            "status": "COMPLETED",
            "valid_rows": {"payments": 1},
            "quarantined_rows": {},
        },
        {
            "status": "FAILED",
            "target": {"valid_payment_count": 1},
        },
    )

    values = {metric["MetricName"]: metric["Value"] for metric in metrics}
    assert values["PipelineSuccess"] == 0
    assert values["ReconciliationPassed"] == 0

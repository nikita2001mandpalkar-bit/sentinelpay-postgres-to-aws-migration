from pathlib import Path

from src.deployment.upload_lakehouse import planned_uploads


def test_planned_uploads_preserves_published_prefix(tmp_path: Path) -> None:
    published_root = tmp_path / "published"
    source = published_root / "publication_id=test" / "silver" / "payments"
    source.mkdir(parents=True)
    parquet_file = source / "part-00000.parquet"
    parquet_file.write_bytes(b"parquet-test")

    uploads = planned_uploads([(source, published_root)], "sentinelpay-migration")

    assert uploads == [
        (
            parquet_file,
            "sentinelpay-migration/published/publication_id=test/silver/payments/part-00000.parquet",
        )
    ]

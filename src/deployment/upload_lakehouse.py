"""Upload selected local lakehouse artifacts to the SentinelPay S3 prefix.

The script defaults to a dry run. Pass --execute only after reviewing the
planned object list, so local test outputs are never uploaded by accident.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


SOURCE_ROOTS = (Path("data/output"), Path("data/published"))
DEFAULT_PREFIX = "sentinelpay-migration"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def resolve_source(source: Path, source_roots: tuple[Path, ...]) -> tuple[Path, Path]:
    resolved_source = source.resolve()
    matching_root = next(
        (root for root in source_roots if resolved_source.is_relative_to(root)),
        None,
    )
    if matching_root is None:
        allowed_roots = ", ".join(str(root) for root in source_roots)
        raise ValueError(f"Source must be inside one of: {allowed_roots}. Received: {source}")
    if not resolved_source.is_dir():
        raise FileNotFoundError(f"Source directory does not exist: {source}")
    return resolved_source, matching_root


def planned_uploads(sources: list[tuple[Path, Path]], prefix: str) -> list[tuple[Path, str]]:
    uploads: list[tuple[Path, str]] = []
    for source, source_root in sources:
        for file_path in sorted(path for path in source.rglob("*") if path.is_file()):
            relative_path = file_path.relative_to(source_root).as_posix()
            if source_root.name == "published":
                relative_path = f"published/{relative_path}"
            object_key = f"{prefix.strip('/')}/{relative_path}"
            uploads.append((file_path, object_key))
    return uploads


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely upload selected SentinelPay lakehouse artifacts to S3.")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name.")
    parser.add_argument("--source", type=Path, action="append", required=True, help="Directory below data/output or data/published. Repeat for each artifact run.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"S3 project prefix. Default: {DEFAULT_PREFIX}")
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE", "sentinelpay-deployer"), help="AWS CLI profile to use.")
    parser.add_argument("--execute", action="store_true", help="Perform uploads. Omit this flag for a dry run.")
    args = parser.parse_args()

    source_roots = tuple(root.resolve() for root in SOURCE_ROOTS)
    sources = [resolve_source(source, source_roots) for source in args.source]
    uploads = planned_uploads(sources, args.prefix)
    total_bytes = sum(path.stat().st_size for path, _ in uploads)

    mode = "UPLOAD" if args.execute else "DRY RUN"
    print(f"{mode}: {len(uploads):,} files, {total_bytes / 1024 / 1024:.2f} MiB")
    for local_path, object_key in uploads:
        print(f"{local_path} -> s3://{args.bucket}/{object_key}")

    if not args.execute:
        print("No files uploaded. Review this list, then rerun with --execute.")
        return

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")
    try:
        for number, (local_path, object_key) in enumerate(uploads, start=1):
            s3.upload_file(str(local_path), args.bucket, object_key)
            print(f"Uploaded {number:,}/{len(uploads):,}: s3://{args.bucket}/{object_key}")
    except (BotoCoreError, ClientError) as error:
        raise RuntimeError("S3 upload failed. No delete or overwrite cleanup was attempted.") from error

    deployment_manifest = {
        "deployed_at": utc_now(),
        "bucket": args.bucket,
        "prefix": args.prefix.strip("/"),
        "profile": args.profile,
        "file_count": len(uploads),
        "total_bytes": total_bytes,
        "sources": [str(source.relative_to(source_root)) for source, source_root in sources],
    }
    manifest_key = f"{args.prefix.strip('/')}/audit/deployments/deployment-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    s3.put_object(Bucket=args.bucket, Key=manifest_key, Body=json.dumps(deployment_manifest, indent=2).encode("utf-8"), ContentType="application/json")
    print(f"Deployment manifest: s3://{args.bucket}/{manifest_key}")


if __name__ == "__main__":
    main()

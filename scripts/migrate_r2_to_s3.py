#!/usr/bin/env python3
"""
Migrate new/missing objects from Cloudflare R2 to AWS S3.
Skips objects that already exist in S3 (checked by key presence).
"""

import os
import sys
import logging
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def build_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CF_R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def build_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def object_exists_in_s3(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def list_all_objects(client, bucket: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            yield obj


def migrate(dry_run: bool = False):
    r2_bucket = os.environ["CF_R2_BUCKET_NAME"]
    s3_bucket = os.environ["S3_BUCKET_NAME"]

    r2 = build_r2_client()
    s3 = build_s3_client()

    copied = skipped = failed = 0

    log.info("Starting R2 → S3 migration | R2: %s → S3: %s", r2_bucket, s3_bucket)

    for obj in list_all_objects(r2, r2_bucket):
        key = obj["Key"]

        if object_exists_in_s3(s3, s3_bucket, key):
            log.debug("SKIP (exists): %s", key)
            skipped += 1
            continue

        if dry_run:
            log.info("DRY-RUN would copy: %s", key)
            copied += 1
            continue

        try:
            response = r2.get_object(Bucket=r2_bucket, Key=key)
            body = response["Body"]
            content_type = response.get("ContentType", "application/octet-stream")

            s3.upload_fileobj(
                body,
                s3_bucket,
                key,
                ExtraArgs={"ContentType": content_type},
            )
            log.info("COPIED: %s", key)
            copied += 1
        except Exception as exc:
            log.error("FAILED: %s — %s", key, exc)
            failed += 1

    log.info(
        "Migration complete | copied=%d  skipped=%d  failed=%d",
        copied,
        skipped,
        failed,
    )

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    migrate(dry_run=dry_run)

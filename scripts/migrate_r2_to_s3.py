#!/usr/bin/env python3
"""
Migrate new/missing objects from Cloudflare R2 to AWS S3.
Skips objects that already exist in S3 (checked by key presence).
"""

import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

TRANSFER_CONFIG = TransferConfig(use_threads=False)


def build_r2_client():
    max_pool = int(os.environ.get("MAX_POOL_CONNECTIONS", "64"))
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CF_R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            max_pool_connections=max_pool,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )


def build_s3_client():
    max_pool = int(os.environ.get("MAX_POOL_CONNECTIONS", "64"))
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(
            max_pool_connections=max_pool,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )


def list_all_objects(client, bucket: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            yield obj


def list_all_keys(client, bucket: str) -> set:
    return {obj["Key"] for obj in list_all_objects(client, bucket)}


def copy_one_object(r2, s3, r2_bucket: str, s3_bucket: str, key: str):
    body = None
    try:
        response = r2.get_object(Bucket=r2_bucket, Key=key)
        body = response["Body"]
        content_type = response.get("ContentType", "application/octet-stream")

        s3.upload_fileobj(
            body,
            s3_bucket,
            key,
            ExtraArgs={"ContentType": content_type},
            Config=TRANSFER_CONFIG,
        )
        return key, None
    except Exception as exc:
        return key, exc
    finally:
        if body is not None:
            body.close()


def migrate(dry_run: bool = False):
    r2_bucket = os.environ["CF_R2_BUCKET_NAME"]
    s3_bucket = os.environ["S3_BUCKET_NAME"]
    max_workers = int(os.environ.get("MIGRATION_WORKERS", "32"))

    r2 = build_r2_client()
    s3 = build_s3_client()

    copied = skipped = failed = 0

    log.info("Starting R2 → S3 migration | R2: %s → S3: %s", r2_bucket, s3_bucket)

    log.info("Loading key index from R2...")
    r2_keys = list_all_keys(r2, r2_bucket)
    log.info("R2 objects found: %d", len(r2_keys))

    log.info("Loading key index from S3...")
    s3_keys = list_all_keys(s3, s3_bucket)
    log.info("S3 objects found: %d", len(s3_keys))

    missing_keys = [key for key in r2_keys if key not in s3_keys]
    skipped = len(r2_keys) - len(missing_keys)

    log.info(
        "Objects to copy: %d | Already present (skipped): %d",
        len(missing_keys),
        skipped,
    )

    if dry_run:
        for key in missing_keys:
            log.info("DRY-RUN would copy: %s", key)
        copied = len(missing_keys)
    elif missing_keys:
        log.info("Starting concurrent copy with %d workers", max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(copy_one_object, r2, s3, r2_bucket, s3_bucket, key): key
                for key in missing_keys
            }
            for index, future in enumerate(as_completed(futures), start=1):
                key, error = future.result()
                if error is None:
                    copied += 1
                    log.info("COPIED: %s", key)
                else:
                    failed += 1
                    log.error("FAILED: %s — %s", key, error)

                if index % 100 == 0 or index == len(missing_keys):
                    log.info(
                        "Progress: %d/%d | copied=%d failed=%d",
                        index,
                        len(missing_keys),
                        copied,
                        failed,
                    )

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

import csv
import os
import random
import string
import tempfile
from enum import Enum

import boto3

from ccget.consts import AWS_REGION


class S3StorageClass(Enum):
    STANDARD = 1
    STANDARD_IA = 2
    ONEZONE_IA = 3
    GLACIER = 4
    INTELLIGENT_TIERING = 5
    DEEP_ARCHIVE = 6
    GLACIER_IR = 7


def job_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=8))


def manifest_arn(s3_manifest_key: str, bucket_name: str) -> str:
    return f"arn:aws:s3:::{bucket_name}/{s3_manifest_key}"


def bucket_arn(bucket_name: str) -> str:
    return f"arn:aws:s3:::{bucket_name}"


def object_etag(bucket: str, key: str) -> str:
    s3 = boto3.client("s3")
    return s3.head_object(Bucket=bucket, Key=key)["ETag"]


def get_role_arn(role_name: str) -> str:
    iam = boto3.client("iam")
    res = iam.get_role(RoleName=role_name)

    return res["Role"]["Arn"]


def create_job_manifest_on_s3(
    keys: list[str],
    manifest_prefix: str,
    source_bucket_name: str,
    dest_bucket_name: str,
) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # We'll write the S3 expected format manifest file here
    with tempfile.TemporaryDirectory() as tmpdir:
        manifest_fn = f"manifest-{job_suffix()}.csv"
        s3_manifest_key = f"{manifest_prefix}/{manifest_fn}"
        manifest_fullpath = os.path.join(tmpdir, manifest_fn)

        with open(manifest_fullpath, "w", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["bucket", "key"])
            writer.writerows(
                [{"bucket": source_bucket_name, "key": key} for key in keys]
            )

        s3.upload_file(manifest_fullpath, dest_bucket_name, s3_manifest_key)

        return s3_manifest_key

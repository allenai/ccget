import csv
import os
import random
import string
import tempfile
from enum import Enum

import boto3
from botocore.exceptions import ClientError
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


def check_s3_object_exists(bucket_name: str, object_key: str, s3_client=None) -> bool:
    """
    Check if an object exists in an S3 bucket

    :param bucket_name: String name of the bucket
    :param object_key: String key of the object
    :return: True if object exists, False if not
    """
    s3_client = s3_client or boto3.client('s3')

    try:
        # Use head_object which only retrieves metadata and is more efficient
        # than trying to download the object
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        # If error code is 404, the object does not exist
        if e.response['Error']['Code'] == '404':
            return False
        # For any other error, raise it
        else:
            raise

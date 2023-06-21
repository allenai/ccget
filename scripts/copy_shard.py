"""
A shard includes tens of thousands of .warc.gz files that are the original file archives
in WARC format. This script copies these files from a source bucket -- like Common Crawl
-- to a destination bucket or locally.

Common Crawl is heavily throttled on S3 so it's recommended to never copy from the
source CC buckets to anywhere other than another bucket within us-east-1 on S3. Use this
secondary bucket to copy files locally as needed.

@rauthur
"""
import argparse
import csv
import gzip
import os
import random
import string
import tempfile
from dataclasses import dataclass
from enum import Enum
from functools import cache
from time import sleep

import boto3

from ccget.consts import AWS_REGION, CC_BUCKET, account_id
from ccget.paths import warc_paths_local_fn
from ccget.shards import list_shards


class S3StorageClass(Enum):
    STANDARD = 1
    STANDARD_IA = 2
    ONEZONE_IA = 3
    GLACIER = 4
    INTELLIGENT_TIERING = 5
    DEEP_ARCHIVE = 6
    GLACIER_IR = 7


@dataclass
class Config:
    shard_id: str
    n: int
    cache_dir: str
    dest_bucket_name: str
    manifest_prefix: str
    reports_prefix: str
    role_arn: str
    storage_class: S3StorageClass


def _job_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=8))


def _manifest_arn(s3_manifest_key: str, config: Config) -> str:
    return f"arn:aws:s3::{account_id()}:{config.dest_bucket_name}/{s3_manifest_key}"


def _bucket_arn(config: Config) -> str:
    return f"arn:aws:s3::{account_id()}:{config.dest_bucket_name}"


def _object_etag(bucket: str, key: str) -> str:
    s3 = boto3.client("s3")
    return s3.head_object(Bucket=bucket, Key=key)["ETag"]


def _get_role_arn(role_name: str) -> str:
    iam = boto3.client("iam")
    res = iam.get_role(RoleName=role_name)

    return res["Role"]["Arn"]


def _verify_bucket_region(bucket: str) -> None:
    s3 = boto3.client("s3")
    res = s3.get_bucket_location(Bucket=bucket)

    # us-east-1 is None for LocationConstraint!!!
    if res["LocationConstraint"] is not None:
        raise RuntimeError(
            "To avoid cross-region data transfer destination bucket must be in "
            f"{AWS_REGION}! Found {res['LocationConstraint']}"
        )


def _verify_deep_archive_when_all(n: int, storage_class: str):
    if n == 0 and storage_class != S3StorageClass.DEEP_ARCHIVE.name:
        raise RuntimeError(
            "Cannot archive ALL common crawl files to non-Deep Archive"
            f" storage class {storage_class}"
        )


def _verify_deep_archive_when_many(n: int, storage_class: str):
    if n > 1000 and storage_class != S3StorageClass.DEEP_ARCHIVE.name:
        raise RuntimeError("Estimated archive size is over 1 TB to non-Deep Archive!")


def _verify_shard(shard: str):
    all_shards = set([s.id for s in list_shards()])

    if shard not in all_shards:
        raise RuntimeError(f"Unknown shard: {shard}")


def create_job_manifest_on_s3(keys: list[str], config: Config) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # We'll write the S3 expected format manifest file here
    with tempfile.TemporaryDirectory() as tmpdir:
        manifest_fn = f"manifest-{_job_suffix()}.csv"
        s3_manifest_key = f"{config.manifest_prefix}/{manifest_fn}"
        manifest_fullpath = os.path.join(tmpdir, manifest_fn)

        with open(manifest_fullpath, "w", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["bucket", "key"])
            writer.writerows([{"bucket": CC_BUCKET, "key": key} for key in keys])

        s3.upload_file(manifest_fullpath, config.dest_bucket_name, s3_manifest_key)

        return s3_manifest_key


def create_batch_job(s3_manifest_key: str, config: Config) -> str:
    s3control = boto3.client("s3control", region_name=AWS_REGION)

    response = s3control.create_job(
        AccountId=account_id(),
        ConfirmationRequired=False,
        Operation={
            "S3PutObjectCopy": {
                "TargetResource": _bucket_arn(config),
                "CannedAccessControlList": "private",
                "StorageClass": config.storage_class.name,
            }
        },
        Report={
            "Bucket": config.dest_bucket_name,
            "Format": "Report_CSV_20180820",
            "Enabled": True,
            "Prefix": config.reports_prefix,
            "ReportScope": "FailedTasks",
        },
        ClientRequestToken=s3_manifest_key,
        Manifest={
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": _manifest_arn(s3_manifest_key, config),
                "ETag": _object_etag(config.dest_bucket_name, s3_manifest_key),
            },
        },
        Priority=1,  # Higher is more urgent
        RoleArn=config.role_arn,
    )

    print("Created Batch Copy JobId: ", response["JobId"])

    return response["JobId"]


def poll_job_status(s3control, job_id: str) -> bool:
    response = s3control.describe_job(AccountId=account_id(), JobId=job_id)

    summary = response["Job"]["ProgressSummary"]
    succeeded = summary["NumberOfTasksSucceeded"]
    failed = summary["NumberOfTasksFailed"]
    total = summary["TotalNumberOfTasks"]
    progress = ((succeeded + failed) / total) * 100
    status = response["Job"]["Status"]

    print(
        f"Total: {total}; Succeeded: {succeeded}; "
        f"Failed: {failed}; Progress%: {progress:.2f} "
        f"Status: {status}"
    )

    return status != "Active"


def main(config: Config):
    with gzip.open(warc_paths_local_fn(config.shard_id, config.cache_dir)) as f:
        keys = [k for k in f.read().decode("utf-8").splitlines()]

    if config.n > 0:
        # Here we will set a seed that is not shared with other RNG states to allow the
        # job suffix to be different while the sampled files are the same
        rng = random.Random(102)
        keys = rng.sample(keys, config.n)

    s3_manifest_key = create_job_manifest_on_s3(keys, config)
    print(f"Created manifest: s3://{config.dest_bucket_name}/{s3_manifest_key}")

    job_id = create_batch_job(s3_manifest_key, config)

    print("Polling job status (safe to CTRL-C/KILL this process now)...")
    s3control = boto3.client("s3control", region_name=AWS_REGION)
    while not poll_job_status(s3control, job_id):
        sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--shard",
        type=str,
        required=True,
        help="The shard to archive",
    )
    parser.add_argument(
        "-n",
        type=int,
        required=True,
        help="Number of ~1GB WARC files to archive (0 for all). Randomly sampled.",
    )
    parser.add_argument(
        "-c",
        "--cache-dir",
        required=True,
        help="Local location of warc.paths.gz files",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        type=str,
        required=True,
        help="Destination bucket for the archive operation",
    )
    parser.add_argument(
        "-m",
        "--manifest-prefix",
        type=str,
        required=False,
        default="batch-copy-manifests",
        help="Key prefix in destination bucket for batch job manifest files",
    )
    parser.add_argument(
        "-r",
        "--reports-prefix",
        type=str,
        required=False,
        default="batch-copy-reports",
        help="Key prefix in destination bucket for batch job report files",
    )
    parser.add_argument(
        "--role-name",
        type=str,
        required=True,
        help="Role name for the batch job execution role",
    )
    parser.add_argument(
        "--storage-class",
        required=True,
        choices=[c.name for c in list(S3StorageClass)],
        help="Storage class for S3",
    )

    args = parser.parse_args()

    _verify_bucket_region(args.bucket)
    _verify_deep_archive_when_all(args.n, args.storage_class)
    _verify_deep_archive_when_many(args.n, args.storage_class)
    _verify_shard(args.shard)

    config = Config(
        shard_id=args.shard,
        n=args.n,
        cache_dir=args.cache_dir,
        dest_bucket_name=args.bucket,
        manifest_prefix=args.manifest_prefix,
        reports_prefix=args.reports_prefix,
        role_arn=_get_role_arn(args.role_name),
        storage_class=S3StorageClass[args.storage_class],
    )

    main(config)

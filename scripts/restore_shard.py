"""
Restore a shard to active S3 storage (standard tier) for a fixed number of days.

@rauthur
"""
import argparse
import gzip
import random
from dataclasses import dataclass
from math import ceil

import boto3

from ccget.aws import (
    bucket_arn,
    create_job_manifest_on_s3,
    get_role_arn,
    manifest_arn,
    object_etag,
)
from ccget.consts import AWS_REGION, account_id
from ccget.paths import warc_paths_local_fn


@dataclass
class Config:
    shard_id: str
    n: int
    cache_dir: str
    dest_bucket_name: str
    manifest_prefix: str
    reports_prefix: str
    restore_days: int
    role_arn: str


def _create_batch_job(s3_manifest_key: str, config: Config) -> str:
    s3control = boto3.client("s3control", region_name=AWS_REGION)

    response = s3control.create_job(
        AccountId=account_id(),
        ConfirmationRequired=True,
        Operation={
            "S3InitiateRestoreObject": {
                "ExpirationInDays": config.restore_days,
                "GlacierJobTier": "BULK",
            },
        },
        Report={
            "Bucket": bucket_arn(config.dest_bucket_name),
            "Format": "Report_CSV_20180820",
            "Enabled": True,
            "Prefix": config.reports_prefix,
            "ReportScope": "AllTasks",
        },
        Manifest={
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": manifest_arn(s3_manifest_key, config.dest_bucket_name),
                "ETag": object_etag(config.dest_bucket_name, s3_manifest_key),
            },
        },
        Priority=10,  # Higher is more urgent
        RoleArn=config.role_arn,
    )

    print("Created Batch Restore Initiate JobId: ", response["JobId"])

    return response["JobId"]


def _restore_estimate(num_keys: int, config: Config):
    """A key is assumed to be approximately 1 GB of data"""

    # 1,000 requests x 0.000025 USD = 0.025 USD (Cost for Restore requests (Bulk))
    # 1,024 GB per month x 0.0025 USD = 2.56 USD (Cost for Retrieval (Bulk))

    s3_gb_cost_per_day = 0.02 / 30
    s3_storage_cost = config.restore_days * s3_gb_cost_per_day * num_keys

    num_1k_requests = ceil(num_keys / 1000)

    return num_1k_requests * 0.000025 + num_keys * 0.0025 + s3_storage_cost


def main(config: Config):
    with gzip.open(warc_paths_local_fn(config.shard_id, config.cache_dir)) as f:
        keys = [k for k in f.read().decode("utf-8").splitlines()]

    if config.n > 0:
        # Here we will set a seed that is not shared with other RNG states to allow the
        # job suffix to be different while the sampled files are the same
        rng = random.Random(102)
        keys = rng.sample(keys, config.n)

    cost_estimate = _restore_estimate(len(keys), config)
    print(f"\nThis restore job is estimated to cost ${cost_estimate:.2f}")

    s3_manifest_key = create_job_manifest_on_s3(
        keys,
        config.manifest_prefix,
        config.dest_bucket_name,
    )
    print(f"Created manifest: s3://{config.dest_bucket_name}/{s3_manifest_key}")

    job_id = _create_batch_job(s3_manifest_key, config)
    print(f"Job ID: {job_id}")
    print("Please confirm and start the job from the AWS S3 console")


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
        default="batch-restore-manifests",
        help="Key prefix in destination bucket for batch job manifest files",
    )
    parser.add_argument(
        "-r",
        "--reports-prefix",
        type=str,
        required=False,
        default="batch-restore-reports",
        help="Key prefix in destination bucket for batch job report files",
    )
    parser.add_argument(
        "-d",
        "--restore-days",
        type=int,
        required=True,
        help="Number of days to keep restored copies in S3 standard",
    )
    parser.add_argument(
        "--role-name",
        type=str,
        required=True,
        help="Role name for the batch job execution role",
    )

    args = parser.parse_args()

    config = Config(
        shard_id=args.shard,
        n=args.n,
        cache_dir=args.cache_dir,
        dest_bucket_name=args.bucket,
        manifest_prefix=args.manifest_prefix,
        reports_prefix=args.reports_prefix,
        restore_days=args.restore_days,
        role_arn=get_role_arn(args.role_name),
    )

    main(config)

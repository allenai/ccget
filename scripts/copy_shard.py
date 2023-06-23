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
import gzip
import random
from dataclasses import dataclass

import boto3

from ccget.aws import (
    S3StorageClass,
    bucket_arn,
    create_job_manifest_on_s3,
    get_role_arn,
    manifest_arn,
    object_etag,
)
from ccget.consts import AWS_REGION, CC_BUCKET, account_id
from ccget.paths import warc_paths_local_fn
from ccget.shards import list_shards


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


def _create_batch_job(s3_manifest_key: str, config: Config) -> str:
    s3control = boto3.client("s3control", region_name=AWS_REGION)

    response = s3control.create_job(
        AccountId=account_id(),
        ConfirmationRequired=True,
        Operation={
            "S3PutObjectCopy": {
                "TargetResource": bucket_arn(config.dest_bucket_name),
                "MetadataDirective": "REPLACE",
                "NewObjectMetadata": {"RequesterCharged": False},
                "NewObjectTagging": [],
                "CannedAccessControlList": "private",
                "StorageClass": config.storage_class.name,
                "RequesterPays": False,
                "BucketKeyEnabled": False,
            }
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

    print("Created Batch Copy JobId: ", response["JobId"])

    return response["JobId"]


def main(config: Config):
    with gzip.open(warc_paths_local_fn(config.shard_id, config.cache_dir)) as f:
        keys = [k for k in f.read().decode("utf-8").splitlines()]

    if config.n > 0:
        # Here we will set a seed that is not shared with other RNG states to allow the
        # job suffix to be different while the sampled files are the same
        rng = random.Random(102)
        keys = rng.sample(keys, config.n)

    s3_manifest_key = create_job_manifest_on_s3(
        keys,
        config.manifest_prefix,
        CC_BUCKET,
        config.dest_bucket_name,
    )
    print(f"Created manifest: s3://{config.dest_bucket_name}/{s3_manifest_key}")

    _create_batch_job(s3_manifest_key, config)
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
        role_arn=get_role_arn(args.role_name),
        storage_class=S3StorageClass[args.storage_class],
    )

    main(config)

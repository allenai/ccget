"""
Replicate warc.paths.gz files to a location on S3. Then, download these files locally.

warc.paths.gz files are small (< 200KB) and give S3 keys for all WARC files in a shard.

@rauthur
"""

import argparse
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import os

import boto3

from ccget.shards import fetch_warc_paths, list_shards, replicate_warc_paths
from ccget.aws import check_s3_object_exists

@dataclass
class Config:
    shard_ids: list[str]
    cache_dir: Optional[str]
    ignore_cache: bool
    dest_bucket: str


def process_shard(s3, shard_id: str, config: Config):
    # First check if already replicated and copy if not
    replicate_warc_paths(
        s3,
        shard_id=shard_id,
        dest_bucket_name=config.dest_bucket,
        ignore_cache=config.ignore_cache,
    )

    # We only download if a local cache dir is specified
    if config.cache_dir:
        # Then check local cache and download if not in cache
        fetch_warc_paths(
            s3,
            shard_id,
            config.cache_dir,
            ignore_cache=config.ignore_cache,
            bucket_name=config.dest_bucket,
        )


def process_cc_news(config: Config, commoncrawl_bucket: str = "commoncrawl") -> None:
    s3_client = boto3.client("s3")

    current_month = (now := datetime.now()).month
    current_year = now.year


    for year in range(2016, current_year + 1):
        if year == 2016 and current_month < 8:
            # commoncrawl has no CC-NEWS data for 2016 before August
            continue

        for month in range(1, 13):
            # if current year and current month, break.
            # we don't wanna replicate the current month cuz it could be incomplete
            if year == current_year and month >= current_month:
                break

            # check if shard exists
            shard_id = f"CC-NEWS/{year}/{month:02d}"
            paths_manifest = f"crawl-data/{shard_id}/warc.paths.gz"

            # use boto3 to check if object exists
            if not check_s3_object_exists(commoncrawl_bucket, paths_manifest, s3_client):
                print(f"Shard {shard_id} not found, skipping")
                continue

            assert config.cache_dir is not None
            os.makedirs(os.path.join(config.cache_dir, shard_id), exist_ok=True)

            dest_path = os.path.join(config.cache_dir, shard_id, 'warc.paths.gz')

            if os.path.exists(dest_path) and not config.ignore_cache:
                print(f"Shard {shard_id} already exists in cache, skipping")
                continue

            # download warc.paths.gz
            s3_client.download_file(commoncrawl_bucket, paths_manifest, dest_path)
            print(f"Downloaded manifest for shard {shard_id}")


def main(config: Config) -> None:
    s3 = boto3.resource("s3")

    if "CC-NEWS" in config.shard_ids:
        return process_cc_news(config)

    for shard_id in config.shard_ids:
        process_shard(s3, shard_id, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--shards",
        required=False,
        nargs="+",
        help="List of shards to replicate (default is ALL)",
    )
    parser.add_argument(
        "-c",
        "--cache-dir",
        default=os.path.abspath(os.path.join(os.path.dirname(__file__), "../tmp")),
        help="Local location to download files after replication",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        required=True,
        help="Destination bucket for replication",
    )
    parser.add_argument(
        "--ignore-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite any existing files on S3 and local cache",
    )

    args = parser.parse_args()
    shard_ids = args.shards

    if not shard_ids:
        shard_ids = [s.id for s in list_shards()]

    config = Config(
        shard_ids=shard_ids,
        cache_dir=args.cache_dir,
        ignore_cache=args.ignore_cache,
        dest_bucket=args.bucket,
    )

    main(config)

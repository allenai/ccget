"""
Replicate warc.paths.gz files to a location on S3. Then, download these files locally.

warc.paths.gz files are small (< 200KB) and give S3 keys for all WARC files in a shard.

@rauthur
"""

import argparse
from dataclasses import dataclass
from typing import Optional

import boto3

from ccget.shards import fetch_warc_paths, replicate_warc_paths


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


def main(config: Config) -> None:
    s3 = boto3.resource("s3")

    for shard_id in config.shard_ids:
        process_shard(s3, shard_id, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--shards",
        required=True,
        nargs="+",
        help="List of shards to replicate",
    )
    parser.add_argument(
        "-c",
        "--cache-dir",
        required=False,
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
    config = Config(
        shard_ids=args.shards,
        cache_dir=args.cache_dir,
        ignore_cache=args.ignore_cache,
        dest_bucket=args.bucket,
    )

    main(config)

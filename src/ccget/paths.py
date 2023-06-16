"""
Utility functions to get paths to various CC data objects on S3.

@rauthur
"""

import os

from ccget.consts import CC_BUCKET


def wet_paths_s3_key(shard_id: str) -> str:
    return f"crawl-data/{shard_id}/wet.paths.gz"


def wet_paths_s3_fn(shard_id: str, bucket_name=CC_BUCKET) -> str:
    """Location of file on S3 that lists all WET file paths"""
    return f"s3://{bucket_name}/{wet_paths_s3_key(shard_id)}"


def warc_paths_s3_key(shard_id: str) -> str:
    return f"crawl-data/{shard_id}/warc.paths.gz"


def warc_paths_s3_fn(shard_id: str, bucket_name=CC_BUCKET) -> str:
    """Location of file on S3 that lists all WARC file paths"""
    return f"s3://{bucket_name}/{warc_paths_s3_key(shard_id)}"


def warc_paths_local_fn(shard_id: str, cache_dir: str) -> str:
    return os.path.join(cache_dir, shard_id, "warc.paths.gz")


def src_fn(fn: str) -> str:
    return f"s3://{CC_BUCKET}/{fn}"

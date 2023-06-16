"""
Work with specific CC shard files for replication and fetching.

@rauthur
"""
import logging
import os
from dataclasses import dataclass

import botocore.exceptions
import requests

from ccget.consts import CC_BUCKET
from ccget.paths import warc_paths_local_fn, warc_paths_s3_key

_URL = "https://index.commoncrawl.org/collinfo.json"


@dataclass
class ShardInfo:
    id: str
    name: str


def list_shards() -> list[ShardInfo]:
    res = requests.get(_URL)
    shards = res.json()

    return [ShardInfo(id=s["id"], name=s["name"]) for s in shards]


def warc_is_replicated(s3, shard_id: str, dest_bucket_name: str) -> bool:
    client = s3.meta.client

    try:
        key = warc_paths_s3_key(shard_id)
        client.head_object(Bucket=dest_bucket_name, Key=key)

        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response["Error"]["Code"] == "404":
            return False
        else:
            raise ex


def replicate_warc_paths(
    s3,
    shard_id: str,
    dest_bucket_name: str,
    ignore_cache=False,
) -> None:
    """Replicates a warc.paths.gz file from Common Crawl to an intermediary bucket.
    The key is copied over in the same pattern as the Common Crawl structure.

    Args:
        s3: s3 Resource from boto3
        shard_id (str): The shard to replicate
        dest_bucket_name (str): Where to store the warc.paths.gz file
        ignore_cache (bool): If True then always replication. Defaults to False.
    """
    if not ignore_cache and warc_is_replicated(s3, shard_id, dest_bucket_name):
        print(f"Skipping S3 -> S3 replication for {shard_id}")
        return

    dest_bucket = s3.Bucket(dest_bucket_name)
    obj_key = warc_paths_s3_key(shard_id)

    source_config = {"Bucket": CC_BUCKET, "Key": obj_key}

    dest_bucket.copy(source_config, obj_key)


def fetch_warc_paths(
    s3,
    shard_id: str,
    cache_dir: str,
    ignore_cache=False,
    bucket_name=CC_BUCKET,
) -> None:
    """Fetches the warc.paths.gz file for a shard. This should be from replication
    source and not directly from the Common Crawl bucket.

    Args:
        s3: S3 resource client
        shard_id (str): Shard ID to fetch
        cache_dir (str): Local cache directory
        ignore_cache (bool, optional): Overwrites files if True. Defaults to False.
        bucket_name (_type_, optional): Source bucket name. Defaults to CC_BUCKET.
    """
    if bucket_name == CC_BUCKET:
        logging.warning(
            "Trying to download from Common Crawl bucket."
            " This will probably be throttled and fail!"
            " Consider copying files to an intermediate bucket first."
        )

    out_fn = warc_paths_local_fn(shard_id=shard_id, cache_dir=cache_dir)

    if not ignore_cache and os.path.exists(out_fn):
        print(f"Skipping S3 -> local fetch for {shard_id}")
        return

    os.makedirs(os.path.dirname(out_fn), exist_ok=True)

    bucket = s3.Bucket(bucket_name)
    bucket.download_file(warc_paths_s3_key(shard_id), out_fn)

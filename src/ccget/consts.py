from functools import cache

import boto3

CC_BUCKET = "commoncrawl"
AWS_REGION = "us-east-1"


@cache
def account_id() -> str:
    client = boto3.client("sts")
    return client.get_caller_identity()["Account"]

"""
Create a role with appropriate permissions for batch operations.
https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html

The role created here DOES NOT support object versioning as an additional precaution to
minimize storage costs. The intended use of this tool is to bulk archive massive amounts
of data with as little cost as possible.

If the provided role exists then only policies are attached. If policies exist then they
are re-used as well.

@rauthur
"""

import argparse
import json
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

_ASSUME_ROLE = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "batchoperations.s3.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)

_PUT_OBJECTS_ROLE_NAME = "S3BatchOpsPutObjects_CCGET"
_RESTORE_OBJECTS_ROLE_NAME = "S3BatchOpsRestoreObjects_CCGET"


def _put_objects_policy_document(dest_bucket_name: str) -> dict:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:PutObjectTagging",
                    ],
                    "Effect": "Allow",
                    "Resource": f"arn:aws:s3:::{dest_bucket_name}/*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{dest_bucket_name}/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{dest_bucket_name}/*"],
                },
            ],
        }
    )


def _restore_objects_policy_document(dest_bucket_name: str) -> dict:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:RestoreObject"],
                    "Resource": f"arn:aws:s3:::{dest_bucket_name}/*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{dest_bucket_name}/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{dest_bucket_name}/*"],
                },
            ],
        }
    )


@dataclass
class Config:
    role_name: str
    dest_bucket_name: str


def _policy_arn(role_name: str) -> str:
    return f"arn:aws:iam::aws:policy/{role_name}"


def _create_managed_policy(iam, policy_name: str, policy_details: any) -> dict:
    return iam.create_policy(
        PolicyName=policy_name,
        PolicyDocument=policy_details,
    )


def _get_or_create_managed_policy(iam, policy_name: str, policy_details: any) -> dict:
    try:
        return iam.get_policy(PolicyArn=_policy_arn(policy_name))
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchEntity":
            return _create_managed_policy(iam, policy_name, policy_details)
        else:
            raise ex


def _create_role(iam, role_name: str) -> dict:
    return iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=_ASSUME_ROLE)


def _get_or_create_role(iam, role_name: str) -> dict:
    try:
        return iam.get_role(RoleName=role_name)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchEntity":
            return _create_role(iam, role_name)
        else:
            raise ex


def main(config: Config):
    iam = boto3.client("iam")

    put_objects_policy = _get_or_create_managed_policy(
        iam,
        _PUT_OBJECTS_ROLE_NAME,
        _put_objects_policy_document(config.dest_bucket_name),
    )

    restore_objects_policy = _get_or_create_managed_policy(
        iam,
        _RESTORE_OBJECTS_ROLE_NAME,
        _restore_objects_policy_document(config.dest_bucket_name),
    )

    role = _get_or_create_role(iam, config.role_name)

    iam.attach_role_policy(
        RoleName=config.role_name,
        PolicyArn=put_objects_policy["Policy"]["Arn"],
    )
    iam.attach_role_policy(
        RoleName=config.role_name,
        PolicyArn=restore_objects_policy["Policy"]["Arn"],
    )

    print(f"Role ARN: {role['Role']['Arn']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--role-name",
        type=str,
        required=True,
        help="Role name (e.g., S3BatchOpsRole) to create",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        type=str,
        required=True,
        help="Destination bucket for result of copy operations",
    )

    args = parser.parse_args()

    main(Config(role_name=args.role_name, dest_bucket_name=args.bucket))

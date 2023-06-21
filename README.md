# ccget

This repository contains scripts to archive Common Crawl data on S3 to a bucket on us-east-1 (same region as Common Crawl). Shards are saved to Glacier Deep Archive on S3 (lowest cost storage but with access time and cost penalty). Copy operations are managed with S3's batch operations including object restoration and can all be driven by scripts in this repository.

## Summary of scripts

1. `scripts/create_role.py` -- Create a role capable of executing the batch operation jobs. Most likely this role already exists! So, you should not need to run this script, however, it illustrates the permissions assigned to the role.
1. `scripts/replicate_warc_paths.py` -- Common Crawl's warc.paths.gz files are text files containing the location of all data files in a given shard. Use this script to copy and download these files (~200KB/file; 90 files). This is fast and does not require much storage. Files are NOT re-copied if they exist in a location (unless `ignore_cache` is set). This behavior is probably what you want!
1. `scripts/copy_shard.py` -- Creates and submits an S3 batch operations job to copy from Common Crawl's bucket to a private bucket on S3. It is possible to sub-sample a set of shard files to the S3 standard storage class, however, full copies must go to the Deep Archive storage class and this is enforced in the script.
1. [TODO] `scripts/restore_shard.py` -- Combines local `warc.paths.gz` files with archived files to pull to standard S3 for a fixed number of days. After time expires files are moved back to Deep Archive. This restoration can take up to 48 hours and MUST go through Bulk restoration mode. Never use Expedited restoration unless it's an emergency as the cost is extremely high.

## Illustration of archive process

TODO

## Cost estimation for using Deep Archive

TODO

## Download the latest collinfo.json

This contains the full list of shards in JSON format. Please note that ARC (very old archive files) are ignored!

[Go here (JSON format)](https://index.commoncrawl.org/collinfo.json)

## Command to create bucket

This CLI command will create a `us-east-1` bucket. Most likely the bucket with Common Crawl data already exists so you should not need to run this!

```bash
aws s3api create-bucket --acl private --bucket ai2-russella --object-ownership BucketOwnerEnforced
```

Location constraint is not specified as the default is `us-east-1` which is the same region as the Common Crawl data.

## AWS Permissions

If you do need to create a role (described above as to why this is unlikely to be the case) then your IAM user will need permission to create roles and policies. Here is an example, generic policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:AttachRolePolicy",
                "iam:CreateRole",
                "iam:CreatePolicy",
                "iam:PutRolePolicy"
            ],
            "Resource": "*"
        }
    ]
}
```

You will also need permission to pass any created role to S3 batch operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": [
                "arn:aws:iam::AWS_ACCOUNT_ID:role/ROLE_NAME"
            ]
        }
    ]
}
```

Replace AWS_ACCOUNT_ID and ROLE_NAME with appropriate values. You will also need permission for `s3:CreateJob`, however, it's likely that you are a power user and already have full access to S3.

## Test Role and Bucket

A test bucket exists within the AllenNLP AWS account: `ai2-russella`. There is also a role with the appropriate permissions that can write and retore to this bucket: `S3BatchOpsRole_CCGET_Test`. Please use these things to test the scripts. But, it's important that a FULL archive is NOT run with the test setup for cost purposes.

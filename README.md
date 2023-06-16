# Retrying requests automatically

Set up some environment variables:

```bash
export AWS_RETRY_MODE=standard
export AWS_MAX_ATTEMPTS=10
```

This will use an exponential backoff up to 20 seconds between 10 total attempts.

# Download the latest collinfo.json

[Go here (JSON format)](https://index.commoncrawl.org/collinfo.json)

# Command to create bucket

```bash
aws s3api create-bucket --acl private --bucket ai2-russella --object-ownership BucketOwnerEnforced
```

Location constraint is not specified as the default is `us-east-1` which is the same region as the Common Crawl data.

Little application for controlling assets and generating alerts. 

Before start set up environment variables.

```
export ACCESS=aws-access-key
expport SECRET=aws-secret-key
export REGION_NAME=aws-region
export S3_BUCKET=aws-s3-bucket
```

create aws s3 bucket

```
aws s3 mb s3://my-crypto-bucket --region $REGION_NAME
```

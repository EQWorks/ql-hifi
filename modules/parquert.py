import os
import json
import gzip
import boto3
import pandas as pd


def to_parquert(bucket, key) -> None:
    s3 = boto3.client(
        service_name='s3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='us-east-1',
    )
    filename = 'tmp/results.parquet'
    obj = s3.get_object(Bucket=bucket, Key=key)
    with gzip.open(obj['Body'], 'rt') as gf:
        df = pd.read_json(gf)
        df.to_parquet(filename)

    with open(filename, 'rb') as data:
        s3.upload_fileobj(data, bucket, f'{key}.parquet')

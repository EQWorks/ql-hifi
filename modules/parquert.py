import os
import json
import gzip
import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def to_parquert(bucket, key) -> None:
    s3 = boto3.client(
        service_name='s3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='us-east-1',
    )
    filename = 'tmp/results.parquet'
    obj = s3.get_object(Bucket=bucket, Key=key)
    with gzip.open(obj['Body'], 'rt', encoding='UTF-8') as gf:
        data = json.loads(gf.read())
        table = pa.Table.from_pylist(data)
        pq.write_table(table, filename)

    with open(filename, 'rb') as data:
        s3.upload_fileobj(data, bucket, f'{key}.parquet')

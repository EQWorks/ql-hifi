try:
    import unzip_requirements
except ImportError:
    pass

import json

from modules import duck, parquet, s3


def query(event, context):
    q = event.get('query', event.get('q'))
    bucket = event.get('bucket')
    key = event.get('key')
    ttl = event.get('ttl')

    if 's3://' in q:
        duck.init()  # init the in-memory database with S3 connectivity

    data = duck.query(event.get('query', event.get('q')))
    result = json.dumps(data, default=str)
    if bucket and key:
        result = s3.put_to_s3(bucket, key, result, ttl)

    return {
        'statusCode': 200,
        'body': result,
    }


def to_parquet(event, context):
    bucket = event.get('bucket')
    keys = event.get('keys')
    for key in keys:
        parquet.to_parquet(bucket, key)

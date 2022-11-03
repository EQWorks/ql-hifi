import json

from modules import duck, parquert


def query(event, context):
    q = event.get('query', event.get('q'))

    if 's3://' in q:
        duck.init()  # init the in-memory database with S3 connectivity

    data = duck.query(event.get('query', event.get('q')))
    return {
        'statusCode': 200,
        'body': json.dumps(data, default=str),
    }


def to_parquet(event, context):
    bucket = event.get('bucket')
    keys = event.get('keys')
    for key in keys:
        parquert.to_parquert(bucket, key)


if __name__ == '__main__':
    event = {
        'bucket': 'ml-execution-cache-dev',
        'keys': ['27848/3121/1'],
    }
    to_parquet(event, None)

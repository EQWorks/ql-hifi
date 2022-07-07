import json

from modules import duck


def query(event, context):
    q = event.get('query', event.get('q'))

    if 's3://' in q:
        duck.init()  # init the in-memory database with S3 connectivity

    data = duck.query(event.get('query', event.get('q')))
    return {
        'statusCode': 200,
        'body': json.dumps(data),
    }

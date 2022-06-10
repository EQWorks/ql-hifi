import json

from modules import duck


def query(event, context):
    data = duck.query(event.get('query', event.get('q')))
    return {
        'statusCode': 200,
        'body': json.dumps(data),
    }

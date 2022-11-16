import os
import boto3


def put_to_s3(bucket, key, body, ttl=600) -> str:
    s3 = boto3.client(
        service_name='s3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='us-east-1',
    )
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
    return s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': bucket,
            'Key': key,
        },
        ExpiresIn=ttl,
    )

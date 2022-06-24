from typing import Tuple, Optional, List

import boto3


URI_PAYMI_TRANSACTION_FOLDER = 's3://paymi-prod-normalized-transaction-data'
PAYMI_REGION = 'ca-central-1'
PAYMI_AWS_ACCESS_KEY_ID = ''
PAYMI_AWS_SECRET_ACCESS_KEY = ''


def get_paymi_transaction_file_uris(
    year: str,
    month: Optional[str] = None,
    day: Optional[str] = None,
    regex: Optional[str] = '.*parquet',
    aws_access_key_id: Optional[str] = PAYMI_AWS_ACCESS_KEY_ID,
    aws_secret_access_key: Optional[str] = PAYMI_AWS_SECRET_ACCESS_KEY,
    region: Optional[str] = PAYMI_REGION,
) -> List[str]:
    prefix = f'{URI_PAYMI_TRANSACTION_FOLDER}/year={year}/'
    if month:
        prefix = f'{prefix}month={month}/'
        if day:
            prefix = f'{prefix}day={day}/'

    uris = list_obj_uri_with_prefix(
        uri_prefix=prefix,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region=region,
    )

    if regex:
        import re

        pattern = re.compile(regex)
        return [uri for uri in uris if pattern.match(uri)]

    return uris


def parse_s3_uri(path: str) -> Tuple[str, str]:
    """
    Args:
        path: s3 path in format 's3://bucket_name/obj_key'
    Returns:
        (bucket name, key)
    """
    tmp = path.strip().split('/', 3)
    return tmp[-2], tmp[-1]


def list_obj_uri_with_prefix(
    uri_prefix: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region: str = 'us-east-1',
) -> List[str]:
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region,
    )
    bucket, key_prefix = parse_s3_uri(uri_prefix)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=key_prefix)

    obj_uris = []
    for page in pages:
        for obj_dic in page.get('Contents', []):
            obj_uris.append(f's3://{bucket}/{obj_dic["Key"]}')

    return obj_uris


# if __name__ == '__main__':
#     import time
#     start = time.time()
#
#     year = '2022'
#     month = '06'
#     day = '23'
#
#     uris = get_paymi_transaction_file_uris(year, month, day)
#
#     print(uris)
#     print('length: ', len(uris))
#
#     print('time: ', time.time() - start)

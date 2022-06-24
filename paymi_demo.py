import os
import time
import pprint

from modules import duck
from modules.utils import get_paymi_transaction_file_uris


if __name__ == '__main__':
    year = '2022'
    parqs = []

    start = time.time()
    for m in ['05', '06']:
        parqs += get_paymi_transaction_file_uris(year, m)

    parqs = ','.join([f"'{p}'" for p in parqs])
    print(f'\nParquet URIs fetched in {time.time() - start} seconds.')

    # NOTE: this exposes a hard limit of DuckDB:
    # it could only handle up to one AWS credential and region per query
    duck.init(
        aws_access_key_id=os.getenv('PAYMI_AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('PAYMI_AWS_SECRET_ACCESS_KEY'),
        region=os.getenv('PAYMI_REGION', 'ca-central-1'),
    )

    start = time.time()

    """Note: the following query is an emulation of the following Trino queries:

        -- S3/Hive source, the same underlying parquets as ^ parqs
        SELECT
            source_type,
            institution_name,
            account_type,
            count(*)
        FROM paymi_hive.paymi_production.transaction
        WHERE
            year = '2022'
            AND month IN ('05', '06')
            AND date_posted >= DATE('2020-05-01')
        GROUP BY 1, 2, 3;

        -- MongoDB source, slightly inaccurate lookup range due to indexing differences
        SELECT
            source_type,
            institution_name,
            account_type,
            count(*)
        FROM paymi_mongo.paymi_production.transaction_normalizeds
        WHERE
            _id >= ObjectID(
                concat(
                    to_hex(to_big_endian_32(CAST(to_unixtime(CAST('2022-05-01' AS TIMESTAMP) - INTERVAL '7' DAY) AS INTEGER))),
                    '0000000000000000'
                )
            )
            AND date_posted >= DATE('2022-05-01')
        GROUP BY 1, 2, 3;
    """
    query = f'''
        SELECT
            source_type,
            institution_name,
            account_type,
            count(*)
        FROM read_parquet([{parqs}])
        WHERE date_posted >= '2020-05-01'::DATE
        GROUP BY 1, 2, 3;
    '''
    r = duck.query(query)
    print(f'\nQuery executed in {time.time() - start} seconds.')
    print('Query result:')
    pprint.pprint(r)

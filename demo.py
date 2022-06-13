import time
import pprint

from modules import duck


PARQS = ',\n'.join(
    [
        f"'{p}'"
        for p in [
            's3://paymi-duckdb/monthly/monthly_raw_2022-01-01.parquet',
            's3://paymi-duckdb/monthly/monthly_raw_2022-02-01.parquet',
            's3://paymi-duckdb/monthly/monthly_raw_2022-03-01.parquet',
            's3://paymi-duckdb/monthly/monthly_raw_2022-04-01.parquet',
            's3://paymi-duckdb/monthly/monthly_raw_2022-05-01.parquet',
        ]
    ]
)


def _execute(query):
    start = time.time()
    r = duck.query(query)
    print(f'\nQuery executed in {time.time() - start} seconds.')
    print('Query result:')
    pprint.pprint(r)


def _execute_save(query, fname):
    start = time.time()
    duck.query(query)
    print(f'\nQuery executed in {time.time() - start} seconds.')
    print(f'Query result: {fname}')


def count_by_gender():
    query = f'''
        SELECT consumer_gender, count(*) AS "count"
        FROM read_parquet([{PARQS}])
        GROUP BY 1;
    '''
    print(f'Executing Query (count by consumer_gender):\n{query}')
    _execute(query)


def avg_amount_rollup_location():
    fname = './avg_amount_rollup_loc.csv'
    query = f'''
        COPY(
            SELECT
                consumer_profile_province AS "province",
                consumer_profile_postal_code AS "postal_code",
                avg(amount_cents) AS "avg_amount_cents"
            FROM read_parquet([{PARQS}])
            GROUP BY ROLLUP(consumer_profile_province, consumer_profile_postal_code)
            ORDER BY 1, 2
        ) TO '{fname}' (HEADER);
    '''
    print(f'Executing Query (Avg amount (cents) rollup by location):\n{query}')
    _execute_save(query, fname)


def max_amount_gs_gender_source():
    fname = './max_amount_gs_gender_source.csv'
    query = f'''
        COPY (
            SELECT
                source_type AS "source",
                consumer_gender AS "gender",
                max(amount_cents) AS "max_amount_cents"
            FROM read_parquet([{PARQS}])
            GROUP BY GROUPING SETS (
                (source_type, consumer_gender),
                (source_type),
                (consumer_gender),
                () -- all
            )
            ORDER BY 1, 2
        ) TO '{fname}' (HEADER);
    '''
    print(f'Executing Query (Max amount (cents) gs by source and gender):\n{query}')
    _execute_save(query, fname)


def count_cube_cat_retailer_province():
    fname = './count_cube_cat_retailer_province.csv'
    query = f'''
        COPY (
            SELECT
                sic_code AS "category",
                retailer_name AS "retailer",
                consumer_profile_province AS "province",
                count(*) AS "count"
            FROM read_parquet([{PARQS}])
            GROUP BY CUBE (sic_code, retailer_name, consumer_profile_province)
        ) TO '{fname}' (HEADER);
    '''
    print(f'Executing Query (Count (txns) cube by category and retailer):\n{query}')
    _execute_save(query, fname)


if __name__ == '__main__':
    start = time.time()
    duck.init()
    print(f'DuckDB init with S3 conn in {time.time() - start} seconds.')

    count_by_gender()
    avg_amount_rollup_location()
    max_amount_gs_gender_source()
    count_cube_cat_retailer_province()

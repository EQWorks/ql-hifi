from modules import duck


if __name__ == "__main__":
    import time
    import pprint

    start = time.time()
    duck.init()
    print(
        f"Database initialized with S3 import capability in {time.time() - start} seconds."
    )

    start = time.time()

    parqs = [
        "s3://paymi-duckdb/monthly/monthly_raw_2022-01-01.parquet",
        "s3://paymi-duckdb/monthly/monthly_raw_2022-02-01.parquet",
        "s3://paymi-duckdb/monthly/monthly_raw_2022-03-01.parquet",
        "s3://paymi-duckdb/monthly/monthly_raw_2022-04-01.parquet",
        "s3://paymi-duckdb/monthly/monthly_raw_2022-05-01.parquet",
    ]
    parqs = ",\n".join([f"'{p}'" for p in parqs])
    query = f"""
        SELECT consumer_gender, count(*) AS "count"
        FROM read_parquet([{parqs}])
        GROUP BY 1;
    """

    print(f"Executing Query:\n{query}")

    r = duck.query(query)
    print("Query result:")
    pprint.pprint(r)

    print(f"\nQuery executed in {time.time() - start} seconds.")

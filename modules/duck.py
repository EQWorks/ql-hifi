import os
from typing import List

import sqlalchemy as sa


engine = sa.create_engine("duckdb:///:memory:")


def init(
    aws_access_key_id: str = os.getenv('AWS_KEY'),
    aws_secret_access_key: str = os.getenv('AWS_SECRET'),
    region: str = 'us-east-1',
) -> None:
    """
    Initialize the database with HTTPFS module loaded for S3 imports.
    """
    with engine.connect() as conn:
        conn.execute(
            f"""
                INSTALL httpfs;

                LOAD httpfs;

                SET s3_region='{region}';
                SET s3_access_key_id='{aws_access_key_id}';
                SET s3_secret_access_key='{aws_secret_access_key}';
            """
        )


def vet(query: str) -> bool:
    return 'duckdb_settings' not in query.lower()


def query(query: str) -> List[dict]:
    """
    Query the database.
    """
    if not vet(query):
        raise ValueError('Query contains illegal phrases.')

    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(query).fetchall()]

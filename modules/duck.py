import os
from typing import List

import sqlalchemy as sa


engine = sa.create_engine("duckdb:///:memory:")


def init():
    """
    Initialize the database with HTTPFS module loaded for S3 imports.
    """
    with engine.connect() as conn:
        conn.execute(
            f"""
                INSTALL httpfs;

                LOAD httpfs;

                SET s3_region='us-east-1';
                SET s3_access_key_id='{os.getenv("AWS_KEY")}';
                SET s3_secret_access_key='{os.getenv("AWS_SECRET")}';
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

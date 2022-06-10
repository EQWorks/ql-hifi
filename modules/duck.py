import os

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
                SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
                SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
            """
        )


def query(query: str):
    """
    Query the database.
    """
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(query).fetchall()]

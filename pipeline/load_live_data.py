"""
A script for extracting the FACT and DIM tables, 
converting them to parquet files and loading them to the database.
"""

# pylint: disable=redefined-outer-name

from os import environ as ENV

from dotenv import load_dotenv
import boto3
import pandas as pd
import awswrangler as wr

from extract import get_db_connection, extract_transaction_from_datetime
from transform import make_date_partitioned_parquet, get_batch_time

TIME_INTERVAL = 3


def create_s3_session():
    """Starts an s3 session."""

    return boto3.Session(
        aws_access_key_id=ENV["AWS_ACCESS_KEY"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"],
        region_name="eu-west-2"
    )


def upload_parquet_files_to_s3(df: pd.DataFrame, name: str, partition: bool, s3: boto3.Session):
    """Uploads the parquet files to the s3 bucket."""

    wr.s3.to_parquet(df=df,
                     path=f"s3://c19-niall-truck-bucket/trucks/input/{name}/",
                     dataset=True,
                     mode="append",
                     database="default",
                     table=name,
                     partition_cols=[
                         "year", "month", "day", "hour"] if partition else None,
                     boto3_session=s3)


if __name__ == "__main__":

    load_dotenv()
    batch_time = get_batch_time(TIME_INTERVAL)

    with get_db_connection() as conn:
        transaction = extract_transaction_from_datetime(conn, batch_time)

    transaction_df = pd.DataFrame.from_dict(transaction)

    make_date_partitioned_parquet(transaction_df, "transaction")

    s3 = create_s3_session()
    upload_parquet_files_to_s3(transaction_df, "transaction", True, s3)

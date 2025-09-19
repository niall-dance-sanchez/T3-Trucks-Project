"""Functions to connect and retrieve the data."""
# pylint:disable=unused-variable

from os import environ as ENV
import awswrangler as wr
import pandas as pd
import boto3
import streamlit as st


@st.cache_resource
def start_s3_session() -> boto3.Session:
    """Establishes an s3 connection."""

    return boto3.Session(
        aws_access_key_id=ENV["AWS_ACCESS_KEY"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"],
        region_name="eu-west-2"
    )


@st.cache_data
def retrieve_all_truck_data(database: str, _session: boto3.Session) -> pd.DataFrame:
    """Retrieve all of the truck data."""

    query = """
            SELECT *
            FROM transaction
            JOIN truck
                USING (truck_id)
            JOIN payment_method
                USING (payment_method_id)
            ;
            """

    return wr.athena.read_sql_query(
        query,
        database=database,
        boto3_session=_session)

"""Generates a report of the previous day's truck sales."""

from datetime import datetime, timedelta
from os import environ as ENV


import boto3
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv


def get_s3_connection():
    """Creates a connection with the S3 bucket."""

    return boto3.Session(aws_access_key_id=ENV["AWS_ACCESS_KEY_NIALL"],
                         aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY_NIALL"],
                         region_name="eu-west-2")


def retrieve_truck_data_by_date(s3_session, day: int, month: int) -> list[dict]:
    """Retrieve the truck data from the s3 bucket for a given date."""

    query = """
            SELECT total/100 AS value, *
            FROM transaction
            JOIN payment_method
                USING (payment_method_id)
            JOIN truck
                USING (truck_id)
            WHERE day = ':day'
                AND month = ':month'
            ;
            """

    return wr.athena.read_sql_query(query,
                                    database="c19-niall-truck-db",
                                    params={"day": day,
                                            "month": month},
                                    boto3_session=s3_session)


def get_total_revenue(df: pd.DataFrame) -> int:
    """Returns the total revenue of the trucks."""

    return int(df["value"].sum())


def get_total_transactions(df: pd.DataFrame) -> int:
    """Returns the total transactions."""

    return df.shape[0]


def get_truck_metrics(df: pd.DataFrame) -> dict:
    """Returns the mean, sum and count of the transactions for each truck."""

    return df.groupby(["truck_name"])["value"].agg(transactions='count',
                                                   revenue='sum',
                                                   avg_value='mean').round(2)


def get_payment_type_metrics(df: pd.DataFrame) -> dict:
    """Returns the count of each payment type."""

    return df["payment_method"].value_counts()


def summarise_previous_day_as_html_string(s3_session) -> str:
    """Summarises the previous day's truck data in html, ready to be opened."""

    yesterday = datetime.now() - timedelta(1)
    date = yesterday.date()
    day = yesterday.day
    month = yesterday.month

    df = retrieve_truck_data_by_date(s3_session, day, month)

    html_string = f"""<h1> T3 Report - {date}<h1>
                    <p style=font-size:20px> Total revenue £{get_total_revenue(df)} </p>
                    <p style=font-size:20px> Total transactions {get_total_transactions(df)} </p>
                    <br>
                    {pd.DataFrame(get_payment_type_metrics(df)).to_html()}
                    <br>
                    {get_truck_metrics(df).to_html()}
                    """

    return html_string


def handler(event=None, context=None) -> dict:
    """Main Lambda handler function."""

    load_dotenv()
    s3 = get_s3_connection()
    html_string = summarise_previous_day_as_html_string(s3)

    return {"statusCode": 200,
            "html": html_string}

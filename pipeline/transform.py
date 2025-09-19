"""A script with the functions for transforming the truck data to the correct format."""

from os import mkdir, path
import csv
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from extract import get_db_connection, extract_all_truck_data


def transform_total_to_pounds(total: float) -> float:
    """Transforms the payment total to pounds."""

    return total / 100


def prepare_truck_data(truck_data: list[dict]) -> list[tuple]:
    """Arrange truck data into the correct format."""

    return [(t["transaction_id"],
             transform_total_to_pounds(t["total"]),
             t["at"],
             t["payment_method"],
             t["truck_name"],
             t["has_card_reader"],
             t["fsa_rating"])
            for t in truck_data]


def write_truck_data_to_csv(clean_truck_data: list[tuple]) -> None:
    """Given the cleaned data as a list of tuples, write it to a csv file"""

    with open("trucks.csv", "w", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "transaction_id",
            "total",
            "transaction_at",
            "payment_method",
            "truck_name",
            "has_card_reader",
            "fsa_rating"
        ])
        writer.writerows(clean_truck_data)


def make_simple_parquet(df: pd.DataFrame, name: str):
    """Makes a parquet file of a database."""

    if not path.exists(name):
        mkdir(name)
    df.to_parquet(f"{name}/{name}.parquet")


def make_date_partitioned_parquet(df: pd.DataFrame, name: str):
    """Creates database parquet files partitioned by date."""

    df["year"] = df["at"].dt.year
    df["month"] = df["at"].dt.month
    df["day"] = df["at"].dt.day
    df["hour"] = df["at"].dt.hour

    df.to_parquet(name, partition_cols=[
        "year", "month", "day", "hour"])


def get_batch_time(hours: int) -> str:
    """Returns the current time minus a given number of hours as a string."""

    batch_time = datetime.now() - timedelta(hours=hours)

    return batch_time.strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    load_dotenv()
    conn = get_db_connection()
    data = extract_all_truck_data(conn)
    print(data[0])
    for d in data[0].values():
        print(d, type(d))

    clean_data = prepare_truck_data(data)
    write_truck_data_to_csv(clean_data)

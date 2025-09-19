from os import environ as ENV

from dotenv import load_dotenv
import pandas as pd

from extract import get_db_connection, extract_tables_from_truck_data
from transform import make_simple_parquet
from load_live_data import create_s3_session, upload_parquet_files_to_s3


if __name__ == "__main__":
    load_dotenv()
    with get_db_connection() as conn:
        truck = extract_tables_from_truck_data(conn, "DIM_Truck")
        payment_method = extract_tables_from_truck_data(
            conn, "DIM_Payment_Method")

    truck_df = pd.DataFrame.from_dict(truck)
    payment_method_df = pd.DataFrame.from_dict(payment_method)

    make_simple_parquet(truck_df, "truck")
    make_simple_parquet(payment_method_df, "payment_method")

    s3 = create_s3_session()
    upload_parquet_files_to_s3(truck_df, "truck", False, s3)
    upload_parquet_files_to_s3(payment_method_df, "payment_method", False, s3)

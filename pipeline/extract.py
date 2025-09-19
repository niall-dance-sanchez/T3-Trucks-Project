"""A script with the functions for extracting the truck data from the RDS."""

# pylint:disable=redefined-outer-name

from os import environ as ENV
import pymysql.cursors


def get_db_connection() -> None:
    """Connects to the trucks RDS."""

    return pymysql.connect(host=ENV["DB_HOST"],
                           user=ENV["DB_USER"],
                           password=ENV["DB_PASSWORD"],
                           database=ENV["DB_NAME"],
                           port=3306,
                           cursorclass=pymysql.cursors.DictCursor)


def extract_all_truck_data(conn) -> list[dict]:
    """Extracts all the data from the truck RDS."""

    with conn.cursor() as cur:
        query = """
                SELECT * 
                FROM FACT_Transaction 
                JOIN DIM_Payment_Method 
                    USING (Payment_Method_ID) 
                JOIN DIM_Truck 
                USING (truck_id)  
                ;
                """
        cur.execute(query)
        data = cur.fetchall()

    return data


def extract_tables_from_truck_data(conn, table: str) -> list[dict]:
    """Extracts all the data from a selected table name."""

    with conn.cursor() as cur:
        query = """
                SELECT * 
                FROM %s
                ;
                """
        cur.execute(query % table)
        data = cur.fetchall()

    return data


def extract_transaction_from_datetime(conn, datetime: str) -> list[dict]:
    """Downloads a set of rows from the transaction table from a given date and time."""

    with conn.cursor() as cur:
        query = """
                SELECT * 
                FROM FACT_Transaction 
                WHERE at >= %(datetime)s
                ;
                """
        cur.execute(
            query, {'datetime': datetime})
        data = cur.fetchall()

    return data

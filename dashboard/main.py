"""Runs streamlit dashboard."""

import streamlit as st
from dotenv import load_dotenv
from data import start_s3_session, retrieve_all_truck_data
from visualisations import (create_title, create_profit_chart,
                            create_profit_metric, create_card_reader_metric,
                            create_payment_type_chart, create_payment_method_metric,
                            create_sales_per_day, create_avg_value_metric,
                            create_least_popular_day_metric)


if __name__ == "__main__":
    load_dotenv()

    session = start_s3_session()
    DATABASE = "c19-niall-truck-db"

    truck_df = retrieve_all_truck_data(DATABASE, session)

    create_title()

    row = st.container(horizontal=True)
    with row:
        create_profit_metric(truck_df)
        create_avg_value_metric(truck_df)
        create_least_popular_day_metric(truck_df)

    all_trucks = truck_df["truck_name"].unique()
    chosen_trucks = st.sidebar.multiselect(
        label="Selected Trucks", options=all_trucks, default=all_trucks)
    chosen_trucks_df = truck_df[truck_df["truck_name"].isin(chosen_trucks)]

    if not chosen_trucks:
        st.write("No data selected.")
    else:
        st.plotly_chart(create_sales_per_day(chosen_trucks_df))

    row = st.container(horizontal=True)
    with row:
        create_payment_method_metric(truck_df, "card")
        create_payment_method_metric(truck_df, "cash")
        create_card_reader_metric(truck_df)

    left, right = st.columns(2)

    with left:
        if not chosen_trucks:
            st.write("No data selected.")
        else:
            st.plotly_chart(create_profit_chart(chosen_trucks_df))
    with right:
        st.plotly_chart(create_payment_type_chart(truck_df))

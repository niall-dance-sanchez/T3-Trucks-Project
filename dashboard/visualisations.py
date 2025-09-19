"""Functions for dashboard metrics and visualisations."""
# pylint:disable=unused-variable

import streamlit as st
import pandas as pd
import plotly.express as px


@st.cache_data
def create_title() -> None:
    """Configure the title and subheading of the streamlit dashboard."""

    st.title("T3 Profits Summary")
    st.write("Data to help inform T3 how to cut costs and raise profits.")


@st.cache_data
def create_profit_metric(df: pd.DataFrame) -> None:
    """Creates a metric of the total profit made by all trucks."""

    label = "Total Profit (3 s.f.)"
    total_profit = round(df["total"].sum()/100, -2)

    st.metric(label=label, value=f"£{total_profit}")


@st.cache_data
def create_avg_value_metric(df: pd.DataFrame) -> None:
    """Creates a metric of the average transaction value of all trucks."""

    label = "Average Transaction Value"
    avg_value = round(df["total"].mean()/100, 2)

    st.metric(label=label, value=f"£{avg_value}")


@st.cache_data
def create_least_popular_day_metric(df: pd.DataFrame) -> None:
    """Creates a metric of the least busy day of the week."""

    label = "Day with least transactions"
    day = df["at"].dt.day_name().value_counts().keys()[-1]

    st.metric(label=label, value=day)


@st.cache_data
def create_profit_chart(df: pd.DataFrame) -> px.bar:
    """Produces a bar chart of total truck profit."""

    df = df.groupby("truck_name")["total"].sum().reset_index()

    return px.bar(df, x="truck_name", y="total", title="Profit per Truck")


@st.cache_data
def create_sales_per_day(df: pd.DataFrame) -> px.line:
    """Produces a line chart of total sales over time."""

    df["date"] = df["at"].dt.date
    df = df.groupby(["date"])["truck_name"].value_counts().reset_index()

    return px.line(df, x='date', y='count', color='truck_name')


@st.cache_data
def create_card_reader_metric(df: pd.DataFrame) -> None:
    """Creates a metric  highlighting the only van without a card reader."""

    label = "Truck without card reader:"

    no_reader_truck = df[df["has_card_reader"] == 0]["truck_name"].unique()[0]

    st.metric(label=label, value=no_reader_truck)


@st.cache_data
def create_payment_method_metric(df: pd.DataFrame, method: str) -> None:
    """Creates a metric  highlighting the only van without a card reader."""

    label = f"Total {method} transaction value (3 s.f.)"

    value = round(df[df["payment_method"] == method]["total"].sum()/100, -2)

    st.metric(label=label, value=f"£{value}")


@st.cache_data
def create_payment_type_chart(df: pd.DataFrame) -> px.pie:
    """Produces a pie chart of the proportion of transaction payment methods."""

    df = df["payment_method"].value_counts().reset_index()

    return px.pie(df,
                  names=df["payment_method"],
                  values=df["count"],
                  title="Transaction Payment Types")

"""Streamlit dashboard for the generated traffic analytics."""

from io import BytesIO
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "output"

st.set_page_config(page_title="Cloud Smart Traffic Analytics", page_icon="🚦", layout="wide")
st.title("Cloud Smart Traffic Analytics")
st.caption("Explore road conditions, hourly patterns, and congestion levels.")

local_files = {
    "locations": OUTPUT_DIR / "location_stats.csv",
    "hours": OUTPUT_DIR / "hourly_stats.csv",
    "days": OUTPUT_DIR / "daily_stats.csv",
    "congestion": OUTPUT_DIR / "congestion_stats.csv",
}

with st.sidebar:
    st.header("Data source")
    source = st.radio(
    "Choose analytics source",
    ["Local files", "Amazon S3"],
    index=0,
    key="analytics_source",
)

if source == "Local files":
    missing_files = [
        path.name
        for path in local_files.values()
        if not path.is_file() or path.stat().st_size == 0
    ]
    if missing_files:
        st.info(
            "Analytics outputs are not available yet. Generate the data and run "
            "`python run_analytics.py` first."
        )
        st.stop()
    datasets = {name: pd.read_csv(path) for name, path in local_files.items()}
else:
    bucket = os.environ.get("AWS_S3_BUCKET")
    prefix = os.environ.get("AWS_S3_OUTPUT_PREFIX", "analytics").strip("/")
    if not bucket:
        st.error("Set AWS_S3_BUCKET before selecting Amazon S3 as the data source.")
        st.stop()

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION"))
    try:
        datasets = {}
        for name, filename in (
            ("locations", "location_stats.csv"),
            ("hours", "hourly_stats.csv"),
            ("days", "daily_stats.csv"),
            ("congestion", "congestion_stats.csv"),
        ):
            key = f"{prefix}/{filename}" if prefix else filename
            response = s3.get_object(Bucket=bucket, Key=key)
            datasets[name] = pd.read_csv(BytesIO(response["Body"].read()))
    except (BotoCoreError, ClientError) as error:
        st.error(f"Unable to read analytics from s3://{bucket}/{key}: {error}")
        st.stop()

locations = datasets["locations"]
hours = datasets["hours"]
days = datasets["days"]
congestion = datasets["congestion"]

if locations.empty or hours.empty or days.empty or congestion.empty:
    st.warning("The selected source contains no analytics. Run the Spark pipeline first.")
    st.stop()

total_records = int(locations["record_count"].sum()) if not locations.empty else 0
average_speed = (
    (locations["avg_speed_kmh"] * locations["record_count"]).sum() / total_records
    if total_records
    else 0
)
top_road = (
    locations.iloc[0]["location"] if not locations.empty else "No data"
)
high_congestion = congestion.loc[
    congestion["congestion_level"] == "High", "percentage"
]
high_percentage = float(high_congestion.iloc[0]) if not high_congestion.empty else 0
peak_hour = int(hours.loc[hours["avg_vehicle_count"].idxmax(), "hour"])

metric_columns = st.columns(5)
metric_columns[0].metric("Traffic records", f"{total_records:,}")
metric_columns[1].metric("Average speed", f"{average_speed:.1f} km/h")
metric_columns[2].metric("Most congested location", top_road)
metric_columns[3].metric("High congestion", f"{high_percentage:.1f}%")
metric_columns[4].metric("Peak traffic hour", f"{peak_hour:02d}:00")

left, right = st.columns(2)
with left:
    st.subheader("Average traffic by road")
    st.plotly_chart(
        px.bar(
            locations.sort_values("avg_vehicle_count", ascending=False),
            x="location",
            y="avg_vehicle_count",
            color="avg_congestion_index",
            color_continuous_scale="OrRd",
            labels={
                "location": "Road",
                "avg_vehicle_count": "Average vehicles",
                "avg_congestion_index": "Congestion index",
            },
        ),
        use_container_width=True,
    )

with right:
    st.subheader("Traffic by hour")
    st.plotly_chart(
        px.line(
            hours,
            x="hour",
            y="avg_vehicle_count",
            markers=True,
            labels={"hour": "Hour of day", "avg_vehicle_count": "Average vehicles"},
        ),
        use_container_width=True,
    )

st.subheader("Traffic by day")
st.plotly_chart(
    px.line(
        days,
        x="date",
        y="avg_vehicle_count",
        markers=True,
        labels={"date": "Date", "avg_vehicle_count": "Average vehicles"},
    ),
    use_container_width=True,
)

left, right = st.columns(2)
with left:
    st.subheader("Congestion distribution")
    st.plotly_chart(
        px.pie(
            congestion,
            names="congestion_level",
            values="record_count",
            color="congestion_level",
            color_discrete_map={"High": "#d62728", "Medium": "#ff7f0e", "Low": "#2ca02c"},
        ),
        use_container_width=True,
    )

with right:
    st.subheader("Road statistics")
    st.dataframe(locations, use_container_width=True, hide_index=True)

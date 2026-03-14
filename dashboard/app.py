from __future__ import annotations
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "data" / "dashboard" / "charge_sessions_enriched.csv"

@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # datetime conversions
    df["session_start_ts"] = pd.to_datetime(df["session_start_ts"])
    df["session_end_ts"] = pd.to_datetime(df["session_end_ts"])
    df["session_date"] = pd.to_datetime(df["session_date"])
    df["session_month"] = df["session_month"].astype(str)

    # numeric conversion
    numeric_cols = [
        "energy_kwh", "queue_wait_min", "price_per_kwh",
        "estimated_revenue_eur", "postcode", "latitude", "longitude",
        "connector_count", "power_kw", "session_hour", "session_duration_min",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


df = load_data(DATA_PATH)

st.set_page_config(page_title="EV Charging Dashboard", layout="wide")
st.title("EV Charging Demand and Utilization Dashboard")

# Sidebar filters
with st.sidebar:
    st.header("Filters")

    city_options = ["All"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("City", city_options)

    charger_options = ["All"] + sorted(df["charging_type"].dropna().unique().tolist())
    selected_charger = st.selectbox("Charging Type", charger_options)

    month_options = ["All"] + sorted(df["session_month"].dropna().unique().tolist())
    selected_month = st.selectbox("Session Month", month_options)

# Filter dataframe
filtered = df.copy()
if selected_city != "All":
    filtered = filtered[filtered["city"] == selected_city]
if selected_charger != "All":
    filtered = filtered[filtered["charging_type"] == selected_charger]
if selected_month != "All":
    filtered = filtered[filtered["session_month"] == selected_month]

# KPI cards
total_sessions = len(filtered)
total_revenue = filtered["estimated_revenue_eur"].sum()
avg_energy = filtered["energy_kwh"].mean()
failed_rate = (
    (filtered["session_status"].eq("failed").sum() / total_sessions) * 100
    if total_sessions > 0 else 0
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Sessions", f"{total_sessions:,}")
c2.metric("Total Revenue (€)", f"{total_revenue:,.2f}")
c3.metric("Avg Energy (kWh)", f"{avg_energy:,.2f}" if pd.notna(avg_energy) else "0.00")
c4.metric("Failed Rate (%)", f"{failed_rate:,.2f}")

# Monthly revenue chart
st.subheader("Monthly Revenue by City")
monthly_city = (
    filtered.groupby(["session_month", "city"], as_index=False)["estimated_revenue_eur"].sum()
)
fig1 = px.bar(monthly_city, x="session_month", y="estimated_revenue_eur", color="city", barmode="group")
st.plotly_chart(fig1, use_container_width=True)

# Session status chart
st.subheader("Session Status Distribution")
status_counts = filtered.groupby("session_status").size().reset_index(name="count")
fig2 = px.pie(status_counts, names="session_status", values="count")
st.plotly_chart(fig2, use_container_width=True)

# Operator performance table
st.subheader("Operator Performance")
operator_summary = (
    filtered.groupby(["operator_name", "city"], as_index=False)
    .agg(
        total_sessions=("session_id", "count"),
        total_revenue_eur=("estimated_revenue_eur", "sum"),
        avg_queue_wait_min=("queue_wait_min", "mean"),
        failed_sessions=("session_status", lambda s: (s == "failed").sum()),
        aborted_sessions=("session_status", lambda s: (s == "aborted").sum()),
    )
)
operator_summary["failed_rate_pct"] = (
    operator_summary["failed_sessions"] / operator_summary["total_sessions"] * 100
).round(2)
operator_summary["total_revenue_eur"] = operator_summary["total_revenue_eur"].round(2)
operator_summary["avg_queue_wait_min"] = operator_summary["avg_queue_wait_min"].round(2)
st.dataframe(operator_summary.sort_values("total_revenue_eur", ascending=False), use_container_width=True)

# Sample table
st.subheader("Filtered Session Sample")
st.dataframe(filtered.head(200), use_container_width=True)


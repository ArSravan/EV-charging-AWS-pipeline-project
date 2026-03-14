from __future__ import annotations
from pathlib import Path
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "data" / "dashboard" / "charge_sessions_enriched.csv"

@task
def load_dashboard_csv(path: Path) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Loading CSV from {path}")
    return pd.read_csv(path)

@task
def summarize_data(df: pd.DataFrame) -> pd.DataFrame:
    # Monthly revenue by city
    summary_df = (
        df.groupby(["session_month", "city"], as_index=False)
        .agg(
            total_sessions=("session_id", "count"),
            total_revenue_eur=("estimated_revenue_eur", "sum"),
            avg_energy=("energy_kwh", "mean")
        )
    )
    return summary_df

@task
def publish_artifacts(df: pd.DataFrame):
    # Publish table artifact
    create_table_artifact(
        key="monthly-revenue-summary",
        table=df.to_dict(orient="records"),
        description="Monthly revenue summary by city"
    )

    # Publish Markdown summary
    total_rows = len(df)
    latest_month = df["session_month"].max()
    total_revenue = df["total_revenue_eur"].sum()
    create_markdown_artifact(
        key="pipeline-summary",
        markdown=f"""
# Dashboard Pipeline Summary

- Total rows: {total_rows}
- Latest month: {latest_month}
- Total revenue: €{total_revenue:,.2f}
""",
        description="Summary of dashboard pipeline run"
    )


@flow(name="ev-dashboard-summary-flow")
def ev_dashboard_summary_flow():
    df = load_dashboard_csv(DATA_PATH)
    summary_df = summarize_data(df)
    publish_artifacts(summary_df)


if __name__ == "__main__":
    ev_dashboard_summary_flow()
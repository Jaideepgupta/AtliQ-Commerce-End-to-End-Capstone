"""
AtliQ Phase 2 — Streaming Ops DAG

Scheduled operational work around the streaming pipeline:

    check_fresh_events
            ↓
    optimize_tables
            ↓
    refresh_daily_summary
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksSqlOperator,
)


# ============================================================
# Databricks SQL Warehouse
# ============================================================

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/2523712671c74bd9"


# ============================================================
# Default arguments
# ============================================================

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # ========================================================
    # TASK 1 — Data Quality / Freshness Check
    # ========================================================

    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            SELECT
                assert_true(
                    COUNT(*) > 0,
                    'No fresh events found in the last 2 hours'
                ) AS freshness_check
            FROM atliq.streaming.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
        """,
    )


    # ========================================================
    # TASK 2 — Table Maintenance
    # ========================================================

    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            """
            OPTIMIZE atliq.streaming.silver_order_events
            """,

            """
            OPTIMIZE atliq.streaming.gold_revenue_5min
            """,
        ],
    )


    # ========================================================
    # TASK 3 — Daily Summary
    # ========================================================

    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            CREATE OR REPLACE TABLE atliq.streaming.gold_daily_summary AS

            SELECT
                CAST(event_ts AS DATE) AS event_date,

                COUNT_IF(
                    event_type = 'order_placed'
                ) AS orders_placed,

                COUNT_IF(
                    event_type = 'payment_received'
                ) AS orders_paid,

                COUNT_IF(
                    event_type = 'order_cancelled'
                ) AS orders_cancelled,

                SUM(
                    CASE
                        WHEN event_type = 'payment_received'
                        THEN order_amount
                        ELSE 0
                    END
                ) AS revenue

            FROM atliq.streaming.silver_order_events

            GROUP BY CAST(event_ts AS DATE)
        """,
    )


    # ========================================================
    # TASK DEPENDENCY
    # ========================================================

    check_fresh_events >> optimize_tables >> refresh_daily_summary
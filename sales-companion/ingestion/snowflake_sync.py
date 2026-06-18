"""
Sprint 1 — Step 2: Snowflake → Delta Lake Sync
Pulls ARR, ACV, health, forecast, and NRR from Snowflake and
upserts into the corresponding sales.metrics.* Delta tables.

Run as a Databricks Workflow task on a schedule (e.g., hourly).
Configure via environment variables or Databricks Secrets.
"""

import os
from datetime import datetime, timezone

import snowflake.connector
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# ── Snowflake connection ───────────────────────────────────────────────────────

def get_snowflake_options() -> dict:
    return {
        "sfURL": os.environ["SNOWFLAKE_ACCOUNT"] + ".snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "SALES_DW"),
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "METRICS"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE", "SALES_READ"),
    }


def read_from_snowflake(query: str) -> DataFrame:
    opts = get_snowflake_options()
    return (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("query", query)
        .load()
    )


# ── Sync helpers ───────────────────────────────────────────────────────────────

def upsert_to_delta(df: DataFrame, target: str, merge_key: str):
    """
    Merge source dataframe into target Delta table.
    Inserts new rows and updates changed ones.
    """
    tmp_view = f"_sync_{target.replace('.', '_')}"
    df.createOrReplaceTempView(tmp_view)

    cols = [c for c in df.columns if c != merge_key]
    update_set = ", ".join(f"t.{c} = s.{c}" for c in cols)
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in df.columns)

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {tmp_view} AS s
        ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


# ── Individual sync jobs ───────────────────────────────────────────────────────

def sync_arr():
    print("  Syncing ARR...")
    df = read_from_snowflake("""
        SELECT
            account_id,
            account_name,
            arr,
            mrr,
            arr_growth_pct,
            new_logo_arr,
            expansion_arr,
            churn_arr,
            segment,
            CURRENT_DATE()  AS snapshot_date
        FROM metrics.arr_current
        WHERE arr IS NOT NULL
    """).withColumn("synced_at", current_timestamp())

    upsert_to_delta(df, "sales.metrics.arr", merge_key="account_id")
    print(f"    ✓ ARR synced: {df.count()} rows")


def sync_acv():
    print("  Syncing ACV...")
    df = read_from_snowflake("""
        SELECT
            opportunity_id,
            account_id,
            account_name,
            acv,
            tcv,
            term_months,
            close_date,
            segment
        FROM metrics.acv_by_opportunity
        WHERE acv IS NOT NULL
    """).withColumn("synced_at", current_timestamp())

    upsert_to_delta(df, "sales.metrics.acv", merge_key="opportunity_id")
    print(f"    ✓ ACV synced: {df.count()} rows")


def sync_health():
    print("  Syncing Customer Health...")
    df = read_from_snowflake("""
        SELECT
            account_id,
            account_name,
            health_score,
            nps_score,
            csat_score,
            risk_tier,
            dau_30d,
            feature_adoption,
            last_login_date,
            CURRENT_DATE() AS snapshot_date
        FROM metrics.customer_health_current
    """).withColumn("synced_at", current_timestamp())

    upsert_to_delta(df, "sales.metrics.health", merge_key="account_id")
    print(f"    ✓ Health synced: {df.count()} rows")


def sync_forecast():
    print("  Syncing Forecast...")
    df = read_from_snowflake("""
        SELECT
            period,
            segment,
            owner_id,
            committed,
            best_case,
            pipeline,
            closed_won,
            quota,
            CASE WHEN quota > 0 THEN closed_won / quota ELSE NULL END AS attainment_pct,
            CURRENT_DATE() AS snapshot_date
        FROM metrics.sales_forecast_current
    """).withColumn("synced_at", current_timestamp())

    # Forecast is append-only (daily snapshots), not an upsert
    df.write.format("delta").mode("append").saveAsTable("sales.metrics.forecast")
    print(f"    ✓ Forecast synced: {df.count()} rows")


def sync_nrr():
    print("  Syncing NRR...")
    df = read_from_snowflake("""
        SELECT
            period,
            segment,
            nrr_pct,
            gross_retention,
            expansion_pct,
            churn_pct,
            CURRENT_DATE() AS snapshot_date
        FROM metrics.nrr_monthly
        WHERE period >= ADD_MONTHS(CURRENT_DATE(), -24)
    """).withColumn("synced_at", current_timestamp())

    df.write.format("delta").mode("overwrite").option("replaceWhere", "snapshot_date = CURRENT_DATE()").saveAsTable("sales.metrics.nrr")
    print(f"    ✓ NRR synced: {df.count()} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    start = datetime.now(timezone.utc)
    print(f"\n[{start.isoformat()}] Starting Snowflake sync\n")

    sync_arr()
    sync_acv()
    sync_health()
    sync_forecast()
    sync_nrr()

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n✅ Snowflake sync complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run()

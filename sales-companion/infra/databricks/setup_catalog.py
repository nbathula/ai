"""
Sprint 1 — Step 1: Unity Catalog Setup
Creates all schemas and Delta tables for Sales Companion.
Run once in Databricks as a notebook or Workflow task.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "sales"


def run(sql: str):
    spark.sql(sql)


# ── Schemas ────────────────────────────────────────────────────────────────────

def create_schemas():
    for schema in ["realtime", "metrics", "crm", "documents", "monitoring"]:
        run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
        print(f"  ✓ Schema: {CATALOG}.{schema}")


# ── Realtime tables (Kafka / DLT feeds these) ─────────────────────────────────

def create_realtime_tables():
    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.realtime.opportunity_current (
      opportunity_id      STRING        NOT NULL,
      account_id          STRING,
      account_name        STRING,
      owner_id            STRING,
      owner_name          STRING,
      name                STRING,
      amount              DECIMAL(18,2),
      acv                 DECIMAL(18,2),
      stage               STRING,
      close_date          DATE,
      probability         INTEGER,
      forecast_category   STRING,
      days_in_stage       INTEGER,
      last_activity_date  DATE,
      health_score        INTEGER,
      created_date        DATE,
      updated_at          TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true'
    )
    COMMENT 'Live Salesforce opportunity data — fed by Kafka / DLT'
    """)
    print(f"  ✓ Table: {CATALOG}.realtime.opportunity_current")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.realtime.account_current (
      account_id      STRING NOT NULL,
      name            STRING,
      industry        STRING,
      segment         STRING,
      owner_id        STRING,
      csm_id          STRING,
      arr             DECIMAL(18,2),
      health_score    INTEGER,
      risk_tier       STRING,
      contract_end    DATE,
      updated_at      TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Live Salesforce account data — fed by Kafka / DLT'
    """)
    print(f"  ✓ Table: {CATALOG}.realtime.account_current")


# ── Metrics tables (Snowflake sync feeds these) ────────────────────────────────

def create_metrics_tables():
    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metrics.arr (
      account_id          STRING NOT NULL,
      account_name        STRING,
      arr                 DECIMAL(18,2),
      mrr                 DECIMAL(18,2),
      arr_growth_pct      DOUBLE,
      new_logo_arr        DECIMAL(18,2),
      expansion_arr       DECIMAL(18,2),
      churn_arr           DECIMAL(18,2),
      segment             STRING,
      snapshot_date       DATE,
      synced_at           TIMESTAMP
    )
    USING DELTA
    COMMENT 'ARR metrics synced from Snowflake'
    """)
    print(f"  ✓ Table: {CATALOG}.metrics.arr")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metrics.acv (
      opportunity_id  STRING NOT NULL,
      account_id      STRING,
      account_name    STRING,
      acv             DECIMAL(18,2),
      tcv             DECIMAL(18,2),
      term_months     INTEGER,
      close_date      DATE,
      segment         STRING,
      synced_at       TIMESTAMP
    )
    USING DELTA
    COMMENT 'ACV per deal synced from Snowflake'
    """)
    print(f"  ✓ Table: {CATALOG}.metrics.acv")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metrics.health (
      account_id       STRING NOT NULL,
      account_name     STRING,
      health_score     INTEGER,
      nps_score        INTEGER,
      csat_score       DOUBLE,
      risk_tier        STRING,
      dau_30d          INTEGER,
      feature_adoption DOUBLE,
      last_login_date  DATE,
      snapshot_date    DATE,
      synced_at        TIMESTAMP
    )
    USING DELTA
    COMMENT 'Customer health scores synced from Snowflake'
    """)
    print(f"  ✓ Table: {CATALOG}.metrics.health")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metrics.forecast (
      period            STRING NOT NULL,
      segment           STRING,
      owner_id          STRING,
      committed         DECIMAL(18,2),
      best_case         DECIMAL(18,2),
      pipeline          DECIMAL(18,2),
      closed_won        DECIMAL(18,2),
      quota             DECIMAL(18,2),
      attainment_pct    DOUBLE,
      snapshot_date     DATE,
      synced_at         TIMESTAMP
    )
    USING DELTA
    COMMENT 'Sales forecast synced from Snowflake'
    """)
    print(f"  ✓ Table: {CATALOG}.metrics.forecast")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metrics.nrr (
      period          STRING NOT NULL,
      segment         STRING,
      nrr_pct         DOUBLE,
      gross_retention DOUBLE,
      expansion_pct   DOUBLE,
      churn_pct       DOUBLE,
      snapshot_date   DATE,
      synced_at       TIMESTAMP
    )
    USING DELTA
    COMMENT 'Net Revenue Retention synced from Snowflake'
    """)
    print(f"  ✓ Table: {CATALOG}.metrics.nrr")


# ── Documents table (ingestion pipeline feeds this) ───────────────────────────

def create_documents_tables():
    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.documents.metadata (
      doc_id          STRING NOT NULL,
      file_name       STRING,
      file_path       STRING,
      doc_type        STRING,
      account_id      STRING,
      account_name    STRING,
      opportunity_id  STRING,
      owner_id        STRING,
      doc_date        DATE,
      expiry_date     DATE,
      source          STRING,
      ingested_at     TIMESTAMP,
      file_hash       STRING
    )
    USING DELTA
    COMMENT 'Document-level metadata for all ingested files'
    """)
    print(f"  ✓ Table: {CATALOG}.documents.metadata")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.documents.chunks (
      chunk_id        STRING NOT NULL,
      doc_id          STRING,
      doc_type        STRING,
      account_id      STRING,
      account_name    STRING,
      opportunity_id  STRING,
      doc_date        DATE,
      content         STRING,
      source_page     INTEGER,
      chunk_index     INTEGER,
      token_count     INTEGER,
      embedding       ARRAY<FLOAT>,
      ingested_at     TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Chunked document content with embeddings for Vector Search'
    """)
    print(f"  ✓ Table: {CATALOG}.documents.chunks")


# ── Monitoring tables ─────────────────────────────────────────────────────────

def create_monitoring_tables():
    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.agent_traces (
      trace_id              STRING NOT NULL,
      session_id            STRING,
      user_id               STRING,
      agent_type            STRING,
      query                 STRING,
      response              STRING,
      generated_sql         STRING,
      retrieved_chunk_ids   ARRAY<STRING>,
      total_latency_ms      INTEGER,
      retrieval_latency_ms  INTEGER,
      llm_latency_ms        INTEGER,
      confidence_score      DOUBLE,
      groundedness_score    DOUBLE,
      input_tokens          INTEGER,
      output_tokens         INTEGER,
      estimated_cost_usd    DOUBLE,
      agent_version         STRING,
      model                 STRING,
      ts                    TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'delta.logRetentionDuration' = 'interval 90 days'
    )
    COMMENT 'Every agent response traced here — primary monitoring table'
    """)
    print(f"  ✓ Table: {CATALOG}.monitoring.agent_traces")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.user_feedback (
      trace_id   STRING,
      user_id    STRING,
      rating     INTEGER,
      comment    STRING,
      ts         TIMESTAMP
    )
    USING DELTA
    COMMENT 'Thumbs up / down feedback from the React UI'
    """)
    print(f"  ✓ Table: {CATALOG}.monitoring.user_feedback")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.groundedness_issues (
      trace_id           STRING,
      query              STRING,
      unsupported_claim  STRING,
      ts                 TIMESTAMP
    )
    USING DELTA
    COMMENT 'Unsupported claims flagged by groundedness scorer'
    """)
    print(f"  ✓ Table: {CATALOG}.monitoring.groundedness_issues")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.eval_runs (
      run_id              STRING NOT NULL,
      triggered_by        STRING,
      agent_version       STRING,
      total_questions     INTEGER,
      passed              INTEGER,
      failed              INTEGER,
      avg_groundedness    DOUBLE,
      avg_sql_accuracy    DOUBLE,
      pass_rate           DOUBLE,
      threshold_met       BOOLEAN,
      mlflow_run_id       STRING,
      ts                  TIMESTAMP
    )
    USING DELTA
    COMMENT 'Pre-release eval run results'
    """)
    print(f"  ✓ Table: {CATALOG}.monitoring.eval_runs")

    run(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.sql_log (
      trace_id              STRING,
      query                 STRING,
      generated_sql         STRING,
      executed_successfully BOOLEAN,
      error_message         STRING,
      rows_returned         INTEGER,
      execution_ms          INTEGER,
      agent_version         STRING,
      ts                    TIMESTAMP
    )
    USING DELTA
    COMMENT 'Every Text-to-SQL attempt logged for accuracy tracking'
    """)
    print(f"  ✓ Table: {CATALOG}.monitoring.sql_log")


# ── Volumes ───────────────────────────────────────────────────────────────────

def create_volumes():
    for vol in ["contracts", "proposals", "transcripts", "emails"]:
        run(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.documents.{vol}
        COMMENT 'Raw {vol} files before ingestion'
        """)
        print(f"  ✓ Volume: {CATALOG}.documents.{vol}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"\n✓ Catalog: {CATALOG}\n")

    print("Creating schemas...")
    create_schemas()

    print("\nCreating realtime tables...")
    create_realtime_tables()

    print("\nCreating metrics tables...")
    create_metrics_tables()

    print("\nCreating document tables...")
    create_documents_tables()

    print("\nCreating monitoring tables...")
    create_monitoring_tables()

    print("\nCreating volumes...")
    create_volumes()

    print("\n✅ Sprint 1 — Unity Catalog setup complete")
    print(f"   Catalog : {CATALOG}")
    print(f"   Schemas : realtime · metrics · crm · documents · monitoring")

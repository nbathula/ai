"""
Sprint 2 — Delta Live Tables Pipeline
Reads Salesforce CDC events from MSK Kafka topics and upserts into
sales.realtime.opportunity_current and sales.realtime.account_current.

Deploy as a Databricks DLT pipeline (continuous mode).
"""

import os
from typing import Iterator

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, from_json, get_json_object,
    lit, to_date, to_timestamp, when,
)
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)

MSK_BOOTSTRAP = os.environ["MSK_BOOTSTRAP_SERVERS"]
MSK_KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": MSK_BOOTSTRAP,
    "kafka.security.protocol": "SSL",
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "50000",
}

# ── Salesforce CDC event schema ────────────────────────────────────────────────

OPPORTUNITY_SCHEMA = StructType([
    StructField("Id",                   StringType()),
    StructField("AccountId",            StringType()),
    StructField("AccountName",          StringType()),
    StructField("OwnerId",              StringType()),
    StructField("OwnerName",            StringType()),
    StructField("Name",                 StringType()),
    StructField("Amount",               StringType()),
    StructField("ACV__c",               StringType()),
    StructField("StageName",            StringType()),
    StructField("CloseDate",            StringType()),
    StructField("Probability",          StringType()),
    StructField("ForecastCategory",     StringType()),
    StructField("DaysInCurrentStage__c",StringType()),
    StructField("LastActivityDate",     StringType()),
    StructField("HealthScore__c",       StringType()),
    StructField("CreatedDate",          StringType()),
    StructField("LastModifiedDate",     StringType()),
    StructField("_EventType",           StringType()),   # CREATE | UPDATE | DELETE
])

ACCOUNT_SCHEMA = StructType([
    StructField("Id",               StringType()),
    StructField("Name",             StringType()),
    StructField("Industry",         StringType()),
    StructField("Segment__c",       StringType()),
    StructField("OwnerId",          StringType()),
    StructField("CSM__c",           StringType()),
    StructField("ARR__c",           StringType()),
    StructField("HealthScore__c",   StringType()),
    StructField("RiskTier__c",      StringType()),
    StructField("ContractEndDate__c", StringType()),
    StructField("LastModifiedDate", StringType()),
    StructField("_EventType",       StringType()),
])


# ── Bronze layer — raw CDC events ─────────────────────────────────────────────

@dlt.table(
    name="opportunity_cdc_raw",
    comment="Raw Salesforce Opportunity CDC events from MSK",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def opportunity_cdc_raw() -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .options(**MSK_KAFKA_OPTIONS)
        .option("subscribe", "salesforce.opportunity.cdc")
        .load()
        .select(
            col("value").cast("string").alias("raw_json"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_ts"),
        )
    )


@dlt.table(
    name="account_cdc_raw",
    comment="Raw Salesforce Account CDC events from MSK",
    table_properties={"quality": "bronze"},
)
def account_cdc_raw() -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .options(**MSK_KAFKA_OPTIONS)
        .option("subscribe", "salesforce.account.cdc")
        .load()
        .select(
            col("value").cast("string").alias("raw_json"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_ts"),
        )
    )


# ── Silver layer — parsed and typed CDC events ─────────────────────────────────

@dlt.expect_all_or_drop({
    "valid_opportunity_id": "opportunity_id IS NOT NULL",
    "valid_event_type": "event_type IN ('CREATE', 'UPDATE', 'DELETE')",
})
@dlt.table(
    name="opportunity_cdc_parsed",
    comment="Parsed Opportunity CDC events — typed columns, DQ enforced",
    table_properties={"quality": "silver"},
)
def opportunity_cdc_parsed() -> DataFrame:
    raw = dlt.read_stream("opportunity_cdc_raw")

    parsed = raw.select(
        from_json(col("raw_json"), OPPORTUNITY_SCHEMA).alias("data"),
        col("kafka_ts"),
    ).select(
        col("data.Id").alias("opportunity_id"),
        col("data.AccountId").alias("account_id"),
        col("data.AccountName").alias("account_name"),
        col("data.OwnerId").alias("owner_id"),
        col("data.OwnerName").alias("owner_name"),
        col("data.Name").alias("name"),
        col("data.Amount").cast(DecimalType(18, 2)).alias("amount"),
        col("data.ACV__c").cast(DecimalType(18, 2)).alias("acv"),
        col("data.StageName").alias("stage"),
        to_date(col("data.CloseDate"), "yyyy-MM-dd").alias("close_date"),
        col("data.Probability").cast(IntegerType()).alias("probability"),
        col("data.ForecastCategory").alias("forecast_category"),
        col("data.DaysInCurrentStage__c").cast(IntegerType()).alias("days_in_stage"),
        to_date(col("data.LastActivityDate"), "yyyy-MM-dd").alias("last_activity_date"),
        col("data.HealthScore__c").cast(IntegerType()).alias("health_score"),
        to_date(col("data.CreatedDate"), "yyyy-MM-dd").alias("created_date"),
        to_timestamp(col("data.LastModifiedDate")).alias("updated_at"),
        col("data._EventType").alias("event_type"),
        col("kafka_ts"),
    )
    return parsed


@dlt.expect_all_or_drop({
    "valid_account_id": "account_id IS NOT NULL",
})
@dlt.table(
    name="account_cdc_parsed",
    comment="Parsed Account CDC events",
    table_properties={"quality": "silver"},
)
def account_cdc_parsed() -> DataFrame:
    raw = dlt.read_stream("account_cdc_raw")

    parsed = raw.select(
        from_json(col("raw_json"), ACCOUNT_SCHEMA).alias("data"),
        col("kafka_ts"),
    ).select(
        col("data.Id").alias("account_id"),
        col("data.Name").alias("name"),
        col("data.Industry").alias("industry"),
        col("data.Segment__c").alias("segment"),
        col("data.OwnerId").alias("owner_id"),
        col("data.CSM__c").alias("csm_id"),
        col("data.ARR__c").cast(DecimalType(18, 2)).alias("arr"),
        col("data.HealthScore__c").cast(IntegerType()).alias("health_score"),
        col("data.RiskTier__c").alias("risk_tier"),
        to_date(col("data.ContractEndDate__c"), "yyyy-MM-dd").alias("contract_end"),
        to_timestamp(col("data.LastModifiedDate")).alias("updated_at"),
        col("data._EventType").alias("event_type"),
        col("kafka_ts"),
    )
    return parsed


# ── Gold layer — current state (upsert / SCD Type 1) ─────────────────────────

@dlt.table(
    name="opportunity_current",
    comment="Current Salesforce Opportunity state — maintained by CDC upserts",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def opportunity_current():
    """
    APPLY CHANGES upserts the latest event per opportunity_id.
    DELETE events soft-delete (handled by event_type filter in queries).
    """
    pass   # populated by the apply_changes call below


dlt.apply_changes(
    target="opportunity_current",
    source="opportunity_cdc_parsed",
    keys=["opportunity_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=col("event_type") == lit("DELETE"),
    stored_as_scd_type=1,
    column_list=[
        "opportunity_id", "account_id", "account_name", "owner_id",
        "owner_name", "name", "amount", "acv", "stage", "close_date",
        "probability", "forecast_category", "days_in_stage",
        "last_activity_date", "health_score", "created_date", "updated_at",
    ],
)


@dlt.table(
    name="account_current",
    comment="Current Salesforce Account state — maintained by CDC upserts",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def account_current():
    pass


dlt.apply_changes(
    target="account_current",
    source="account_cdc_parsed",
    keys=["account_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1,
    column_list=[
        "account_id", "name", "industry", "segment", "owner_id",
        "csm_id", "arr", "health_score", "risk_tier", "contract_end", "updated_at",
    ],
)


# ── DLQ monitor — alert on dead-letter queue growth ───────────────────────────

@dlt.table(
    name="dlq_events",
    comment="Dead-letter queue events — failed CDC records needing investigation",
    table_properties={"quality": "bronze"},
)
def dlq_events() -> DataFrame:
    opp_dlq = (
        spark.readStream
        .format("kafka")
        .options(**MSK_KAFKA_OPTIONS)
        .option("subscribe", "sales-companion.dlq.opportunity,sales-companion.dlq.account")
        .load()
        .select(
            col("value").cast("string").alias("raw_json"),
            col("topic"),
            col("offset"),
            col("timestamp").alias("kafka_ts"),
            current_timestamp().alias("ingested_at"),
        )
    )
    return opp_dlq

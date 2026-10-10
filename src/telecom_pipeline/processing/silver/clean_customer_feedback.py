
"""
Telecom Spark Silver - Customer Feedback

Inputs:
    raw/batch/complaints.csv
    raw/batch/customer_experience.csv

Outputs:
    processed/silver/complaints/
    processed/silver/customer_experience/
    processed/quarantine/complaints/
    processed/quarantine/customer_experience/
    reports/silver/customer_feedback_quality.json

Spark: cleaning, typing, validation, quality flags.
dbt: facts, dimensions, SLA KPIs, NPS, impact analysis.
"""

from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BASE = DATA_ROOT
REPORT = REPORTS / "customer_feedback_quality.json"

COMPLAINT_COLUMNS = [
    "complaint_id", "customer_id", "complaint_type_id",
    "incident_id", "created_timestamp", "resolved_timestamp",
    "status", "priority", "channel", "complaint_text",
    "resolution_minutes", "sla_breached",
]

CX_COLUMNS = [
    "cx_id", "customer_id", "event_timestamp", "rating",
    "satisfaction_score", "nps_score", "call_quality_score",
    "internet_experience_score", "network_quality_score",
    "channel",
]


# ------------------------------------------------------
# Shared utilities
# ------------------------------------------------------

def read_csv(spark, table, columns):
    path = RAW / f"{table}.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(path))
    )

    if df.columns != columns:
        raise ValueError(
            f"{table}: unexpected schema.\n"
            f"Expected: {columns}\n"
            f"Actual: {df.columns}"
        )

    return df


def text(column):
    value = F.regexp_replace(
        F.trim(F.col(column)), r"\s+", " "
    )
    return F.when(value == "", None).otherwise(value)


def safe_double(column):
    return F.expr(f"try_cast(`{column}` as double)")


def safe_int(column):
    return F.expr(f"try_cast(`{column}` as int)")


def safe_timestamp(column):
    # Explicitly match the generator's timestamp format.
    return F.to_timestamp(
        F.col(column), "yyyy-MM-dd HH:mm:ss"
    )


def safe_boolean(column):
    value = F.lower(F.col(column))

    return (
        F.when(value.isin("true", "1", "yes"), True)
        .when(value.isin("false", "0", "no"), False)
        .otherwise(F.lit(None).cast("boolean"))
    )


def flag(df, name, expression):
    return df.withColumn(
        name, F.coalesce(expression, F.lit(False))
    )


def duplicate_flag(df, key):
    return flag(
        df, "is_duplicate_key",
        F.count("*").over(Window.partitionBy(key)) > 1
    )


def normalize_all(df):
    for column in df.columns:
        df = df.withColumn(column, text(column))
    return df


def add_customer_reference_flag(df, spark):
    """
    Check against accepted Silver customers.
    No business enrichment or dimension joins.
    """
    customers = (
        spark.read.parquet(str(SILVER / "customers"))
        .select("customer_id")
        .distinct()
        .withColumnRenamed("customer_id", "_valid_customer_id")
    )

    df = df.join(
        F.broadcast(customers),
        df["customer_id"] == customers["_valid_customer_id"],
        "left"
    )

    return flag(
        df, "is_invalid_customer_reference",
        F.col("_valid_customer_id").isNull()
    ).drop("_valid_customer_id")


def save_dataset(df, table):
    quality_flags = [
        c for c, t in df.dtypes
        if c.startswith("is_")
        and t == "boolean"
        and c not in ("is_quarantined", "is_sla_breached")
    ]

    metrics = df.agg(
        F.count("*").alias("raw_rows"),
        F.sum(F.when(F.col("is_quarantined"), 1)
              .otherwise(0)).alias("quarantined_rows"),
        *[
            F.sum(F.when(F.col(c), 1).otherwise(0)).alias(c)
            for c in quality_flags
        ]
    ).first().asDict()

    raw_count = int(metrics.pop("raw_rows"))
    rejected = int(metrics.pop("quarantined_rows") or 0)

    # Keep source audit values in quarantine only.
    raw_audit = [
        c for c in df.columns if c.endswith("_raw")
    ]

    silver_df = (
        df.filter(~F.col("is_quarantined"))
        .drop("is_quarantined", *raw_audit)
    )

    quarantine_df = df.filter("is_quarantined")

    silver_df.write.mode("overwrite").parquet(
        str(SILVER / table)
    )
    quarantine_df.write.mode("overwrite").parquet(
        str(QUARANTINE / table)
    )

    result = {
        "raw_rows": raw_count,
        "silver_rows": raw_count - rejected,
        "quarantined_rows": rejected,
        "quality_flags": {
            k: int(v or 0) for k, v in metrics.items()
        },
    }

    print(
        f"[{table.upper()}] Raw={raw_count:,} "
        f"Silver={raw_count - rejected:,} "
        f"Quarantine={rejected:,}",
        flush=True,
    )
    return result


# ------------------------------------------------------
# Complaints
# ------------------------------------------------------

def process_complaints(spark):
    print("\n[COMPLAINTS] Processing...", flush=True)

    df = normalize_all(
        read_csv(spark, "complaints", COMPLAINT_COLUMNS)
    )

    # Keep values needed to diagnose failed casts.
    for column in [
        "complaint_type_id", "created_timestamp",
        "resolved_timestamp", "resolution_minutes",
        "sla_breached",
    ]:
        df = df.withColumn(f"{column}_raw", F.col(column))

    df = (
        df
        .withColumn("complaint_type_id",
                    safe_int("complaint_type_id"))
        .withColumn("created_timestamp",
                    safe_timestamp("created_timestamp"))
        .withColumn("resolved_timestamp",
                    safe_timestamp("resolved_timestamp"))
        .withColumn("resolution_minutes",
                    safe_double("resolution_minutes"))
        .withColumn("sla_breached",
                    safe_boolean("sla_breached"))
        .withColumn("status", F.upper("status"))
        .withColumn("priority", F.upper("priority"))
        .withColumn("channel",
                    F.initcap(F.lower("channel")))
    )

    df = duplicate_flag(df, "complaint_id")
    df = add_customer_reference_flag(df, spark)

    df = flag(
        df, "is_missing_key",
        F.col("complaint_id").isNull()
    )
    df = flag(
        df, "is_invalid_complaint_type",
        F.col("complaint_type_id").isNull()
        | (F.col("complaint_type_id") <= 0)
    )

    df = flag(
        df, "is_invalid_created_timestamp",
        F.col("created_timestamp").isNull()
    )
    df = flag(
        df, "is_invalid_resolved_timestamp",
        F.col("resolved_timestamp_raw").isNotNull()
        & F.col("resolved_timestamp").isNull()
    )
    df = flag(
        df, "is_resolution_before_creation",
        F.col("resolved_timestamp").isNotNull()
        & (F.col("resolved_timestamp")
           < F.col("created_timestamp"))
    )
    df = flag(
        df, "is_closed_without_resolution",
        (F.col("status") == "CLOSED")
        & F.col("resolved_timestamp").isNull()
    )
    df = flag(
        df, "is_open_with_resolution",
        F.col("status").isin("OPEN", "PENDING")
        & F.col("resolved_timestamp").isNotNull()
    )
    df = flag(
        df, "is_invalid_status",
        ~F.col("status").isin("OPEN", "PENDING", "CLOSED")
        | F.col("status").isNull()
    )
    df = flag(
        df, "is_invalid_priority",
        ~F.col("priority").isin("LOW", "MEDIUM", "HIGH")
        | F.col("priority").isNull()
    )
    df = flag(
        df, "is_invalid_channel",
        F.col("channel").isNotNull()
        & ~F.col("channel").isin(
            "Call Center", "Mobile App", "Web", "Branch"
        )
    )
    df = flag(
        df, "is_missing_complaint_text",
        F.col("complaint_text").isNull()
    )
    df = flag(
        df, "is_standalone_complaint",
        F.col("incident_id").isNull()
    )
    df = flag(
        df, "is_invalid_resolution_minutes",
        F.col("resolution_minutes_raw").isNotNull()
        & (
            F.col("resolution_minutes").isNull()
            | (F.col("resolution_minutes") < 0)
        )
    )
    df = flag(
        df, "is_invalid_sla_breached",
        F.col("sla_breached").isNull()
    )

    # Technical consistency check (tolerance: 1 minute).
    actual_minutes = (
        F.unix_timestamp("resolved_timestamp")
        - F.unix_timestamp("created_timestamp")
    ) / 60.0

    df = flag(
        df, "is_resolution_duration_mismatch",
        F.col("resolved_timestamp").isNotNull()
        & F.col("resolution_minutes").isNotNull()
        & (F.abs(actual_minutes - F.col("resolution_minutes")) > 1)
    )

    # Pending/open complaints have no resolution yet.
    # Don't treat their missing resolution as an error.
    df = flag(
        df, "is_closed_without_duration",
        (F.col("status") == "CLOSED")
        & F.col("resolution_minutes").isNull()
    )

    # Validate incident IDs only when provided.
    incident_ids = (
        spark.read
        .option("header", "true")
        .csv(str(RAW / "network_incidents.csv"))
        .select("incident_id")
        .distinct()
        .withColumnRenamed("incident_id", "_valid_incident_id")
    )

    df = df.join(
        F.broadcast(incident_ids),
        df["incident_id"] == incident_ids["_valid_incident_id"],
        "left"
    )

    df = flag(
        df, "is_invalid_incident_reference",
        F.col("incident_id").isNotNull()
        & F.col("_valid_incident_id").isNull()
    ).drop("_valid_incident_id")

    # Reference validation against existing cleaned types.
    types = (
        spark.read.parquet(str(SILVER / "complaint_types"))
        .select("complaint_type_id")
        .distinct()
        .withColumnRenamed(
            "complaint_type_id", "_valid_complaint_type_id"
        )
    )

    df = df.join(
        F.broadcast(types),
        df["complaint_type_id"] == types["_valid_complaint_type_id"],
        "left"
    )

    df = flag(
        df, "is_unknown_complaint_type",
        F.col("_valid_complaint_type_id").isNull()
    ).drop("_valid_complaint_type_id")

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_invalid_customer_reference",
        "is_invalid_complaint_type",
        "is_unknown_complaint_type",
        "is_invalid_created_timestamp",
        "is_invalid_resolved_timestamp",
        "is_resolution_before_creation",
        "is_closed_without_resolution",
        "is_open_with_resolution",
        "is_closed_without_duration",
        "is_invalid_resolution_minutes",
        "is_invalid_status",
        "is_invalid_priority",
        "is_invalid_sla_breached",
        "is_invalid_incident_reference",
        "is_resolution_duration_mismatch",
    ]

    condition = F.lit(False)
    for name in critical:
        condition = condition | F.col(name)

    df = flag(df, "is_quarantined", condition)

    return save_dataset(df, "complaints")


# ------------------------------------------------------
# Customer Experience
# ------------------------------------------------------

def process_customer_experience(spark):
    print("\n[CUSTOMER EXPERIENCE] Processing...", flush=True)

    df = normalize_all(
        read_csv(spark, "customer_experience", CX_COLUMNS)
    )

    score_ranges = {
        "rating": (1, 5),
        "satisfaction_score": (0, 100),
        "nps_score": (0, 10),
        "call_quality_score": (1, 5),
        "internet_experience_score": (1, 5),
        "network_quality_score": (1, 5),
    }

    df = df.withColumn(
        "event_timestamp_raw", F.col("event_timestamp")
    )
    df = df.withColumn(
        "event_timestamp",
        safe_timestamp("event_timestamp")
    )
    df = df.withColumn(
    "channel",
    F.when(F.upper("channel") == "SMS", "SMS")
    .otherwise(F.initcap(F.lower("channel")))
    )

    for column in score_ranges:
        df = df.withColumn(
            f"{column}_raw", F.col(column)
        )
        df = df.withColumn(
            column,
            safe_int(column) if column == "nps_score"
            else safe_double(column)
        )

    df = duplicate_flag(df, "cx_id")
    df = add_customer_reference_flag(df, spark)

    df = flag(
        df, "is_missing_key",
        F.col("cx_id").isNull()
    )
    df = flag(
        df, "is_invalid_event_timestamp",
        F.col("event_timestamp").isNull()
    )
    df = flag(
        df, "is_invalid_channel",
        F.col("channel").isNull()
        | ~F.col("channel").isin(
            "Mobile App", "SMS", "Call Center", "Survey"
        )
    )

    for column, (minimum, maximum) in score_ranges.items():
        df = flag(
            df, f"is_invalid_{column}",
            F.col(f"{column}_raw").isNotNull()
            & (
                F.col(column).isNull()
                | ~F.col(column).between(minimum, maximum)
            )
        )

        # Missing survey metrics aren't automatically invalid.
        df = flag(
            df, f"is_missing_{column}",
            F.col(f"{column}_raw").isNull()
        )

        # Boundary values are legal, but worth monitoring.
        df = flag(
            df, f"is_boundary_{column}",
            F.col(column).isNotNull()
            & F.col(column).isin(minimum, maximum)
        )

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_invalid_customer_reference",
        "is_invalid_event_timestamp",
        "is_invalid_channel",
    ] + [
        f"is_invalid_{column}" for column in score_ranges
    ]

    condition = F.lit(False)
    for name in critical:
        condition = condition | F.col(name)

    df = flag(df, "is_quarantined", condition)

    return save_dataset(df, "customer_experience")


def main():
    spark = (
        SparkSession.builder
        .appName("Telecom-Customer-Feedback-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        results = {
            "complaints": process_complaints(spark),
            "customer_experience": process_customer_experience(spark),
        }

        report = {
            "pipeline": "customer_feedback_silver",
            "processed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "tables": results,
        }

        REPORT.parent.mkdir(parents=True, exist_ok=True)

        with REPORT.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print("\n=== CUSTOMER FEEDBACK QUALITY REPORT ===")
        print(json.dumps(report, indent=2))
        print("\nPipeline completed successfully.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


"""
Telecom - Spark Silver Incident Impact Pipeline

Inputs:
    data/raw/batch/network_incidents.csv
    data/raw/batch/bridge_incident_customer.csv

Outputs:
    data/processed/silver/network_incidents_reference/
    data/processed/silver/bridge_incident_customer/
    data/processed/quarantine/network_incidents_reference/
    data/processed/quarantine/bridge_incident_customer/
    data/reports/silver/incident_impact_quality.json

The incidents CSV is synthetic ground truth.
It is NOT output from streaming incident detection.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BASE = Path("/opt/telecom/data")
RAW = BASE / "raw/batch"
SILVER = BASE / "processed/silver"
QUARANTINE = BASE / "processed/quarantine"
REPORT = BASE / "reports/silver/incident_impact_quality.json"

INCIDENT_COLUMNS = [
    "incident_id", "tower_id", "area_id", "incident_type_id",
    "start_timestamp", "end_timestamp", "duration_minutes",
    "severity", "max_latency_ms", "avg_latency_ms",
    "avg_signal_strength_dbm", "max_call_drop_rate",
    "avg_download_speed_mbps", "affected_customer_count",
    "complaint_count", "incident_status", "root_cause",
]

BRIDGE_COLUMNS = [
    "incident_id", "customer_id",
    "impact_start_timestamp", "impact_end_timestamp",
    "impact_duration_minutes", "impact_level",
    "impact_score", "service_affected",
]


def read_csv(spark, filename, columns):
    path = RAW / filename

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
            f"{filename}: unexpected columns\n"
            f"Expected: {columns}\n"
            f"Actual: {df.columns}"
        )

    return df


def normalize(df):
    for name in df.columns:
        value = F.regexp_replace(
            F.trim(F.col(name)), r"\s+", " "
        )

        df = df.withColumn(
            name,
            F.when(value == "", F.lit(None)).otherwise(value)
        )

    return df


def as_double(name):
    return F.expr(f"try_cast(`{name}` AS double)")


def as_int(name):
    return F.expr(f"try_cast(`{name}` AS int)")


def as_timestamp(name):
    return F.to_timestamp(
        F.col(name), "yyyy-MM-dd HH:mm:ss"
    )


def as_boolean(name):
    value = F.lower(F.col(name))

    return (
        F.when(value.isin("true", "1", "yes"), True)
        .when(value.isin("false", "0", "no"), False)
        .otherwise(F.lit(None).cast("boolean"))
    )


def add_flag(df, name, condition):
    return df.withColumn(
        name, F.coalesce(condition, F.lit(False))
    )


def mark_duplicates(df, keys):
    return add_flag(
        df,
        "is_duplicate_key",
        F.count("*").over(Window.partitionBy(*keys)) > 1
    )


def reference(spark, table, column):
    return (
        spark.read.parquet(str(SILVER / table))
        .select(column)
        .distinct()
    )


def mark_missing_fk(
    df, parent, child_col, parent_col, flag_name
):
    parent_keys = parent.select(
        F.col(parent_col).alias("_ref_key")
    ).distinct()

    df = df.join(
        parent_keys,
        df[child_col] == parent_keys["_ref_key"],
        "left"
    )

    df = add_flag(
        df,
        flag_name,
        F.col("_ref_key").isNull()
    )

    return df.drop("_ref_key")


def save_dataset(df, name, critical_flags):
    rejected_condition = F.lit(False)

    for column in critical_flags:
        rejected_condition = (
            rejected_condition | F.col(column)
        )

    df = add_flag(
        df, "is_quarantined", rejected_condition
    )

    quality_flags = [
        column
        for column, dtype in df.dtypes
        if dtype == "boolean"
        and column.startswith("is_")
        and column != "is_quarantined"
    ]

    metrics = df.agg(
        F.count("*").alias("raw_rows"),
        F.sum(
            F.when(F.col("is_quarantined"), 1)
            .otherwise(0)
        ).alias("quarantined_rows"),
        *[
            F.sum(
                F.when(F.col(column), 1)
                .otherwise(0)
            ).alias(column)
            for column in quality_flags
        ]
    ).first().asDict()

    total = int(metrics.pop("raw_rows"))
    rejected = int(metrics.pop("quarantined_rows") or 0)

    audit_columns = [
        c for c in df.columns if c.endswith("_raw")
    ]

    accepted_df = (
        df.filter(~F.col("is_quarantined"))
        .drop("is_quarantined", *audit_columns)
    )

    rejected_df = df.filter(F.col("is_quarantined"))

    accepted_df.write.mode("overwrite").parquet(
        str(SILVER / name)
    )

    rejected_df.write.mode("overwrite").parquet(
        str(QUARANTINE / name)
    )

    print(
        f"[{name}] Raw={total:,} "
        f"Silver={total - rejected:,} "
        f"Quarantine={rejected:,}",
        flush=True
    )

    return {
        "raw_rows": total,
        "silver_rows": total - rejected,
        "quarantined_rows": rejected,
        "quality_flags": {
            k: int(v or 0)
            for k, v in metrics.items()
        },
    }


# ======================================================
# INCIDENTS REFERENCE
# ======================================================

def process_incidents(spark):
    print("\n[INCIDENTS] Processing...", flush=True)

    df = normalize(
        read_csv(
            spark,
            "network_incidents.csv",
            INCIDENT_COLUMNS,
        )
    )

    numeric_types = {
        "incident_type_id": "int",
        "duration_minutes": "double",
        "max_latency_ms": "double",
        "avg_latency_ms": "double",
        "avg_signal_strength_dbm": "double",
        "max_call_drop_rate": "double",
        "avg_download_speed_mbps": "double",
        "affected_customer_count": "int",
        "complaint_count": "int",
    }

    for column, dtype in numeric_types.items():
        df = df.withColumn(
            f"{column}_raw", F.col(column)
        )
        df = df.withColumn(
            column,
            as_int(column) if dtype == "int"
            else as_double(column)
        )

    for column in ["start_timestamp", "end_timestamp"]:
        df = df.withColumn(
            f"{column}_raw", F.col(column)
        )
        df = df.withColumn(column, as_timestamp(column))

    df = (
        df
        .withColumn("severity", F.upper("severity"))
        .withColumn(
            "incident_status", F.upper("incident_status")
        )
    )

    df = mark_duplicates(df, ["incident_id"])

    # References to existing accepted Silver tables.
    for table, child_col, parent_col, flag_name in [
        ("towers", "tower_id", "tower_id",
         "is_invalid_tower_reference"),
        ("areas", "area_id", "area_id",
         "is_invalid_area_reference"),
        ("incident_types", "incident_type_id",
         "incident_type_id", "is_invalid_type_reference"),
    ]:
        df = mark_missing_fk(
            df,
            reference(spark, table, parent_col),
            child_col, parent_col, flag_name
        )

    df = add_flag(
        df, "is_missing_key",
        F.col("incident_id").isNull()
    )

    df = add_flag(
        df, "is_invalid_time",
        F.col("start_timestamp").isNull()
        | F.col("end_timestamp").isNull()
        | (
            F.col("end_timestamp")
            < F.col("start_timestamp")
        )
    )

    df = add_flag(
        df, "is_invalid_duration",
        F.col("duration_minutes").isNull()
        | (F.col("duration_minutes") < 0)
    )

    actual_minutes = (
        F.unix_timestamp("end_timestamp")
        - F.unix_timestamp("start_timestamp")
    ) / 60.0

    df = add_flag(
        df, "is_duration_mismatch",
        F.col("duration_minutes").isNotNull()
        & (F.abs(
            F.col("duration_minutes") - actual_minutes
        ) > 1.0)
    )

    df = add_flag(
        df, "is_invalid_severity",
        F.col("severity").isNull()
        | ~F.col("severity").isin(
            "LOW", "MEDIUM", "HIGH", "CRITICAL"
        )
    )

    df = add_flag(
        df, "is_invalid_status",
        F.col("incident_status").isNull()
        | ~F.col("incident_status").isin(
            "OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"
        )
    )

    df = add_flag(
        df, "is_invalid_affected_count",
        F.col("affected_customer_count").isNull()
        | (F.col("affected_customer_count") < 0)
    )

    df = add_flag(
        df, "is_invalid_complaint_count",
        F.col("complaint_count").isNull()
        | (F.col("complaint_count") < 0)
    )

    df = add_flag(
        df, "is_invalid_call_drop_rate",
        F.col("max_call_drop_rate").isNotNull()
        & ~F.col("max_call_drop_rate").between(0, 100)
    )

    df = add_flag(
        df, "is_invalid_download_speed",
        F.col("avg_download_speed_mbps").isNotNull()
        & (F.col("avg_download_speed_mbps") < 0)
    )

    # Outliers are diagnostic, not automatically invalid.
    df = add_flag(
        df, "is_suspicious_latency",
        (F.col("max_latency_ms") > 5000)
        | (F.col("avg_latency_ms") > 2000)
    )

    df = add_flag(
        df, "is_missing_network_metrics",
        F.col("max_latency_ms").isNull()
        | F.col("avg_latency_ms").isNull()
        | F.col("avg_signal_strength_dbm").isNull()
        | F.col("max_call_drop_rate").isNull()
        | F.col("avg_download_speed_mbps").isNull()
    )

    # Check that incident area matches tower's area.
    tower_areas = (
        spark.read.parquet(str(SILVER / "towers"))
        .select(
            "tower_id",
            F.col("area_id").alias("_tower_area_id")
        )
    )

    df = df.join(tower_areas, "tower_id", "left")

    df = add_flag(
        df, "is_tower_area_mismatch",
        F.col("_tower_area_id").isNotNull()
        & (F.col("area_id") != F.col("_tower_area_id"))
    ).drop("_tower_area_id")

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_invalid_tower_reference",
        "is_invalid_area_reference",
        "is_invalid_type_reference",
        "is_invalid_time",
        "is_invalid_duration",
        "is_duration_mismatch",
        "is_invalid_severity",
        "is_invalid_status",
        "is_invalid_affected_count",
        "is_invalid_complaint_count",
        "is_invalid_call_drop_rate",
        "is_invalid_download_speed",
        "is_tower_area_mismatch",
    ]

    return save_dataset(
        df, "network_incidents_reference", critical
    )


# ======================================================
# INCIDENT -> CUSTOMER BRIDGE
# ======================================================

def process_bridge(spark):
    print("\n[BRIDGE] Processing...", flush=True)

    df = normalize(
        read_csv(
            spark,
            "bridge_incident_customer.csv",
            BRIDGE_COLUMNS,
        )
    )

    for column in [
        "impact_start_timestamp",
        "impact_end_timestamp",
    ]:
        df = df.withColumn(
            f"{column}_raw", F.col(column)
        )
        df = df.withColumn(
            column, as_timestamp(column)
        )

    for column in [
        "impact_duration_minutes", "impact_score",
    ]:
        df = df.withColumn(
            f"{column}_raw", F.col(column)
        )
        df = df.withColumn(column, as_double(column))

    df = df.withColumn(
        "impact_level_raw", F.col("impact_level")
    ).withColumn(
        "impact_level", F.upper("impact_level")
    )

    df = df.withColumn(
        "service_affected_raw", F.col("service_affected")
    ).withColumn(
        "service_affected", as_boolean("service_affected")
    )

    df = mark_duplicates(
        df, ["incident_id", "customer_id"]
    )

    # Customer reference and geography.
    customers = (
        spark.read.parquet(str(SILVER / "customers"))
        .select(
            "customer_id",
            F.col("area_id").alias("_customer_area_id")
        )
    )

    df = df.join(customers, "customer_id", "left")

    df = add_flag(
        df, "is_invalid_customer_reference",
        F.col("_customer_area_id").isNull()
    )

    # Incident reference and time boundaries.
    incidents = (
        spark.read.parquet(
            str(SILVER / "network_incidents_reference")
        )
        .select(
            "incident_id",
            F.col("area_id").alias("_incident_area_id"),
            F.col("start_timestamp").alias("_incident_start"),
            F.col("end_timestamp").alias("_incident_end"),
        )
    )

    df = df.join(incidents, "incident_id", "left")

    df = add_flag(
        df, "is_invalid_incident_reference",
        F.col("_incident_start").isNull()
    )

    df = add_flag(
        df, "is_customer_area_mismatch",
        F.col("_customer_area_id").isNotNull()
        & F.col("_incident_area_id").isNotNull()
        & (
            F.col("_customer_area_id")
            != F.col("_incident_area_id")
        )
    )

    # --------------------------------------------------
    # Recover missing duration/end where justified.
    # --------------------------------------------------

    df = add_flag(
        df, "is_missing_impact_end_original",
        F.col("impact_end_timestamp").isNull()
    )

    df = add_flag(
        df, "is_missing_impact_duration_original",
        F.col("impact_duration_minutes").isNull()
    )

    df = add_flag(
        df, "is_missing_impact_level_original",
        F.col("impact_level").isNull()
    )

    # Duration from existing timestamps.
    computed_duration = (
        F.unix_timestamp("impact_end_timestamp")
        - F.unix_timestamp("impact_start_timestamp")
    ) / 60.0

    df = add_flag(
        df, "is_duration_repaired",
        F.col("impact_duration_minutes").isNull()
        & F.col("impact_start_timestamp").isNotNull()
        & F.col("impact_end_timestamp").isNotNull()
        & (computed_duration >= 0)
    )

    df = df.withColumn(
        "impact_duration_minutes",
        F.when(
            F.col("is_duration_repaired"),
            F.round(computed_duration, 2)
        ).otherwise(F.col("impact_duration_minutes"))
    )

    # End from start + known duration.
    df = add_flag(
        df, "is_end_repaired",
        F.col("impact_end_timestamp").isNull()
        & F.col("impact_start_timestamp").isNotNull()
        & F.col("impact_duration_minutes").isNotNull()
        & (F.col("impact_duration_minutes") >= 0)
    )

    df = df.withColumn(
        "impact_end_timestamp",
        F.when(
            F.col("is_end_repaired"),
            F.expr(
                "timestamp_seconds("
                "unix_timestamp(impact_start_timestamp) + "
                "cast(round(impact_duration_minutes * 60) as bigint)"
                ")"
            )
        ).otherwise(F.col("impact_end_timestamp"))
    )

    # The generator uses:
    # HIGH >= 75, MEDIUM >= 45, otherwise LOW.
    expected_level = (
        F.when(F.col("impact_score") >= 75, "HIGH")
        .when(F.col("impact_score") >= 45, "MEDIUM")
        .when(F.col("impact_score").isNotNull(), "LOW")
    )

    df = add_flag(
        df, "is_impact_level_repaired",
        F.col("impact_level").isNull()
        & expected_level.isNotNull()
    )

    df = add_flag(
        df, "is_impact_level_mismatch",
        F.col("impact_level").isNotNull()
        & expected_level.isNotNull()
        & (F.col("impact_level") != expected_level)
    )

    df = df.withColumn(
        "impact_level",
        F.when(
            expected_level.isNotNull(),
            expected_level
        ).otherwise(F.col("impact_level"))
    )

    # --------------------------------------------------
    # Final validation
    # --------------------------------------------------

    df = add_flag(
        df, "is_missing_key",
        F.col("incident_id").isNull()
        | F.col("customer_id").isNull()
    )

    df = add_flag(
        df, "is_invalid_impact_time",
        F.col("impact_start_timestamp").isNull()
        | F.col("impact_end_timestamp").isNull()
        | (
            F.col("impact_end_timestamp")
            < F.col("impact_start_timestamp")
        )
    )

    df = add_flag(
        df, "is_invalid_impact_duration",
        F.col("impact_duration_minutes").isNull()
        | (F.col("impact_duration_minutes") < 0)
    )

    final_minutes = (
        F.unix_timestamp("impact_end_timestamp")
        - F.unix_timestamp("impact_start_timestamp")
    ) / 60.0

    df = add_flag(
        df, "is_impact_duration_mismatch",
        F.col("impact_duration_minutes").isNotNull()
        & F.col("impact_end_timestamp").isNotNull()
        & (
            F.abs(
                F.col("impact_duration_minutes")
                - final_minutes
            ) > 1.0
        )
    )

    # One-second tolerance for timestamp rounding.
    df = add_flag(
        df, "is_outside_incident_window",
        F.col("_incident_start").isNotNull()
        & F.col("_incident_end").isNotNull()
        & (
            (F.unix_timestamp("impact_start_timestamp")
             < F.unix_timestamp("_incident_start") - 1)
            |
            (F.unix_timestamp("impact_end_timestamp")
             > F.unix_timestamp("_incident_end") + 1)
        )
    )

    df = add_flag(
        df, "is_invalid_impact_score",
        F.col("impact_score_raw").isNotNull()
        & (
            F.col("impact_score").isNull()
            | ~F.col("impact_score").between(0, 100)
        )
    )

    df = add_flag(
        df, "is_invalid_impact_level",
        F.col("impact_level").isNotNull()
        & ~F.col("impact_level").isin(
            "LOW", "MEDIUM", "HIGH"
        )
    )

    df = add_flag(
        df, "is_invalid_service_affected",
        F.col("service_affected").isNull()
    )

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_invalid_customer_reference",
        "is_invalid_incident_reference",
        "is_customer_area_mismatch",
        "is_invalid_impact_time",
        "is_invalid_impact_duration",
        "is_impact_duration_mismatch",
        "is_outside_incident_window",
        "is_invalid_impact_score",
        "is_invalid_impact_level",
        "is_invalid_service_affected",
    ]

    df = df.drop(
        "_customer_area_id",
        "_incident_area_id",
        "_incident_start",
        "_incident_end",
    )

    return save_dataset(
        df, "bridge_incident_customer", critical
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Telecom-Incident-Impact-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        incident_result = process_incidents(spark)
        bridge_result = process_bridge(spark)

        report = {
            "pipeline": "incident_impact_silver",
            "source": "synthetic_ground_truth",
            "processed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "tables": {
                "network_incidents_reference": incident_result,
                "bridge_incident_customer": bridge_result,
            },
        }

        REPORT.parent.mkdir(
            parents=True, exist_ok=True
        )

        with REPORT.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print("\n=== INCIDENT IMPACT REPORT ===")
        print(json.dumps(report, indent=2))
        print("\nIncident Impact Pipeline completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

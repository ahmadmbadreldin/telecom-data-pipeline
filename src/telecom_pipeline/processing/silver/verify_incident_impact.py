
"""
Verify Telecom Incident Impact Silver Outputs.

Checks:
- Raw/Silver/Quarantine reconciliation
- Primary keys and composite keys
- Numeric and timestamp data types
- Incident geographical consistency
- Bridge customer/incident references
- Impact windows and durations
- Impact level rules
- Data quality flags and repairs
- Ground-truth metadata

Read-only: does not modify data.
"""

from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    TimestampType,
    DoubleType,
    IntegerType,
    BooleanType,
)

BASE = DATA_ROOT
REPORT = REPORTS / "incident_impact_quality.json"

EXPECTED = {
    "network_incidents_reference": 10000,
    "bridge_incident_customer": 750000,
}


def check(condition, label):
    if not condition:
        raise AssertionError(f"FAIL: {label}")

    print(f"[PASS] {label}", flush=True)


def invalid_count(df, condition):
    return df.filter(
        F.coalesce(condition, F.lit(False))
    ).count()


def check_type(df, col, dtype):
    check(
        isinstance(df.schema[col].dataType, dtype),
        f"{col} has type {dtype.__name__}"
    )


def check_unique(df, keys, name):
    missing = F.lit(False)

    for key in keys:
        missing = (
            missing
            | F.col(key).isNull()
            | (F.trim(F.col(key).cast("string")) == "")
        )

    check(
        invalid_count(df, missing) == 0,
        f"{name}: no missing business keys"
    )

    check(
        df.select(*keys).distinct().count() == df.count(),
        f"{name}: unique business keys"
    )


def verify_counts(spark, name, report):
    print(f"\n=== VERIFY {name.upper()} ===", flush=True)

    df = spark.read.parquet(str(SILVER / name))
    rejected = spark.read.parquet(
        str(QUARANTINE / name)
    )

    accepted_count = df.count()
    rejected_count = rejected.count()
    expected = EXPECTED[name]

    check(
        accepted_count + rejected_count == expected,
        f"{name}: all {expected:,} raw rows accounted for"
    )

    check(
        accepted_count == report["silver_rows"],
        f"{name}: Silver count matches report"
    )

    check(
        rejected_count == report["quarantined_rows"],
        f"{name}: Quarantine count matches report"
    )

    check(
        expected == report["raw_rows"],
        f"{name}: Raw count matches report"
    )

    check(
        "is_quarantined" not in df.columns,
        f"{name}: no quarantine marker in Silver"
    )

    return df, rejected


def verify_incidents(spark, df):
    print("\n--- INCIDENT REFERENCE RULES ---")

    check_unique(
        df, ["incident_id"], "network_incidents"
    )

    for col in [
        "start_timestamp", "end_timestamp",
    ]:
        check_type(df, col, TimestampType)

    for col in [
        "duration_minutes",
        "max_latency_ms",
        "avg_latency_ms",
        "avg_signal_strength_dbm",
        "max_call_drop_rate",
        "avg_download_speed_mbps",
    ]:
        check_type(df, col, DoubleType)

    for col in [
        "incident_type_id",
        "affected_customer_count",
        "complaint_count",
    ]:
        check_type(df, col, IntegerType)

    invalid_times = invalid_count(
        df,
        F.col("start_timestamp").isNull()
        | F.col("end_timestamp").isNull()
        | (
            F.col("end_timestamp")
            < F.col("start_timestamp")
        )
    )

    check(invalid_times == 0, "Incident chronology valid")

    duration = (
        F.unix_timestamp("end_timestamp")
        - F.unix_timestamp("start_timestamp")
    ) / 60.0

    invalid_duration = invalid_count(
        df,
        F.col("duration_minutes").isNull()
        | (F.col("duration_minutes") < 0)
        | (
            F.abs(
                F.col("duration_minutes") - duration
            ) > 1.0
        )
    )

    check(
        invalid_duration == 0,
        "Incident durations consistent"
    )

    invalid_severity = invalid_count(
        df,
        F.col("severity").isNull()
        | ~F.col("severity").isin(
            "LOW", "MEDIUM", "HIGH", "CRITICAL"
        )
    )

    check(
        invalid_severity == 0,
        "Incident severity valid"
    )

    towers = (
        spark.read.parquet(str(SILVER / "towers"))
        .select(
            "tower_id",
            F.col("area_id").alias("tower_area")
        )
    )

    joined = df.join(towers, "tower_id", "left")

    invalid_geography = invalid_count(
        joined,
        F.col("tower_area").isNull()
        | (F.col("area_id") != F.col("tower_area"))
    )

    check(
        invalid_geography == 0,
        "Incident tower/area relationships valid"
    )

    types = (
        spark.read.parquet(str(SILVER / "incident_types"))
        .select("incident_type_id")
    )

    missing_types = df.join(
        types, "incident_type_id", "left_anti"
    ).count()

    check(
        missing_types == 0,
        "Incident type references valid"
    )

    invalid_counts = invalid_count(
        df,
        F.col("affected_customer_count").isNull()
        | (F.col("affected_customer_count") < 0)
        | F.col("complaint_count").isNull()
        | (F.col("complaint_count") < 0)
    )

    check(
        invalid_counts == 0,
        "Incident count measures valid"
    )

    print(
        "Suspicious latency rows:",
        invalid_count(df, F.col("is_suspicious_latency"))
    )


def verify_bridge(spark, df, incidents):
    print("\n--- INCIDENT-CUSTOMER BRIDGE RULES ---")

    check_unique(
        df,
        ["incident_id", "customer_id"],
        "bridge_incident_customer"
    )

    for col in [
        "impact_start_timestamp",
        "impact_end_timestamp",
    ]:
        check_type(df, col, TimestampType)

    for col in [
        "impact_duration_minutes", "impact_score",
    ]:
        check_type(df, col, DoubleType)

    check_type(
        df, "service_affected", BooleanType
    )

    customers = (
        spark.read.parquet(str(SILVER / "customers"))
        .select(
            "customer_id",
            F.col("area_id").alias("customer_area")
        )
    )

    incident_info = incidents.select(
        "incident_id",
        F.col("area_id").alias("incident_area"),
        F.col("start_timestamp").alias("incident_start"),
        F.col("end_timestamp").alias("incident_end"),
    )

    joined = (
        df.join(customers, "customer_id", "left")
        .join(incident_info, "incident_id", "left")
    )

    invalid_references = invalid_count(
        joined,
        F.col("customer_area").isNull()
        | F.col("incident_area").isNull()
    )

    check(
        invalid_references == 0,
        "Bridge customer and incident FKs valid"
    )

    wrong_area = invalid_count(
        joined,
        F.col("customer_area") != F.col("incident_area")
    )

    check(
        wrong_area == 0,
        "Affected customers belong to incident area"
    )

    invalid_times = invalid_count(
        joined,
        F.col("impact_start_timestamp").isNull()
        | F.col("impact_end_timestamp").isNull()
        | (
            F.col("impact_end_timestamp")
            < F.col("impact_start_timestamp")
        )
    )

    check(
        invalid_times == 0,
        "Impact chronology valid"
    )

    outside_window = invalid_count(
        joined,
        (
            F.unix_timestamp("impact_start_timestamp")
            < F.unix_timestamp("incident_start") - 1
        )
        |
        (
            F.unix_timestamp("impact_end_timestamp")
            > F.unix_timestamp("incident_end") + 1
        )
    )

    check(
        outside_window == 0,
        "Impact windows inside incident windows"
    )

    actual_duration = (
        F.unix_timestamp("impact_end_timestamp")
        - F.unix_timestamp("impact_start_timestamp")
    ) / 60.0

    invalid_duration = invalid_count(
        joined,
        F.col("impact_duration_minutes").isNull()
        | (F.col("impact_duration_minutes") < 0)
        | (
            F.abs(
                F.col("impact_duration_minutes")
                - actual_duration
            ) > 1.0
        )
    )

    check(
        invalid_duration == 0,
        "Impact durations consistent"
    )

    invalid_score = invalid_count(
        df,
        F.col("impact_score").isNotNull()
        & ~F.col("impact_score").between(0, 100)
    )

    check(invalid_score == 0, "Impact scores valid")

    expected_level = (
        F.when(F.col("impact_score") >= 75, "HIGH")
        .when(F.col("impact_score") >= 45, "MEDIUM")
        .when(F.col("impact_score").isNotNull(), "LOW")
    )

    mismatched_levels = invalid_count(
        df,
        expected_level.isNotNull()
        & (F.col("impact_level") != expected_level)
    )

    check(
        mismatched_levels == 0,
        "Impact levels match impact scores"
    )

    invalid_levels = invalid_count(
        df,
        F.col("impact_level").isNotNull()
        & ~F.col("impact_level").isin(
            "LOW", "MEDIUM", "HIGH"
        )
    )

    check(invalid_levels == 0, "Impact levels valid")

    invalid_service = invalid_count(
        df,
        F.col("service_affected").isNull()
    )

    check(
        invalid_service == 0,
        "Service affected flag valid"
    )

    # Verify repaired values actually exist.
    missing_repaired_end = invalid_count(
        df,
        F.col("is_end_repaired")
        & F.col("impact_end_timestamp").isNull()
    )

    check(
        missing_repaired_end == 0,
        "Repaired impact end timestamps populated"
    )

    missing_repaired_duration = invalid_count(
        df,
        F.col("is_duration_repaired")
        & F.col("impact_duration_minutes").isNull()
    )

    check(
        missing_repaired_duration == 0,
        "Repaired impact durations populated"
    )

    missing_repaired_level = invalid_count(
        df,
        F.col("is_impact_level_repaired")
        & F.col("impact_level").isNull()
    )

    check(
        missing_repaired_level == 0,
        "Repaired impact levels populated"
    )

    print(
        "Missing impact scores:",
        invalid_count(df, F.col("impact_score").isNull())
    )

    print(
        "Recovered durations:",
        invalid_count(df, F.col("is_duration_repaired"))
    )

    print(
        "Recovered end timestamps:",
        invalid_count(df, F.col("is_end_repaired"))
    )

    print(
        "Recovered impact levels:",
        invalid_count(
            df, F.col("is_impact_level_repaired")
        )
    )


def verify_aggregate_relationships(spark, incidents, bridge):
    """
    Ground-truth consistency check.

    This uses the synthetic incident dataset.
    Aggregation here is only a verification test,
    not a published dbt analytical model.
    """
    print("\n--- GROUND TRUTH RECONCILIATION ---")

    counts = (
        bridge.groupBy("incident_id")
        .agg(
            F.countDistinct("customer_id").alias(
                "actual_affected_customers"
            )
        )
    )

    compared = incidents.join(
        counts, "incident_id", "left"
    )

    discrepancies = invalid_count(
        compared,
        F.coalesce(
            F.col("actual_affected_customers"),
            F.lit(0)
        ) != F.col("affected_customer_count")
    )

    check(
        discrepancies == 0,
        "Incident affected counts match bridge records"
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-Telecom-Incident-Impact")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        with REPORT.open("r", encoding="utf-8") as f:
            report = json.load(f)

        check(
            report["source"] == "synthetic_ground_truth",
            "Reference incident source correctly identified"
        )

        incidents, rejected_incidents = verify_counts(
            spark,
            "network_incidents_reference",
            report["tables"]["network_incidents_reference"],
        )

        bridge, rejected_bridge = verify_counts(
            spark,
            "bridge_incident_customer",
            report["tables"]["bridge_incident_customer"],
        )

        verify_incidents(spark, incidents)
        verify_bridge(spark, bridge, incidents)

        # Run count reconciliation when all reference
        # incidents and bridge records were accepted.
        if (
            rejected_incidents.count() == 0
            and rejected_bridge.count() == 0
        ):
            verify_aggregate_relationships(
                spark, incidents, bridge
            )
        else:
            print(
                "[INFO] Ground-truth count reconciliation "
                "requires investigation of quarantine."
            )

        print("\n========================================")
        print("ALL INCIDENT IMPACT CHECKS PASSED")
        print("========================================", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


"""
Verify Customer Feedback Silver Outputs

Read-only verification:
- Complaints
- Customer Experience
- Quality report reconciliation
- Primary keys
- Data types
- Business rules
- Referential integrity
"""

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

BASE = Path("/opt/telecom/data")
SILVER = BASE / "processed/silver"
QUARANTINE = BASE / "processed/quarantine"
REPORT = BASE / "reports/silver/customer_feedback_quality.json"

EXPECTED_RAW = {
    "complaints": 75000,
    "customer_experience": 150000,
}

KEYS = {
    "complaints": "complaint_id",
    "customer_experience": "cx_id",
}


def check(condition, message):
    if not condition:
        raise AssertionError(f"FAIL: {message}")
    print(f"[PASS] {message}", flush=True)


def count_where(df, condition):
    return df.filter(condition).count()


def check_type(df, name, expected):
    check(
        isinstance(df.schema[name].dataType, expected),
        f"{name}: type is {expected.__name__}"
    )


def verify_common(spark, table, report):
    print(f"\n=== VERIFYING {table.upper()} ===", flush=True)

    df = spark.read.parquet(str(SILVER / table))
    quarantine = spark.read.parquet(
        str(QUARANTINE / table)
    )

    silver_rows = df.count()
    quarantined_rows = quarantine.count()
    raw_rows = EXPECTED_RAW[table]

    check(
        silver_rows + quarantined_rows == raw_rows,
        f"{table}: no unexplained row loss"
    )
    check(
        silver_rows == report["silver_rows"],
        f"{table}: Silver matches quality report"
    )
    check(
        quarantined_rows == report["quarantined_rows"],
        f"{table}: Quarantine matches quality report"
    )
    check(
        raw_rows == report["raw_rows"],
        f"{table}: Raw count matches quality report"
    )

    key = KEYS[table]

    check(
        count_where(
            df,
            F.col(key).isNull()
            | (F.trim(F.col(key)) == "")
        ) == 0,
        f"{table}: no missing primary keys"
    )

    check(
        df.select(key).distinct().count() == silver_rows,
        f"{table}: unique primary keys"
    )

    check(
        count_where(
            df, F.col("is_invalid_customer_reference")
        ) == 0,
        f"{table}: customer references valid"
    )

    customers = (
        spark.read.parquet(str(SILVER / "customers"))
        .select("customer_id")
        .distinct()
    )

    invalid_fk = df.join(
        customers, on="customer_id", how="left_anti"
    ).count()

    check(
        invalid_fk == 0,
        f"{table}: FK customer_id verified independently"
    )

    return df


def verify_complaints(df):
    print("\n--- COMPLAINT RULES ---")

    check_type(df, "complaint_type_id", IntegerType)
    check_type(df, "created_timestamp", TimestampType)
    check_type(df, "resolved_timestamp", TimestampType)
    check_type(df, "resolution_minutes", DoubleType)
    check_type(df, "sla_breached", BooleanType)

    invalid_status = count_where(
        df,
        F.col("status").isNull()
        | ~F.col("status").isin("OPEN", "PENDING", "CLOSED")
    )
    check(invalid_status == 0, "Valid complaint status")

    invalid_priority = count_where(
        df,
        F.col("priority").isNull()
        | ~F.col("priority").isin("LOW", "MEDIUM", "HIGH")
    )
    check(invalid_priority == 0, "Valid priority")

    invalid_times = count_where(
        df,
        F.col("created_timestamp").isNull()
        | (
            F.col("resolved_timestamp").isNotNull()
            & (
                F.col("resolved_timestamp")
                < F.col("created_timestamp")
            )
        )
    )
    check(invalid_times == 0, "Valid resolution chronology")

    invalid_closed = count_where(
        df,
        (F.col("status") == "CLOSED")
        & (
            F.col("resolved_timestamp").isNull()
            | F.col("resolution_minutes").isNull()
        )
    )
    check(invalid_closed == 0, "Closed complaints resolved")

    invalid_open = count_where(
        df,
        F.col("status").isin("OPEN", "PENDING")
        & F.col("resolved_timestamp").isNotNull()
    )
    check(invalid_open == 0, "Open complaints not resolved")

    invalid_duration = count_where(
        df,
        F.col("resolution_minutes").isNotNull()
        & (F.col("resolution_minutes") < 0)
    )
    check(invalid_duration == 0, "Valid resolution duration")

    invalid_incident_flags = count_where(
        df,
        F.col("is_invalid_incident_reference")
        | F.col("is_unknown_complaint_type")
    )
    check(
        invalid_incident_flags == 0,
        "Valid incident and complaint type references"
    )

    incorrect_standalone = count_where(
        df,
        F.col("is_standalone_complaint")
        != F.col("incident_id").isNull()
    )
    check(
        incorrect_standalone == 0,
        "Standalone complaint flags correct"
    )

    print(
        "Standalone complaints:",
        count_where(df, F.col("incident_id").isNull())
    )
    print(
        "Unresolved complaints:",
        count_where(df, F.col("status").isin("OPEN", "PENDING"))
    )


def verify_customer_experience(df):
    print("\n--- CUSTOMER EXPERIENCE RULES ---")

    check_type(df, "event_timestamp", TimestampType)
    check_type(df, "nps_score", IntegerType)

    ranges = {
        "rating": (1, 5),
        "satisfaction_score": (0, 100),
        "nps_score": (0, 10),
        "call_quality_score": (1, 5),
        "internet_experience_score": (1, 5),
        "network_quality_score": (1, 5),
    }

    for column, (minimum, maximum) in ranges.items():
        if column != "nps_score":
            check_type(df, column, DoubleType)

        invalid = count_where(
            df,
            F.col(column).isNotNull()
            & ~F.col(column).between(minimum, maximum)
        )

        check(
            invalid == 0,
            f"{column}: valid range"
        )

        incorrect_missing_flag = count_where(
            df,
            F.col(f"is_missing_{column}")
            != F.col(column).isNull()
        )

        check(
            incorrect_missing_flag == 0,
            f"{column}: missing-value flags correct"
        )

    invalid_channel = count_where(
        df,
        F.col("channel").isNull()
        | ~F.col("channel").isin(
            "Mobile App", "SMS", "Call Center", "Survey"
        )
    )
    check(invalid_channel == 0, "Valid CX channels")

    check(
        count_where(
            df,
            F.col("event_timestamp").isNull()
        ) == 0,
        "Valid CX timestamps"
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-Customer-Feedback-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        with REPORT.open("r", encoding="utf-8") as f:
            report = json.load(f)["tables"]

        complaints = verify_common(
            spark, "complaints", report["complaints"]
        )

        cx = verify_common(
            spark,
            "customer_experience",
            report["customer_experience"]
        )

        verify_complaints(complaints)
        verify_customer_experience(cx)

        print("\n======================================")
        print("ALL CUSTOMER FEEDBACK CHECKS PASSED")
        print("======================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

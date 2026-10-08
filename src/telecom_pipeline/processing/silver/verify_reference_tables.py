
"""
Telecom Data Pipeline
Verify Spark Silver - Reference Tables

Tables:
    plans
    services
    incident_types
    complaint_types

Checks:
    - Row counts
    - Primary key uniqueness
    - Missing primary keys
    - Explicit data types
    - Business constraints
    - Data quality flags
    - Unlimited plan handling
    - Quarantine counts

This script reads Parquet only.
It does not modify any dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
)


BASE = "/opt/telecom/data/processed"

SILVER = f"{BASE}/silver"
QUARANTINE = f"{BASE}/quarantine"

TABLES = {
    "plans": {
        "expected_rows": 8,
        "primary_key": "plan_id",
    },
    "services": {
        "expected_rows": 5,
        "primary_key": "service_id",
    },
    "incident_types": {
        "expected_rows": 5,
        "primary_key": "incident_type_id",
    },
    "complaint_types": {
        "expected_rows": 5,
        "primary_key": "complaint_type_id",
    },
}


def verify(condition, message):
    if not condition:
        raise AssertionError(message)

    print(f"[PASS] {message}", flush=True)


def verify_schema(df, column, expected_type):
    actual_type = df.schema[column].dataType

    verify(
        isinstance(actual_type, expected_type),
        f"{column} data type = {actual_type}"
    )


def verify_primary_key(df, table, key):
    total = df.count()

    missing_keys = df.filter(
        F.col(key).isNull()
        | (F.trim(F.col(key).cast("string")) == "")
    ).count()

    unique_keys = df.select(
        key
    ).distinct().count()

    verify(
        missing_keys == 0,
        f"{table}: no missing primary keys"
    )

    verify(
        unique_keys == total,
        f"{table}: primary keys are unique"
    )


def verify_quality_flags(df, table):
    if "is_duplicate_key" in df.columns:
        count = df.filter(
            F.col("is_duplicate_key") == True
        ).count()

        verify(
            count == 0,
            f"{table}: no duplicate key flags"
        )

    for field in df.schema.fields:
        column = field.name

        if (
            column.startswith("is_invalid_")
            and isinstance(field.dataType, BooleanType)
        ):
            failed = df.filter(
                F.col(column) == True
            ).count()

            verify(
                failed == 0,
                f"{table}: {column} = 0"
            )


def verify_table(spark, table, config):
    print(
        f"\n========== VERIFYING {table.upper()} ==========",
        flush=True
    )

    df = spark.read.parquet(
        f"{SILVER}/{table}"
    )

    quarantine = spark.read.parquet(
        f"{QUARANTINE}/{table}"
    )

    df.printSchema()

    total = df.count()
    rejected = quarantine.count()
    expected = config["expected_rows"]

    verify(
        total == expected,
        f"{table}: row count = {expected}"
    )

    verify(
        rejected == 0,
        f"{table}: quarantine count = 0"
    )

    verify_primary_key(
        df,
        table,
        config["primary_key"]
    )

    verify_quality_flags(df, table)

    return df


def verify_plans(df):
    print("\n--- PLANS BUSINESS CHECKS ---")

    for column in [
        "monthly_fee",
        "data_limit_gb",
        "speed_limit_mbps",
    ]:
        verify_schema(
            df, column, DecimalType
        )

    for column in [
        "voice_minutes",
        "sms_limit",
    ]:
        verify_schema(
            df, column, IntegerType
        )

    for column in [
        "effective_from",
        "effective_to",
    ]:
        verify_schema(
            df, column, DateType
        )

    verify_schema(
        df, "is_active", BooleanType
    )

    invalid_fee = df.filter(
        F.col("monthly_fee").isNull()
        | (F.col("monthly_fee") < 0)
    ).count()

    verify(
        invalid_fee == 0,
        "plans: all monthly fees are valid"
    )

    unlimited = df.filter(
        F.col("is_unlimited_data") == True
    )

    unlimited_count = unlimited.count()

    verify(
        unlimited_count == 1,
        "plans: exactly one unlimited plan"
    )

    incorrect_unlimited = df.filter(
        F.col("is_unlimited_data").isNull()
        | (
            F.col("is_unlimited_data")
            != F.col("data_limit_gb").isNull()
        )
    ).count()

    verify(
        incorrect_unlimited == 0,
        "plans: unlimited flag matches data limit"
    )

    vip_plan = df.filter(
        F.col("plan_id") == "PLAN-VIP-UNL"
    )

    verify(
        vip_plan.count() == 1,
        "plans: VIP Unlimited exists"
    )

    verify(
        vip_plan.filter(
            F.col("data_limit_gb").isNull()
            & (F.col("is_unlimited_data") == True)
        ).count() == 1,
        "plans: VIP Unlimited retains NULL data limit"
    )

    invalid_dates = df.filter(
        F.col("effective_from").isNull()
        | (
            F.col("effective_to").isNotNull()
            & (
                F.col("effective_to")
                < F.col("effective_from")
            )
        )
    ).count()

    verify(
        invalid_dates == 0,
        "plans: effective dates are valid"
    )

    verify(
        df.filter(
            F.col("is_active").isNull()
        ).count() == 0,
        "plans: active flags are valid booleans"
    )


def verify_services(df):
    print("\n--- SERVICES BUSINESS CHECKS ---")

    verify_schema(
        df, "is_active", BooleanType
    )

    invalid_categories = df.filter(
        F.col("service_category").isNull()
        | ~F.col("service_category").isin(
            "Voice", "Data", "Internet"
        )
    ).count()

    verify(
        invalid_categories == 0,
        "services: categories are valid"
    )

    invalid_technologies = df.filter(
        F.col("technology").isNull()
        | ~F.col("technology").isin(
            "LTE", "4G", "5G", "FIBER"
        )
    ).count()

    verify(
        invalid_technologies == 0,
        "services: technologies are valid"
    )

    verify(
        df.filter(
            F.col("is_active").isNull()
        ).count() == 0,
        "services: active flags are valid"
    )


def verify_incident_types(df):
    print("\n--- INCIDENT TYPES BUSINESS CHECKS ---")

    verify_schema(
        df, "incident_type_id", IntegerType
    )

    invalid_severity = df.filter(
        F.col("default_severity").isNull()
        | ~F.col("default_severity").isin(
            "LOW", "MEDIUM", "HIGH", "CRITICAL"
        )
    ).count()

    verify(
        invalid_severity == 0,
        "incident_types: severities are valid"
    )

    invalid_categories = df.filter(
        F.col("category").isNull()
        | ~F.col("category").isin(
            "Performance", "Quality", "Availability"
        )
    ).count()

    verify(
        invalid_categories == 0,
        "incident_types: categories are valid"
    )


def verify_complaint_types(df):
    print("\n--- COMPLAINT TYPES BUSINESS CHECKS ---")

    verify_schema(
        df, "complaint_type_id", IntegerType
    )

    invalid_priority = df.filter(
        F.col("default_priority").isNull()
        | ~F.col("default_priority").isin(
            "LOW", "MEDIUM", "HIGH"
        )
    ).count()

    verify(
        invalid_priority == 0,
        "complaint_types: priorities are valid"
    )

    invalid_categories = df.filter(
        F.col("category").isNull()
        | ~F.col("category").isin(
            "Network", "Billing", "Service"
        )
    ).count()

    verify(
        invalid_categories == 0,
        "complaint_types: categories are valid"
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-Reference-Tables-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        datasets = {}

        for table, config in TABLES.items():
            datasets[table] = verify_table(
                spark,
                table,
                config
            )

        verify_plans(
            datasets["plans"]
        )

        verify_services(
            datasets["services"]
        )

        verify_incident_types(
            datasets["incident_types"]
        )

        verify_complaint_types(
            datasets["complaint_types"]
        )

        print(
            "\n=========================================="
        )
        print(
            "ALL REFERENCE TABLE CHECKS PASSED"
        )
        print(
            "==========================================",
            flush=True
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


"""
Telecom Data Pipeline
Spark Silver - Reference Tables

Tables:
    plans
    services
    incident_types
    complaint_types

Responsibilities:
    - Read raw CSV files
    - Normalize text
    - Apply explicit data types
    - Validate business rules
    - Detect duplicate keys
    - Separate Silver and Quarantine
    - Generate quality report

Final dimensions and business KPIs belong to dbt.
"""

from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


# ============================================================
# CONFIGURATION
# ============================================================

BASE = DATA_ROOT
REPORT = REPORTS / "reference_tables_quality.json"

CONFIG = {
    "plans": {
        "key": "plan_id",
        "columns": [
            "plan_id",
            "plan_name",
            "plan_type",
            "monthly_fee",
            "data_limit_gb",
            "voice_minutes",
            "sms_limit",
            "speed_limit_mbps",
            "effective_from",
            "effective_to",
            "is_active",
        ],
    },
    "services": {
        "key": "service_id",
        "columns": [
            "service_id",
            "service_name",
            "service_category",
            "technology",
            "description",
            "is_active",
        ],
    },
    "incident_types": {
        "key": "incident_type_id",
        "columns": [
            "incident_type_id",
            "incident_type_code",
            "incident_type_name",
            "category",
            "default_severity",
            "description",
        ],
    },
    "complaint_types": {
        "key": "complaint_type_id",
        "columns": [
            "complaint_type_id",
            "complaint_code",
            "complaint_name",
            "category",
            "default_priority",
            "description",
        ],
    },
}


# ============================================================
# COMMON FUNCTIONS
# ============================================================

def read_csv(spark, table):
    path = RAW / f"{table}.csv"

    if not path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {path}"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(path))
    )

    expected = CONFIG[table]["columns"]

    if df.columns != expected:
        raise ValueError(
            f"{table}: CSV schema mismatch.\n"
            f"Expected: {expected}\n"
            f"Actual: {df.columns}"
        )

    return df


def clean_string(name):
    value = F.regexp_replace(
        F.trim(F.col(name)),
        r"\s+",
        " "
    )

    return F.when(
        value == "",
        F.lit(None)
    ).otherwise(value)


def normalize_text(df):
    for column in df.columns:
        df = df.withColumn(
            column,
            clean_string(column)
        )

    return df


def title_case(name):
    return F.initcap(F.lower(F.col(name)))


def uppercase(name):
    return F.upper(F.col(name))


def cast_number(name, sql_type):
    return F.expr(
        f"try_cast(`{name}` AS {sql_type})"
    )


def cast_date(name):
    return F.expr(
        f"try_cast(`{name}` AS DATE)"
    )


def cast_boolean(name):
    value = F.lower(F.trim(F.col(name)))

    return (
        F.when(
            value.isin("true", "1", "yes"),
            F.lit(True)
        )
        .when(
            value.isin("false", "0", "no"),
            F.lit(False)
        )
        .otherwise(
            F.lit(None).cast("boolean")
        )
    )


def flag_duplicate_keys(df, key):
    window = Window.partitionBy(key)

    return df.withColumn(
        "is_duplicate_key",
        F.count("*").over(window) > 1
    )


def add_flags(df, expressions):
    for flag_name, expression in expressions.items():
        df = df.withColumn(
            flag_name,
            F.coalesce(
                expression,
                F.lit(False)
            )
        )

    return df


def write_parquet(df, path):
    (
        df.write
        .mode("overwrite")
        .parquet(str(path))
    )


# ============================================================
# PLANS
# ============================================================

def clean_plans(df):

    print(
        "[PLANS] Cleaning and validating",
        flush=True
    )

    df = normalize_text(df)

    # Preserve original values for auditing.
    raw_columns = [
        "monthly_fee",
        "data_limit_gb",
        "voice_minutes",
        "sms_limit",
        "speed_limit_mbps",
        "effective_from",
        "effective_to",
        "is_active",
    ]

    for col in raw_columns:
        df = df.withColumn(
            f"{col}_raw",
            F.col(col)
        )

    df = (
        df
        .withColumn(
            "plan_name",
            title_case("plan_name")
        )
        .withColumn(
            "plan_type",
            title_case("plan_type")
        )
        .withColumn(
            "monthly_fee",
            cast_number(
                "monthly_fee",
                "decimal(12,2)"
            )
        )
        .withColumn(
            "data_limit_gb",
            cast_number(
                "data_limit_gb",
                "decimal(12,2)"
            )
        )
        .withColumn(
            "voice_minutes",
            cast_number(
                "voice_minutes",
                "int"
            )
        )
        .withColumn(
            "sms_limit",
            cast_number(
                "sms_limit",
                "int"
            )
        )
        .withColumn(
            "speed_limit_mbps",
            cast_number(
                "speed_limit_mbps",
                "decimal(12,2)"
            )
        )
        .withColumn(
            "effective_from",
            cast_date("effective_from")
        )
        .withColumn(
            "effective_to",
            cast_date("effective_to")
        )
        .withColumn(
            "is_active",
            cast_boolean("is_active")
        )
    )

    df = flag_duplicate_keys(
        df,
        "plan_id"
    )

    df = add_flags(df, {
        "is_missing_key":
            F.col("plan_id").isNull(),

        "is_missing_required_name":
            F.col("plan_name").isNull(),

        "is_invalid_plan_type":
            F.col("plan_type").isNull()
            | ~F.col("plan_type").isin(
                "Prepaid",
                "Postpaid"
            ),

        "is_invalid_monthly_fee":
            F.col("monthly_fee").isNull()
            | (F.col("monthly_fee") < 0),

        "is_invalid_data_limit":
            F.col("data_limit_gb_raw").isNotNull()
            & (
                F.col("data_limit_gb").isNull()
                | (F.col("data_limit_gb") < 0)
            ),

        "is_invalid_voice_minutes":
            F.col("voice_minutes").isNull()
            | (F.col("voice_minutes") < 0),

        "is_invalid_sms_limit":
            F.col("sms_limit").isNull()
            | (F.col("sms_limit") < 0),

        "is_invalid_speed_limit":
            F.col("speed_limit_mbps").isNull()
            | (F.col("speed_limit_mbps") < 0),

        "is_invalid_effective_from":
            F.col("effective_from").isNull(),

        "is_invalid_effective_to":
            F.col("effective_to_raw").isNotNull()
            & F.col("effective_to").isNull(),

        "is_invalid_date_range":
            F.col("effective_to").isNotNull()
            & (
                F.col("effective_to")
                < F.col("effective_from")
            ),

        "is_invalid_active_flag":
            F.col("is_active").isNull(),

        # Null is valid for unlimited data plans.
        "is_unlimited_data":
            F.col("data_limit_gb").isNull()
            & F.col("data_limit_gb_raw").isNull(),
    })

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_missing_required_name",
        "is_invalid_plan_type",
        "is_invalid_monthly_fee",
        "is_invalid_data_limit",
        "is_invalid_voice_minutes",
        "is_invalid_sms_limit",
        "is_invalid_speed_limit",
        "is_invalid_effective_from",
        "is_invalid_effective_to",
        "is_invalid_date_range",
        "is_invalid_active_flag",
    ]

    return df, critical


# ============================================================
# SERVICES
# ============================================================

def clean_services(df):

    print(
        "[SERVICES] Cleaning and validating",
        flush=True
    )

    df = normalize_text(df)

    df = df.withColumn(
        "is_active_raw",
        F.col("is_active")
    )

    df = (
        df
        .withColumn(
            "service_name",
            title_case("service_name")
        )
        .withColumn(
            "service_category",
            title_case("service_category")
        )
        .withColumn(
            "technology",
            uppercase("technology")
        )
        .withColumn(
            "is_active",
            cast_boolean("is_active")
        )
    )

    df = flag_duplicate_keys(
        df,
        "service_id"
    )

    df = add_flags(df, {
        "is_missing_key":
            F.col("service_id").isNull(),

        "is_missing_service_name":
            F.col("service_name").isNull(),

        "is_invalid_service_category":
            F.col("service_category").isNull()
            | ~F.col("service_category").isin(
                "Voice",
                "Data",
                "Internet"
            ),

        "is_invalid_technology":
            F.col("technology").isNull()
            | ~F.col("technology").isin(
                "LTE",
                "4G",
                "5G",
                "FIBER"
            ),

        "is_invalid_active_flag":
            F.col("is_active").isNull(),
    })

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_missing_service_name",
        "is_invalid_service_category",
        "is_invalid_technology",
        "is_invalid_active_flag",
    ]

    return df, critical


# ============================================================
# INCIDENT TYPES
# ============================================================

def clean_incident_types(df):

    print(
        "[INCIDENT TYPES] Cleaning and validating",
        flush=True
    )

    df = normalize_text(df)

    df = df.withColumn(
        "incident_type_id_raw",
        F.col("incident_type_id")
    )

    df = (
        df
        .withColumn(
            "incident_type_id",
            cast_number(
                "incident_type_id",
                "int"
            )
        )
        .withColumn(
            "incident_type_code",
            uppercase("incident_type_code")
        )
        .withColumn(
            "incident_type_name",
            title_case("incident_type_name")
        )
        .withColumn(
            "category",
            title_case("category")
        )
        .withColumn(
            "default_severity",
            uppercase("default_severity")
        )
    )

    df = flag_duplicate_keys(
        df,
        "incident_type_id"
    )

    df = add_flags(df, {
        "is_missing_key":
            F.col("incident_type_id").isNull()
            | (F.col("incident_type_id") <= 0),

        "is_missing_code":
            F.col("incident_type_code").isNull(),

        "is_missing_name":
            F.col("incident_type_name").isNull(),

        "is_invalid_category":
            F.col("category").isNull()
            | ~F.col("category").isin(
                "Performance",
                "Quality",
                "Availability"
            ),

        "is_invalid_severity":
            F.col("default_severity").isNull()
            | ~F.col("default_severity").isin(
                "LOW",
                "MEDIUM",
                "HIGH",
                "CRITICAL"
            ),
    })

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_missing_code",
        "is_missing_name",
        "is_invalid_category",
        "is_invalid_severity",
    ]

    return df, critical


# ============================================================
# COMPLAINT TYPES
# ============================================================

def clean_complaint_types(df):

    print(
        "[COMPLAINT TYPES] Cleaning and validating",
        flush=True
    )

    df = normalize_text(df)

    df = df.withColumn(
        "complaint_type_id_raw",
        F.col("complaint_type_id")
    )

    df = (
        df
        .withColumn(
            "complaint_type_id",
            cast_number(
                "complaint_type_id",
                "int"
            )
        )
        .withColumn(
            "complaint_code",
            uppercase("complaint_code")
        )
        .withColumn(
            "complaint_name",
            title_case("complaint_name")
        )
        .withColumn(
            "category",
            title_case("category")
        )
        .withColumn(
            "default_priority",
            uppercase("default_priority")
        )
    )

    df = flag_duplicate_keys(
        df,
        "complaint_type_id"
    )

    df = add_flags(df, {
        "is_missing_key":
            F.col("complaint_type_id").isNull()
            | (F.col("complaint_type_id") <= 0),

        "is_missing_code":
            F.col("complaint_code").isNull(),

        "is_missing_name":
            F.col("complaint_name").isNull(),

        "is_invalid_category":
            F.col("category").isNull()
            | ~F.col("category").isin(
                "Network",
                "Billing",
                "Service"
            ),

        "is_invalid_priority":
            F.col("default_priority").isNull()
            | ~F.col("default_priority").isin(
                "LOW",
                "MEDIUM",
                "HIGH"
            ),
    })

    critical = [
        "is_missing_key",
        "is_duplicate_key",
        "is_missing_code",
        "is_missing_name",
        "is_invalid_category",
        "is_invalid_priority",
    ]

    return df, critical


# ============================================================
# SAVE TABLE + QUALITY REPORT
# ============================================================

def save_table(df, name, critical_flags):

    quarantine_condition = F.lit(False)

    for flag in critical_flags:
        quarantine_condition = (
            quarantine_condition | F.col(flag)
        )

    df = df.withColumn(
        "is_quarantined",
        F.coalesce(
            quarantine_condition,
            F.lit(False)
        )
    )

    # FIX: Only count actual boolean quality flags.
    # Excludes is_active_raw (STRING)
    # and is_active (business attribute).
    flag_columns = [
        field.name
        for field in df.schema.fields
        if field.name.startswith("is_")
        and isinstance(
            field.dataType,
            BooleanType
        )
        and field.name not in (
            "is_quarantined",
            "is_active",
        )
    ]

    metrics = df.agg(
        F.count("*").alias("raw_rows"),

        F.sum(
            F.when(
                F.col("is_quarantined"),
                1
            ).otherwise(0)
        ).alias("quarantined_rows"),

        *[
            F.sum(
                F.when(
                    F.col(flag),
                    1
                ).otherwise(0)
            ).alias(flag)
            for flag in flag_columns
        ]
    ).first().asDict()

    raw_rows = int(
        metrics.pop("raw_rows")
    )

    rejected = int(
        metrics.pop("quarantined_rows") or 0
    )

    # Preserve audit values only in Quarantine.
    audit_columns = [
        col for col in df.columns
        if col.endswith("_raw")
    ]

    silver_df = (
        df
        .filter(~F.col("is_quarantined"))
        .drop(
            "is_quarantined",
            *audit_columns
        )
    )

    quarantine_df = df.filter(
        F.col("is_quarantined")
    )

    write_parquet(
        silver_df,
        SILVER / name
    )

    write_parquet(
        quarantine_df,
        QUARANTINE / name
    )

    result = {
        "raw_rows": raw_rows,
        "silver_rows": raw_rows - rejected,
        "quarantined_rows": rejected,
        "quality_flags": {
            key: int(value or 0)
            for key, value in metrics.items()
        },
    }

    print(
        f"[{name.upper()}] "
        f"Raw={raw_rows:,} | "
        f"Silver={raw_rows - rejected:,} | "
        f"Quarantine={rejected:,}",
        flush=True
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (
        SparkSession.builder
        .appName(
            "Telecom-Reference-Tables-Silver"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        processors = {
            "plans": clean_plans,
            "services": clean_services,
            "incident_types": clean_incident_types,
            "complaint_types": clean_complaint_types,
        }

        report = {
            "pipeline": "reference_tables_silver",
            "processed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "tables": {},
        }

        for table_name, processor in processors.items():

            print(
                f"\nProcessing {table_name}...",
                flush=True
            )

            raw_df = read_csv(
                spark,
                table_name
            )

            cleaned_df, critical_flags = (
                processor(raw_df)
            )

            report["tables"][table_name] = (
                save_table(
                    cleaned_df,
                    table_name,
                    critical_flags
                )
            )

        REPORT.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with REPORT.open(
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                report,
                file,
                indent=2
            )

        print(
            "\n=== REFERENCE TABLES REPORT ==="
        )

        print(
            json.dumps(report, indent=2)
        )

        print(
            "\nReference Tables Silver Pipeline completed.",
            flush=True
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

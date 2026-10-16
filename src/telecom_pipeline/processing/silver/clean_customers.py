from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

BASE = DATA_ROOT

INPUT_PATH = RAW / "customers.csv"

# Existing local Silver output
SILVER_PATH = SILVER / "customers"

# Existing local Quarantine output
QUARANTINE_PATH = QUARANTINE / "customers"

# Existing quality report
REPORT_PATH = REPORTS / "customers_quality.json"

# NEW:
# SeaweedFS Secondary / Disaster Recovery Silver destination
SEAWEED_SILVER_PATH = "s3a://telecom-silver/customers/"


# All raw fields are initially strings.
# Casting happens explicitly after raw ingestion.
RAW_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("area_id", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("age", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("subscription_status", StringType(), True),
    StructField("activation_date", StringType(), True),
    StructField("churn_date", StringType(), True),
    StructField("last_updated_at", StringType(), True),
])


# ==========================================================
# 2. COMMON CLEANING FUNCTIONS
# ==========================================================

def normalize_whitespace(column):
    """
    Trim surrounding spaces and collapse repeated whitespace.
    Empty strings become NULL.
    """

    cleaned = F.regexp_replace(
        F.trim(F.col(column)),
        r"\s+",
        " "
    )

    return F.when(
        cleaned == "",
        F.lit(None)
    ).otherwise(cleaned)


def normalize_category(column):
    """
    Example:
        ' business ' -> 'Business'
        'VIP'        -> 'Vip' (corrected separately)
    """

    return F.initcap(
        F.lower(
            normalize_whitespace(column)
        )
    )


def expected_age_group(age_column):
    """
    Authoritative grouping from the data generator.
    Only valid numeric ages are eligible.
    """

    return (
        F.when(
            age_column.between(18, 25),
            "18-25"
        )
        .when(
            age_column.between(26, 35),
            "26-35"
        )
        .when(
            age_column.between(36, 45),
            "36-45"
        )
        .when(
            age_column.between(46, 55),
            "46-55"
        )
        .when(
            age_column.between(56, 65),
            "56-65"
        )
        .when(
            age_column.between(66, 150),
            "66+"
        )
    )


# ==========================================================
# 3. READ RAW DATA
# ==========================================================

def read_raw(spark):

    print("\n[1/7] Reading customers.csv", flush=True)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}"
        )

    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("enforceSchema", "false")
        .csv(str(INPUT_PATH))
    )

    print(
        f"Raw rows: {df.count():,}",
        flush=True
    )

    return df


# ==========================================================
# 4. CLEAN TEXT
# ==========================================================

def clean_text(df):

    print(
        "[2/7] Normalizing text",
        flush=True
    )

    for column in [
        "customer_id",
        "customer_name",
        "area_id",
        "plan_id",
        "service_id",
        "age",
        "age_group",
        "customer_segment",
        "subscription_status",
        "activation_date",
        "churn_date",
        "last_updated_at",
    ]:

        df = df.withColumn(
            column,
            normalize_whitespace(column)
        )

    # Retain original age group for audit.
    df = df.withColumn(
        "age_group_original",
        F.col("age_group")
    )

    df = df.withColumn(
        "customer_name",
        F.initcap(
            F.lower(
                F.col("customer_name")
            )
        )
    )

    df = df.withColumn(
        "customer_segment",
        normalize_category(
            "customer_segment"
        )
    )

    # Preserve the canonical VIP spelling.
    df = df.withColumn(
        "customer_segment",
        F.when(
            F.lower(
                F.col("customer_segment")
            ) == "vip",
            "VIP"
        ).otherwise(
            F.col("customer_segment")
        )
    )

    df = df.withColumn(
        "subscription_status",
        normalize_category(
            "subscription_status"
        )
    )

    return df


# ==========================================================
# 5. DATA TYPES
# ==========================================================

def cast_columns(df):

    print(
        "[3/7] Applying data types",
        flush=True
    )

    # Preserve original string for validation.
    df = df.withColumn(
        "age_raw",
        F.col("age")
    )

    # Avoid attempting integer casts on malformed text.
    df = df.withColumn(
        "age",
        F.when(
            F.col("age_raw").rlike(r"^\d+$"),
            F.col("age_raw").cast("int")
        ).otherwise(
            F.lit(None).cast("int")
        )
    )

    df = (
        df

        .withColumn(
            "activation_date_raw",
            F.col("activation_date")
        )

        .withColumn(
            "churn_date_raw",
            F.col("churn_date")
        )

        .withColumn(
            "last_updated_at_raw",
            F.col("last_updated_at")
        )
    )

    df = (
        df

        .withColumn(
            "activation_date",
            F.to_date(
                "activation_date_raw",
                "yyyy-MM-dd"
            )
        )

        .withColumn(
            "churn_date",
            F.to_date(
                "churn_date_raw",
                "yyyy-MM-dd"
            )
        )

        .withColumn(
            "last_updated_at",
            F.to_timestamp(
                "last_updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    return df


# ==========================================================
# 6. DATA QUALITY FLAGS
# ==========================================================

def validate_and_correct(df):

    print(
        "[4/7] Validating business rules",
        flush=True
    )

    df = df.withColumn(
        "is_invalid_age",

        F.col("age_raw").isNotNull()

        & (
            F.col("age").isNull()

            | ~F.col("age").between(
                18,
                150
            )
        )
    )

    df = df.withColumn(
        "expected_age_group",
        expected_age_group(
            F.col("age")
        )
    )

    df = df.withColumn(
        "is_age_group_mismatch",

        F.col(
            "expected_age_group"
        ).isNotNull()

        & F.col(
            "age_group_original"
        ).isNotNull()

        & (
            F.col("age_group_original")
            !=
            F.col("expected_age_group")
        )
    )

    df = df.withColumn(
        "is_missing_age_group",
        F.col(
            "age_group_original"
        ).isNull()
    )

    # Derive age group only when age is valid.
    # Otherwise preserve original value.
    df = df.withColumn(
        "age_group",

        F.coalesce(
            F.col("expected_age_group"),
            F.col("age_group_original")
        )
    )

    df = df.withColumn(
        "is_missing_customer_segment",

        F.col(
            "customer_segment"
        ).isNull()
    )

    df = df.withColumn(
        "is_invalid_customer_segment",

        F.col(
            "customer_segment"
        ).isNotNull()

        & ~F.col(
            "customer_segment"
        ).isin(
            "Consumer",
            "Business",
            "VIP"
        )
    )

    df = df.withColumn(
        "is_invalid_subscription_status",

        F.col(
            "subscription_status"
        ).isNull()

        | ~F.col(
            "subscription_status"
        ).isin(
            "Active",
            "Suspended",
            "Cancelled"
        )
    )

    df = df.withColumn(
        "is_invalid_activation_date",

        F.col(
            "activation_date"
        ).isNull()
    )

    df = df.withColumn(
        "is_invalid_churn_date",

        F.col(
            "churn_date_raw"
        ).isNotNull()

        & F.col(
            "churn_date"
        ).isNull()
    )

    df = df.withColumn(
        "is_churn_before_activation",

        F.coalesce(
            F.col("churn_date")
            <
            F.col("activation_date"),

            F.lit(False)
        )
    )

    df = df.withColumn(
        "is_cancelled_without_churn_date",

        (
            F.col("subscription_status")
            ==
            "Cancelled"
        )

        & F.col(
            "churn_date"
        ).isNull()
    )

    df = df.withColumn(
        "is_invalid_last_updated_at",

        F.col(
            "last_updated_at"
        ).isNull()
    )

    df = df.withColumn(
        "is_missing_customer_id",

        F.col(
            "customer_id"
        ).isNull()
    )

    # Hard failures:
    # records unsafe for downstream modeling.
    df = df.withColumn(
        "is_quarantined",

        F.col("is_missing_customer_id")

        | F.col(
            "is_invalid_subscription_status"
        )

        | F.col(
            "is_invalid_activation_date"
        )

        | F.col(
            "is_invalid_churn_date"
        )

        | F.col(
            "is_churn_before_activation"
        )

        | F.col(
            "is_cancelled_without_churn_date"
        )

        | F.col(
            "is_invalid_last_updated_at"
        )
    )

    return df


# ==========================================================
# 7. SILVER OUTPUT
# ==========================================================

def write_outputs(df):

    print(
        "[5/7] Preparing Silver and Quarantine",
        flush=True
    )

    # Duplicate business keys must never
    # be silently removed.
    duplicates = (
        df
        .filter(
            F.col("customer_id").isNotNull()
        )
        .groupBy("customer_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicates:

        raise ValueError(
            f"Found {duplicates} duplicate customer_id groups. "
            "Investigate duplicates before writing Silver."
        )

    silver = (
        df
        .filter(
            ~F.col("is_quarantined")
        )
        .drop(
            "age_raw",
            "activation_date_raw",
            "churn_date_raw",
            "last_updated_at_raw",
            "expected_age_group",
            "is_quarantined"
        )
    )

    quarantine = (
        df.filter(
            F.col("is_quarantined")
        )
    )

    print(
        "[6/7] Writing Parquet outputs",
        flush=True
    )

    # ------------------------------------------------------
    # LOCAL SILVER
    # ------------------------------------------------------

    print(
        f"Writing local Silver -> {SILVER_PATH}",
        flush=True
    )

    (
        silver.write
        .mode("overwrite")
        .parquet(
            str(SILVER_PATH)
        )
    )

    print(
        "Local Silver write completed.",
        flush=True
    )


    # ------------------------------------------------------
    # SEAWEEDFS SILVER
    # SECONDARY / DISASTER RECOVERY STORAGE
    # ------------------------------------------------------

    print(
        f"Writing SeaweedFS Silver backup -> "
        f"{SEAWEED_SILVER_PATH}",
        flush=True
    )

    (
        silver.write
        .mode("overwrite")
        .parquet(
            SEAWEED_SILVER_PATH
        )
    )

    print(
        "SeaweedFS Silver backup completed.",
        flush=True
    )


    # ------------------------------------------------------
    # QUARANTINE
    # ------------------------------------------------------

    print(
        f"Writing Quarantine -> {QUARANTINE_PATH}",
        flush=True
    )

    (
        quarantine.write
        .mode("overwrite")
        .parquet(
            str(QUARANTINE_PATH)
        )
    )

    print(
        "Quarantine write completed.",
        flush=True
    )

    return silver, quarantine


# ==========================================================
# 8. DATA QUALITY REPORT
# ==========================================================

def create_report(
    df,
    silver,
    quarantine
):

    print(
        "[7/7] Generating quality report",
        flush=True
    )

    flags = [
        "is_invalid_age",
        "is_age_group_mismatch",
        "is_missing_age_group",
        "is_missing_customer_segment",
        "is_invalid_customer_segment",
        "is_invalid_subscription_status",
        "is_invalid_activation_date",
        "is_invalid_churn_date",
        "is_churn_before_activation",
        "is_cancelled_without_churn_date",
        "is_invalid_last_updated_at",
        "is_missing_customer_id",
    ]

    metrics = (
        df
        .agg(

            F.count("*").alias(
                "raw_rows"
            ),

            *[
                F.sum(
                    F.when(
                        F.col(flag),
                        1
                    ).otherwise(0)
                ).alias(flag)

                for flag in flags
            ],

            F.sum(
                F.when(
                    F.col("is_quarantined"),
                    1
                ).otherwise(0)
            ).alias(
                "quarantined_rows"
            )

        )
        .first()
        .asDict()
    )

    raw_rows = metrics.pop(
        "raw_rows"
    )

    quarantined_rows = metrics.pop(
        "quarantined_rows"
    )

    silver_rows = (
        raw_rows
        -
        quarantined_rows
    )

    report = {

        "dataset": "customers",

        "processed_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "raw_rows": raw_rows,

        "silver_rows": silver_rows,

        "quarantined_rows": quarantined_rows,

        "quality_flags": {
            k: int(v or 0)
            for k, v
            in metrics.items()
        },

        "outputs": {

            "silver_local":
                str(SILVER_PATH),

            "silver_seaweedfs":
                SEAWEED_SILVER_PATH,

            "quarantine":
                str(QUARANTINE_PATH),
        },
    }

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with REPORT_PATH.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=2
        )

    print(
        "\n========== QUALITY REPORT =========="
    )

    print(
        json.dumps(
            report,
            indent=2
        )
    )

    print(
        "====================================",
        flush=True
    )


# ==========================================================
# 9. MAIN PIPELINE
# ==========================================================

def main():

    spark = (

        SparkSession.builder

        .appName(
            "Telecom-Customers-Silver"
        )

        # --------------------------------------------------
        # SeaweedFS S3A Configuration
        # --------------------------------------------------

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://seaweed-s3:8333"
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
       .config(
            "spark.hadoop.fs.s3a.access.key",
            "SNOWFLAKE_TELECOM"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "TelecomSeaweed2026SecureKey"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )


        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        raw = read_raw(
            spark
        )

        cleaned = clean_text(
            raw
        )

        typed = cast_columns(
            cleaned
        )

        validated = validate_and_correct(
            typed
        )

        silver, quarantine = write_outputs(
            validated
        )

        create_report(
            validated,
            silver,
            quarantine
        )

        print(
            "\nCustomers Silver Pipeline completed.",
            flush=True
        )

        print(
            f"Local Silver: {SILVER_PATH}",
            flush=True
        )

        print(
            f"SeaweedFS Silver: {SEAWEED_SILVER_PATH}",
            flush=True
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
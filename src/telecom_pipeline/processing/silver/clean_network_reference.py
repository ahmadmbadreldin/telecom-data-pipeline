"""
Telecom Spark Silver: Areas + Towers

Inputs:
    data/raw/batch/areas.csv
    data/raw/batch/towers.csv

Outputs:
    data/processed/silver/areas/
    data/processed/silver/towers/
    data/processed/quarantine/areas/
    data/processed/quarantine/towers/
    data/reports/silver/network_reference_quality.json

SeaweedFS:
    s3a://telecom-silver/areas/
    s3a://telecom-silver/towers/

No dimensional modeling or business KPI aggregation.
"""

from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

BASE = DATA_ROOT

REPORT = REPORTS / "network_reference_quality.json"

SEAWEED_SILVER_BASE = "s3a://telecom-silver"


# ============================================================
# SHARED HELPERS
# ============================================================

def read_csv(spark, name, expected_columns):

    path = RAW / f"{name}.csv"

    if not path.exists():
        raise FileNotFoundError(path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(path))
    )

    if df.columns != expected_columns:

        raise ValueError(
            f"{name}: unexpected CSV columns.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {df.columns}"
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


def title_case(name):

    return F.initcap(
        F.lower(
            clean_string(name)
        )
    )


def uppercase(name):

    return F.upper(
        clean_string(name)
    )


def to_number(name, target_type):

    # Invalid strings become NULL rather than
    # unexpectedly changing a metric.
    return F.expr(
        f"try_cast(`{name}` as {target_type})"
    )


def to_date_safe(name):

    return F.expr(
        f"try_cast(`{name}` as date)"
    )


def add_duplicate_flag(df, key):

    window = Window.partitionBy(key)

    return df.withColumn(
        "is_duplicate_key",
        F.count("*").over(window) > 1
    )


def write_parquet(df, path):

    (
        df.write
        .mode("overwrite")
        .parquet(str(path))
    )


# ============================================================
# AREAS
# ============================================================

AREA_COLUMNS = [
    "area_id",
    "area_name",
    "city",
    "governorate",
    "region",
    "country",
    "latitude",
    "longitude",
    "population_density",
]


def process_areas(spark):

    print(
        "\n[AREAS] Reading raw CSV",
        flush=True
    )

    df = read_csv(
        spark,
        "areas",
        AREA_COLUMNS
    )

    # Preserve identifiers and normalize text.
    for name in AREA_COLUMNS:

        df = df.withColumn(
            name,
            clean_string(name)
        )

    for name in [
        "area_name",
        "city",
        "governorate",
        "region",
        "country",
    ]:

        df = df.withColumn(
            name,
            title_case(name)
        )

    # Explicit numeric conversion.
    for name in [
        "latitude",
        "longitude",
        "population_density",
    ]:

        df = df.withColumn(
            name,
            to_number(
                name,
                "double"
            )
        )

    # ========================================================
    # DATA QUALITY RULES
    # ========================================================

    df = add_duplicate_flag(
        df,
        "area_id"
    )

    df = df.withColumn(
        "is_missing_area_id",
        F.col(
            "area_id"
        ).isNull()
    )

    df = df.withColumn(
        "is_invalid_coordinates",

        F.col(
            "latitude"
        ).isNull()

        | F.col(
            "longitude"
        ).isNull()

        | ~F.col(
            "latitude"
        ).between(
            -90,
            90
        )

        | ~F.col(
            "longitude"
        ).between(
            -180,
            180
        )
    )

    df = df.withColumn(
        "is_missing_governorate",

        F.col(
            "governorate"
        ).isNull()
    )

    df = df.withColumn(
        "is_missing_region",

        F.col(
            "region"
        ).isNull()
    )

    df = df.withColumn(
        "is_invalid_population_density",

        F.coalesce(
            F.col(
                "population_density"
            ) < 0,

            F.lit(False)
        )
    )

    # Only critical failures cause quarantine.
    df = df.withColumn(
        "is_quarantined",

        F.col(
            "is_missing_area_id"
        )

        | F.col(
            "is_duplicate_key"
        )

        | F.col(
            "is_invalid_coordinates"
        )

        | F.col(
            "is_invalid_population_density"
        )
    )

    return df


# ============================================================
# TOWERS
# ============================================================

TOWER_COLUMNS = [
    "tower_id",
    "tower_name",
    "area_id",
    "latitude",
    "longitude",
    "tower_type",
    "technology",
    "vendor",
    "capacity",
    "installation_date",
    "status",
]


def process_towers(spark, valid_areas):

    print(
        "\n[TOWERS] Reading raw CSV",
        flush=True
    )

    df = read_csv(
        spark,
        "towers",
        TOWER_COLUMNS
    )

    for name in TOWER_COLUMNS:

        df = df.withColumn(
            name,
            clean_string(name)
        )

    for name in [
        "tower_name",
        "tower_type",
        "vendor",
    ]:

        df = df.withColumn(
            name,
            title_case(name)
        )

    # Canonical vendor spelling.
    df = df.withColumn(
        "vendor",

        F.when(
            F.upper(
                F.col("vendor")
            ) == "ZTE",

            F.lit("ZTE")
        ).otherwise(
            F.col("vendor")
        )
    )

    df = df.withColumn(
        "technology",
        uppercase("technology")
    )

    df = df.withColumn(
        "status",
        uppercase("status")
    )

    # Explicit types.
    for name in [
        "latitude",
        "longitude",
    ]:

        df = df.withColumn(
            name,
            to_number(
                name,
                "double"
            )
        )

    df = df.withColumn(
        "capacity",
        to_number(
            "capacity",
            "int"
        )
    )

    df = df.withColumn(
        "installation_date",
        to_date_safe(
            "installation_date"
        )
    )

    # ========================================================
    # PRIMARY KEY QUALITY
    # ========================================================

    df = add_duplicate_flag(
        df,
        "tower_id"
    )

    df = df.withColumn(
        "is_missing_tower_id",

        F.col(
            "tower_id"
        ).isNull()
    )

    # ========================================================
    # AREA REFERENCE VALIDATION
    # ========================================================

    area_keys = (
        valid_areas

        .select(
            F.col(
                "area_id"
            ).alias(
                "_valid_area_id"
            )
        )

        .distinct()
    )

    df = df.join(
        F.broadcast(
            area_keys
        ),

        df["area_id"]
        == area_keys[
            "_valid_area_id"
        ],

        "left"
    )

    df = df.withColumn(
        "is_invalid_area_reference",

        F.col(
            "_valid_area_id"
        ).isNull()

    ).drop(
        "_valid_area_id"
    )

    # ========================================================
    # COORDINATES
    # ========================================================

    df = df.withColumn(
        "is_invalid_coordinates",

        F.col(
            "latitude"
        ).isNull()

        | F.col(
            "longitude"
        ).isNull()

        | ~F.col(
            "latitude"
        ).between(
            -90,
            90
        )

        | ~F.col(
            "longitude"
        ).between(
            -180,
            180
        )
    )

    # Capacity must be positive.
    df = df.withColumn(
        "is_invalid_capacity",

        F.col(
            "capacity"
        ).isNull()

        | (
            F.col(
                "capacity"
            ) <= 0
        )
    )

    # Exploratory outlier flag, not a hard error.
    df = df.withColumn(
        "is_suspicious_capacity",

        F.coalesce(
            F.col(
                "capacity"
            ) > 50000,

            F.lit(False)
        )
    )

    valid_statuses = [
        "ACTIVE",
        "INACTIVE",
        "MAINTENANCE",
        "DECOMMISSIONED",
    ]

    df = df.withColumn(
        "is_invalid_status",

        F.col(
            "status"
        ).isNull()

        | ~F.col(
            "status"
        ).isin(
            valid_statuses
        )
    )

    df = df.withColumn(
        "is_invalid_installation_date",

        F.col(
            "installation_date"
        ).isNull()

        | (
            F.col(
                "installation_date"
            )
            > F.lit(
                "2026-09-30"
            ).cast("date")
        )
    )

    # Hard failures only.
    df = df.withColumn(
        "is_quarantined",

        F.col(
            "is_missing_tower_id"
        )

        | F.col(
            "is_duplicate_key"
        )

        | F.col(
            "is_invalid_area_reference"
        )

        | F.col(
            "is_invalid_coordinates"
        )

        | F.col(
            "is_invalid_capacity"
        )

        | F.col(
            "is_invalid_status"
        )

        | F.col(
            "is_invalid_installation_date"
        )
    )

    return df


# ============================================================
# OUTPUTS + QUALITY REPORT
# ============================================================

def save_dataset(df, name, flags):

    silver_df = (
        df
        .filter(
            ~F.col(
                "is_quarantined"
            )
        )
        .drop(
            "is_quarantined"
        )
    )

    quarantine_df = (
        df.filter(
            F.col(
                "is_quarantined"
            )
        )
    )

    # Collect aggregate counts, not raw rows.
    metrics = df.agg(

        F.count("*").alias(
            "raw_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_quarantined"
                ),
                1
            ).otherwise(0)
        ).alias(
            "quarantined_rows"
        ),

        *[
            F.sum(
                F.when(
                    F.col(flag),
                    1
                ).otherwise(0)
            ).alias(flag)

            for flag in flags
        ]

    ).first().asDict()

    raw_count = int(
        metrics.pop(
            "raw_rows"
        )
    )

    rejected = int(
        metrics.pop(
            "quarantined_rows"
        ) or 0
    )

    accepted = (
        raw_count - rejected
    )

    print(
        f"[{name.upper()}] "
        f"Raw={raw_count:,}, "
        f"Silver={accepted:,}, "
        f"Quarantine={rejected:,}",
        flush=True
    )

    # ========================================================
    # 1. LOCAL SILVER
    # ========================================================

    local_silver_path = str(
        SILVER / name
    )

    print(
        f"[{name.upper()}] "
        f"Writing Local Silver -> "
        f"{local_silver_path}",
        flush=True
    )

    write_parquet(
        silver_df,
        SILVER / name
    )

    print(
        f"[{name.upper()}] "
        "Local Silver completed.",
        flush=True
    )

    # ========================================================
    # 2. SEAWEEDFS SILVER
    # ========================================================

    seaweed_silver_path = (
        f"{SEAWEED_SILVER_BASE}/{name}/"
    )

    print(
        f"[{name.upper()}] "
        f"Writing SeaweedFS Silver -> "
        f"{seaweed_silver_path}",
        flush=True
    )

    (
        silver_df.write
        .mode("overwrite")
        .parquet(
            seaweed_silver_path
        )
    )

    print(
        f"[{name.upper()}] "
        "SeaweedFS Silver backup completed.",
        flush=True
    )

    # ========================================================
    # 3. LOCAL QUARANTINE
    # ========================================================

    quarantine_path = str(
        QUARANTINE / name
    )

    print(
        f"[{name.upper()}] "
        f"Writing Quarantine -> "
        f"{quarantine_path}",
        flush=True
    )

    write_parquet(
        quarantine_df,
        QUARANTINE / name
    )

    return {

        "raw_rows":
            raw_count,

        "silver_rows":
            accepted,

        "quarantined_rows":
            rejected,

        "quality_flags": {
            key: int(value or 0)
            for key, value
            in metrics.items()
        },

        "outputs": {

            "local_silver":
                local_silver_path,

            "seaweedfs_silver":
                seaweed_silver_path,

            "quarantine":
                quarantine_path,
        },
    }


# ============================================================
# MAIN
# ============================================================

def main():

    spark = (

        SparkSession.builder

        .appName(
            "Telecom-Network-Reference-Silver"
        )

        # ====================================================
        # SEAWEEDFS S3A CONFIGURATION
        # ====================================================

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
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "SPARK_TELECOM"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "SparkSeaweed2026SecureKey"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        # ====================================================
        # AREAS
        # ====================================================

        areas = process_areas(
            spark
        )

        area_flags = [
            "is_missing_area_id",
            "is_duplicate_key",
            "is_invalid_coordinates",
            "is_missing_governorate",
            "is_missing_region",
            "is_invalid_population_density",
        ]

        area_report = save_dataset(
            areas,
            "areas",
            area_flags
        )

        # Only accepted areas are valid
        # references for towers.
        valid_areas = areas.filter(
            ~F.col(
                "is_quarantined"
            )
        )

        # ====================================================
        # TOWERS
        # ====================================================

        towers = process_towers(
            spark,
            valid_areas
        )

        tower_flags = [
            "is_missing_tower_id",
            "is_duplicate_key",
            "is_invalid_area_reference",
            "is_invalid_coordinates",
            "is_invalid_capacity",
            "is_suspicious_capacity",
            "is_invalid_status",
            "is_invalid_installation_date",
        ]

        tower_report = save_dataset(
            towers,
            "towers",
            tower_flags
        )

        # ====================================================
        # REPORT
        # ====================================================

        report = {

            "pipeline":
                "network_reference_silver",

            "processed_at_utc":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "areas":
                area_report,

            "towers":
                tower_report,

            "outputs": {

                "areas":
                    str(
                        SILVER / "areas"
                    ),

                "towers":
                    str(
                        SILVER / "towers"
                    ),

                "seaweedfs_areas":
                    f"{SEAWEED_SILVER_BASE}/areas/",

                "seaweedfs_towers":
                    f"{SEAWEED_SILVER_BASE}/towers/",

                "quarantine_areas":
                    str(
                        QUARANTINE / "areas"
                    ),

                "quarantine_towers":
                    str(
                        QUARANTINE / "towers"
                    ),
            },
        }

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
            "\n=== NETWORK REFERENCE REPORT ==="
        )

        print(
            json.dumps(
                report,
                indent=2
            )
        )

        print(
            "\nPipeline completed.",
            flush=True
        )

        print(
            "\nSeaweedFS outputs:",
            flush=True
        )

        print(
            f"  {SEAWEED_SILVER_BASE}/areas/",
            flush=True
        )

        print(
            f"  {SEAWEED_SILVER_BASE}/towers/",
            flush=True
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
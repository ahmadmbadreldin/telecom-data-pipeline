
import csv
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

INPUT_DIR = Path("/opt/telecom/data/raw/batch")
REPORT_DIR = Path("/opt/telecom/data/reports/profiling")

# Business keys, not arbitrary row numbers.
TABLE_KEYS = {
    "customers": ["customer_id"],
    "areas": ["area_id"],
    "towers": ["tower_id"],
    "plans": ["plan_id"],
    "services": ["service_id"],
    "incident_types": ["incident_type_id"],
    "complaint_types": ["complaint_type_id"],
    "complaints": ["complaint_id"],
    "customer_experience": ["cx_id"],
    "network_incidents": ["incident_id"],
    "bridge_incident_customer": ["incident_id", "customer_id"],
    "customer_usage": ["customer_id", "service_id", "tower_id",
                       "usage_date", "usage_hour"],
}

def profile_table(spark, file_path):
    table_name = file_path.stem

    # Keep raw values as strings during initial profiling.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(str(file_path))
    )

    columns = df.columns
    row_count = df.count()

    if not columns:
        raise ValueError(f"No columns found in {file_path}")

    null_expressions = [
        F.sum(
            F.when(
                F.col(f"`{col}`").isNull()
                | (F.trim(F.col(f"`{col}`")) == ""),
                1
            ).otherwise(0)
        ).alias(f"null_{i}")
        for i, col in enumerate(columns)
    ]

    null_row = df.agg(*null_expressions).first()

    column_profiles = []
    for i, col in enumerate(columns):
        null_count = int(null_row[f"null_{i}"] or 0)

        column_profiles.append({
            "column": col,
            "null_count": null_count,
            "null_pct": round(
                100 * null_count / row_count, 2
            ) if row_count else 0,
        })

    keys = TABLE_KEYS.get(table_name, [])
    duplicate_keys = None
    missing_key_rows = None

    if keys and all(k in columns for k in keys):
        valid_key_condition = F.lit(True)

        for key in keys:
            valid_key_condition = (
                valid_key_condition
                & F.col(key).isNotNull()
                & (F.trim(F.col(key)) != "")
            )

        missing_key_rows = df.filter(
            ~valid_key_condition
        ).count()

        valid_keys = df.filter(valid_key_condition)

        duplicate_keys = (
            valid_keys.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

    return {
        "table": table_name,
        "rows": row_count,
        "columns": len(columns),
        "primary_key": keys,
        "missing_key_rows": missing_key_rows,
        "duplicate_key_groups": duplicate_keys,
        "column_profiles": column_profiles,
    }


def main():
    spark = (
        SparkSession.builder
        .appName("Telecom-Batch-Data-Profiling")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        files = sorted(INPUT_DIR.glob("*.csv"))

        if not files:
            raise FileNotFoundError(
                f"No CSV files found in {INPUT_DIR}"
            )

        results = []

        for file_path in files:
            print(f"\nProfiling: {file_path.name}", flush=True)

            result = profile_table(spark, file_path)
            results.append(result)

            print(
                f"Rows: {result['rows']:,} | "
                f"Columns: {result['columns']} | "
                f"Duplicate key groups: "
                f"{result['duplicate_key_groups']}",
                flush=True
            )

        json_path = REPORT_DIR / "profiling_report.json"

        with json_path.open("w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        csv_path = REPORT_DIR / "column_quality.csv"

        with csv_path.open(
            "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow([
                "table", "column", "rows",
                "null_count", "null_pct"
            ])

            for table in results:
                for column in table["column_profiles"]:
                    writer.writerow([
                        table["table"],
                        column["column"],
                        table["rows"],
                        column["null_count"],
                        column["null_pct"],
                    ])

        print("\nProfiling completed successfully.")
        print(f"JSON: {json_path}")
        print(f"CSV:  {csv_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

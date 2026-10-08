
import csv
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = Path("/opt/telecom/data")
INPUT = BASE / "raw" / "batch"
OUTPUT = BASE / "reports" / "business_validation"

results = []


def read_table(spark, name):
    path = INPUT / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(path))
        .cache()
    )


def number(col):
    # Invalid numeric strings become NULL for validation.
    return F.expr(f"try_cast(`{col}` as double)")


def timestamp(col):
    return F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss")


def date_col(col):
    return F.to_date(F.col(col), "yyyy-MM-dd")


def add_rule(df, table, rule, condition,
             severity="WARNING", action="FLAG"):
    count = df.filter(
        F.coalesce(condition, F.lit(False))
    ).count()

    results.append({
        "table": table,
        "rule": rule,
        "failed_rows": count,
        "severity": severity,
        "recommended_action": action,
    })

    print(
        f"{table:26} | {rule:36} | {count:,}",
        flush=True
    )


def check_fk(child, parent, table, fk, pk):
    # Exclude missing values: validate those separately.
    keys = (
        parent.select(F.col(pk).alias("_parent_key"))
        .distinct()
    )

    missing = (
        child.filter(
            F.col(fk).isNotNull()
            & (F.trim(F.col(fk)) != "")
        )
        .join(
            keys,
            F.col(fk) == F.col("_parent_key"),
            "left_anti"
        )
        .count()
    )

    results.append({
        "table": table,
        "rule": f"fk_{fk}_not_found",
        "failed_rows": missing,
        "severity": "ERROR",
        "recommended_action": "QUARANTINE_OR_INVESTIGATE",
    })


def main():
    spark = (
        SparkSession.builder
        .appName("Telecom-Business-Validation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tables = {}

    try:
        names = [
            "customers", "areas", "towers", "plans",
            "services", "complaints",
            "customer_experience", "network_incidents",
            "bridge_incident_customer",
            "incident_types", "complaint_types",
        ]

        for name in names:
            tables[name] = read_table(spark, name)

        c = tables["customers"]

        add_rule(
            c, "customers", "invalid_age",
            F.col("age").isNotNull()
            & (
                number("age").isNull()
                | (number("age") < 18)
                | (number("age") > 100)
            ),
            "WARNING", "INVESTIGATE"
        )

        derived_group = (
            F.when(number("age") <= 25, "18-25")
            .when(number("age") <= 35, "26-35")
            .when(number("age") <= 45, "36-45")
            .when(number("age") <= 55, "46-55")
            .when(number("age") <= 65, "56-65")
            .otherwise("66+")
        )

        add_rule(
            c, "customers", "age_group_mismatch",
            number("age").isNotNull()
            & F.col("age_group").isNotNull()
            & (F.col("age_group") != derived_group),
            "WARNING", "DERIVE_FROM_AGE"
        )

        status = F.upper(F.trim(F.col(
            "subscription_status"
        )))

        add_rule(
            c, "customers", "cancelled_without_churn_date",
            (status == "CANCELLED")
            & F.col("churn_date").isNull(),
            "ERROR", "INVESTIGATE"
        )

        add_rule(
            c, "customers", "churn_before_activation",
            date_col("churn_date").isNotNull()
            & (
                date_col("churn_date")
                < date_col("activation_date")
            ),
            "ERROR", "INVESTIGATE"
        )

        for name in ["areas", "towers"]:
            df = tables[name]
            add_rule(
                df, name, "invalid_coordinates",
                number("latitude").isNull()
                | number("longitude").isNull()
                | (number("latitude") < -90)
                | (number("latitude") > 90)
                | (number("longitude") < -180)
                | (number("longitude") > 180),
                "ERROR", "INVESTIGATE"
            )

        add_rule(
            tables["towers"], "towers",
            "suspicious_capacity",
            number("capacity") > 50000,
            "WARNING", "FLAG_OUTLIER"
        )

        cmp = tables["complaints"]
        resolved = timestamp("resolved_timestamp")
        created = timestamp("created_timestamp")

        add_rule(
            cmp, "complaints", "resolution_before_creation",
            resolved.isNotNull() & (resolved < created),
            "ERROR", "INVESTIGATE"
        )

        cmp_status = F.upper(F.trim(F.col("status")))

        add_rule(
            cmp, "complaints", "closed_without_resolution",
            (cmp_status == "CLOSED") & resolved.isNull(),
            "ERROR", "INVESTIGATE"
        )

        add_rule(
            cmp, "complaints", "open_with_resolution",
            cmp_status.isin("OPEN", "PENDING")
            & resolved.isNotNull(),
            "WARNING", "INVESTIGATE"
        )

        cx = tables["customer_experience"]

        for col, low, high in [
            ("rating", 1, 5),
            ("satisfaction_score", 0, 100),
            ("nps_score", 0, 10),
            ("call_quality_score", 1, 5),
            ("internet_experience_score", 1, 5),
            ("network_quality_score", 1, 5),
        ]:
            value = number(col)
            add_rule(
                cx, "customer_experience",
                f"invalid_{col}",
                F.col(col).isNotNull()
                & (
                    value.isNull()
                    | (value < low)
                    | (value > high)
                ),
                "ERROR", "INVESTIGATE"
            )

        incidents = tables["network_incidents"]

        add_rule(
            incidents, "network_incidents",
            "invalid_incident_time",
            timestamp("end_timestamp")
            < timestamp("start_timestamp"),
            "ERROR", "INVESTIGATE"
        )

        add_rule(
            incidents, "network_incidents",
            "suspicious_latency",
            number("max_latency_ms") > 5000,
            "WARNING", "FLAG_OUTLIER"
        )

        bridge = tables["bridge_incident_customer"]

        add_rule(
            bridge, "bridge_incident_customer",
            "invalid_impact_time",
            timestamp("impact_end_timestamp").isNotNull()
            & (
                timestamp("impact_end_timestamp")
                < timestamp("impact_start_timestamp")
            ),
            "ERROR", "INVESTIGATE"
        )

        add_rule(
            bridge, "bridge_incident_customer",
            "invalid_impact_score",
            F.col("impact_score").isNotNull()
            & (
                number("impact_score").isNull()
                | (number("impact_score") < 0)
                | (number("impact_score") > 100)
            ),
            "ERROR", "INVESTIGATE"
        )

        # Referential integrity checks
        relationships = [
            ("customers", "areas", "area_id", "area_id"),
            ("customers", "plans", "plan_id", "plan_id"),
            ("customers", "services", "service_id", "service_id"),
            ("towers", "areas", "area_id", "area_id"),
            ("network_incidents", "towers",
             "tower_id", "tower_id"),
            ("network_incidents", "areas",
             "area_id", "area_id"),
            ("network_incidents", "incident_types",
             "incident_type_id", "incident_type_id"),
            ("complaints", "customers",
             "customer_id", "customer_id"),
            ("complaints", "complaint_types",
             "complaint_type_id", "complaint_type_id"),
            ("complaints", "network_incidents",
             "incident_id", "incident_id"),
            ("customer_experience", "customers",
             "customer_id", "customer_id"),
            ("bridge_incident_customer", "customers",
             "customer_id", "customer_id"),
            ("bridge_incident_customer", "network_incidents",
             "incident_id", "incident_id"),
        ]

        for child, parent, fk, pk in relationships:
            check_fk(
                tables[child], tables[parent],
                child, fk, pk
            )

        # Save validation results
        OUTPUT.mkdir(parents=True, exist_ok=True)

        with (OUTPUT / "business_validation.json").open(
            "w", encoding="utf-8"
        ) as f:
            json.dump(results, f, indent=2)

        with (OUTPUT / "business_validation.csv").open(
            "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "table", "rule", "failed_rows",
                    "severity", "recommended_action"
                ]
            )
            writer.writeheader()
            writer.writerows(results)

        print("\nValidation report saved successfully.")

    finally:
        for df in tables.values():
            df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()

from pipeline_paths import DATA_ROOT, RAW, SILVER, QUARANTINE, REPORTS

from pyspark.sql import SparkSession, functions as F

PATH = str(SILVER / "customers")


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-Customers-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(PATH)

        print("\n=== SILVER CUSTOMERS SCHEMA ===")
        df.printSchema()

        counts = df.agg(
            F.count("*").alias("total"),
            F.countDistinct("customer_id").alias("unique_ids"),
            F.sum(
                F.when(F.col("is_age_group_mismatch"), 1)
                .otherwise(0)
            ).alias("corrected_age_groups"),
            F.sum(
                F.when(
                    F.col("age").isNotNull()
                    & F.col("age_group").isNull(), 1
                ).otherwise(0)
            ).alias("missing_group_with_valid_age")
        ).first()

        print("\n=== VERIFICATION ===")
        print("Total rows:", counts["total"])
        print("Unique customer IDs:", counts["unique_ids"])
        print(
            "Age group mismatch flags:",
            counts["corrected_age_groups"]
        )
        print(
            "Valid age without age group:",
            counts["missing_group_with_valid_age"]
        )

        assert counts["total"] == 100000
        assert counts["unique_ids"] == counts["total"]
        assert counts["corrected_age_groups"] == 1522
        assert counts["missing_group_with_valid_age"] == 0

        print("\nALL CHECKS PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

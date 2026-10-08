
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    DateType,
)

BASE = "/opt/telecom/data/processed/silver"


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-Network-Reference-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # =====================================
        # 1. READ SILVER PARQUET
        # =====================================

        areas = spark.read.parquet(f"{BASE}/areas")
        towers = spark.read.parquet(f"{BASE}/towers")

        print("\n=== AREAS SCHEMA ===")
        areas.printSchema()

        print("\n=== TOWERS SCHEMA ===")
        towers.printSchema()

        # =====================================
        # 2. ROW COUNTS + PRIMARY KEYS
        # =====================================

        area_count = areas.count()
        tower_count = towers.count()

        unique_areas = areas.select(
            "area_id"
        ).distinct().count()

        unique_towers = towers.select(
            "tower_id"
        ).distinct().count()

        missing_area_ids = areas.filter(
            F.col("area_id").isNull()
            | (F.trim("area_id") == "")
        ).count()

        missing_tower_ids = towers.filter(
            F.col("tower_id").isNull()
            | (F.trim("tower_id") == "")
        ).count()

        assert area_count == 50, (
            f"Expected 50 areas, got {area_count}"
        )

        assert tower_count == 500, (
            f"Expected 500 towers, got {tower_count}"
        )

        assert unique_areas == area_count
        assert unique_towers == tower_count

        assert missing_area_ids == 0
        assert missing_tower_ids == 0

        print("\n[PASS] Row counts and primary keys")

        # =====================================
        # 3. DATA TYPES
        # =====================================

        assert isinstance(
            areas.schema["latitude"].dataType,
            DoubleType
        )

        assert isinstance(
            areas.schema["longitude"].dataType,
            DoubleType
        )

        assert isinstance(
            towers.schema["latitude"].dataType,
            DoubleType
        )

        assert isinstance(
            towers.schema["longitude"].dataType,
            DoubleType
        )

        assert isinstance(
            towers.schema["capacity"].dataType,
            IntegerType
        )

        assert isinstance(
            towers.schema["installation_date"].dataType,
            DateType
        )

        print("[PASS] Data types")

        # =====================================
        # 4. COORDINATES
        # =====================================

        invalid_area_coordinates = areas.filter(
            F.col("latitude").isNull()
            | F.col("longitude").isNull()
            | ~F.col("latitude").between(-90, 90)
            | ~F.col("longitude").between(-180, 180)
        ).count()

        invalid_tower_coordinates = towers.filter(
            F.col("latitude").isNull()
            | F.col("longitude").isNull()
            | ~F.col("latitude").between(-90, 90)
            | ~F.col("longitude").between(-180, 180)
        ).count()

        assert invalid_area_coordinates == 0
        assert invalid_tower_coordinates == 0

        print("[PASS] Geographic coordinates")

        # =====================================
        # 5. TOWER -> AREA RELATIONSHIP
        # =====================================

        invalid_area_references = (
            towers
            .join(
                areas.select("area_id").distinct(),
                on="area_id",
                how="left_anti"
            )
            .count()
        )

        assert invalid_area_references == 0

        print("[PASS] Tower -> Area relationship")

        # =====================================
        # 6. CAPACITY VALIDATION
        # =====================================

        invalid_capacity = towers.filter(
            F.col("capacity").isNull()
            | (F.col("capacity") <= 0)
        ).count()

        suspicious_capacity = towers.filter(
            F.col("is_suspicious_capacity") == True
        ).count()

        incorrect_capacity_flags = towers.filter(
            F.col("is_suspicious_capacity").isNull()
            | (
                F.col("is_suspicious_capacity")
                != (F.col("capacity") > 50000)
            )
        ).count()

        assert invalid_capacity == 0
        assert suspicious_capacity == 1
        assert incorrect_capacity_flags == 0

        print("[PASS] Tower capacity and outlier flags")

        # =====================================
        # 7. MISSING OPTIONAL VALUES
        # =====================================

        missing_governorate = areas.filter(
            F.col("governorate").isNull()
        ).count()

        missing_region = areas.filter(
            F.col("region").isNull()
        ).count()

        assert missing_governorate == 1
        assert missing_region == 2

        print("[PASS] Optional null handling")

        # =====================================
        # 8. FINAL RESULT
        # =====================================

        print("\n========== VERIFICATION ==========")
        print(f"Areas: {area_count}")
        print(f"Towers: {tower_count}")
        print(f"Missing governorate: {missing_governorate}")
        print(f"Missing region: {missing_region}")
        print(f"Suspicious capacity: {suspicious_capacity}")
        print(f"Invalid area references: {invalid_area_references}")
        print("==================================")

        print("\nALL NETWORK REFERENCE CHECKS PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

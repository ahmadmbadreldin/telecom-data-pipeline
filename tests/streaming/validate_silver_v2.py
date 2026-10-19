
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/opt/telecom/data/processed"

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Validate-100K-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.parquet(
    f"{BASE}/bronze"
)

silver = spark.read.parquet(
    f"{BASE}/silver/network_events_streaming_v2"
)

quarantine = spark.read.parquet(
    f"{BASE}/quarantine/network_events_streaming_v2"
)

bronze_count = bronze.count()
silver_count = silver.count()
quarantine_count = quarantine.count()

unique_ids = silver.select("event_id").distinct().count()
duplicate_count = silver_count - unique_ids

flagged = silver.filter(
    F.size("quality_flags") > 0
).count()

print("\n=== 100K STREAMING VALIDATION ===")
print(f"Bronze Events: {bronze_count}")
print(f"Silver Events: {silver_count}")
print(f"Quarantine Events: {quarantine_count}")
print(f"Unique Event IDs: {unique_ids}")
print(f"Duplicate Events: {duplicate_count}")
print(f"Quality Flagged Events: {flagged}")

print("\n=== RECONCILIATION ===")
print(
    "Bronze - (Silver + Quarantine):",
    bronze_count - (silver_count + quarantine_count)
)

spark.stop()

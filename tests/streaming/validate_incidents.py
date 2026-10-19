
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/opt/telecom/data/processed"

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Validate-Incident-Detection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

incidents = spark.read.parquet(
    f"{BASE}/incidents/network_incidents_streaming"
)

total = incidents.count()
towers = incidents.select("tower_id").distinct().count()

print("\n=== INCIDENT DETECTION RESULTS ===")
print(f"Total Incident Candidates: {total}")
print(f"Affected Towers: {towers}")

print("\n=== SEVERITY DISTRIBUTION ===")

incidents.groupBy("incident_severity") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

print("\n=== TOP 10 AFFECTED TOWERS ===")

incidents.groupBy("tower_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(10)

spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Telecom-Deduplication-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Simulate repeated network events.
events = [
    ("E001", "T001", 0, 100),
    ("E002", "T002", 0, 101),
    ("E001", "T001", 0, 102),
    ("E003", "T003", 1, 50),
    ("E002", "T002", 1, 51),
]

df = spark.createDataFrame(
    events,
    ["event_id", "tower_id", "partition", "offset"]
)

# Keep the first Kafka occurrence of each event.
window = Window.partitionBy("event_id").orderBy(
    F.col("partition").asc(),
    F.col("offset").asc()
)

deduplicated = (
    df.withColumn("row_number", F.row_number().over(window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

input_count = df.count()
output_count = deduplicated.count()
duplicate_count = input_count - output_count

print(f"Input Events: {input_count}")
print(f"Unique Events: {output_count}")
print(f"Duplicates Removed: {duplicate_count}")

deduplicated.orderBy("event_id").show()

assert input_count == 5
assert output_count == 3
assert duplicate_count == 2

spark.stop()

print("DEDUPLICATION TEST PASSED")


import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


# ============================================================
# 1. Arguments
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--phase",
    choices=["first", "second"],
    required=True
)

args = parser.parse_args()


# ============================================================
# 2. Test Paths
# ============================================================

# Use a new test directory.
# Keep the old test files unchanged.

BASE = Path("/opt/telecom/data/tests/streaming_dedup_v2")

INPUT = BASE / "input"
OUTPUT = BASE / "output"
CHECKPOINT = BASE / "checkpoint"

INPUT.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)
CHECKPOINT.mkdir(parents=True, exist_ok=True)


# ============================================================
# 3. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Telecom-Streaming-Dedup-Test")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print(
    f"\nStarting deduplication test: {args.phase}",
    flush=True
)


# ============================================================
# 4. Event Schema
# ============================================================

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


# ============================================================
# 5. Prepare Test Events
# ============================================================

if args.phase == "first":

    events = [
        {
            "event_id": "E001",
            "event_timestamp": "2026-10-11 10:00:00"
        },
        {
            "event_id": "E002",
            "event_timestamp": "2026-10-11 10:01:00"
        },
        {
            "event_id": "E001",
            "event_timestamp": "2026-10-11 10:00:00"
        },
    ]

    filename = INPUT / "batch_01.json"
    expected_count = 2

else:

    events = [
        {
            "event_id": "E002",
            "event_timestamp": "2026-10-11 10:01:00"
        },
        {
            "event_id": "E003",
            "event_timestamp": "2026-10-11 10:02:00"
        },
    ]

    filename = INPUT / "batch_02.json"
    expected_count = 3


# Prevent overwriting existing test input.
if filename.exists():
    raise RuntimeError(
        f"Test input already exists: {filename}"
    )


# Write test events as JSON Lines.
with filename.open("w", encoding="utf-8") as file:

    for event in events:
        file.write(json.dumps(event) + "\n")


print(
    f"Created test input: {filename.name}",
    flush=True
)


# ============================================================
# 6. Read Streaming Source
# ============================================================

source = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(str(INPUT))
)


# ============================================================
# 7. Streaming Deduplication
# ============================================================

processed = (
    source
    .withColumn(
        "event_time",
        F.to_timestamp(
            F.col("event_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withWatermark("event_time", "72 hours")
    .dropDuplicates([
        "event_id",
        "event_time"
    ])
)


# ============================================================
# 8. Start Streaming Query
# ============================================================

query = (
    processed.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        str(OUTPUT)
    )
    .option(
        "checkpointLocation",
        str(CHECKPOINT)
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

if query.exception() is not None:
    raise RuntimeError(
        f"Streaming failed: {query.exception()}"
    )


# ============================================================
# 9. Verify Results
# ============================================================

result = spark.read.parquet(str(OUTPUT))

total_count = result.count()

print("\n===================================")
print(f"PHASE: {args.phase}")
print(f"TOTAL UNIQUE EVENTS: {total_count}")
print(f"EXPECTED EVENTS: {expected_count}")
print("===================================")

result.groupBy("event_id") \
    .count() \
    .orderBy("event_id") \
    .show(truncate=False)


# Check total row count.
assert total_count == expected_count, (
    f"Expected {expected_count} events, "
    f"but found {total_count}"
)


# Check for duplicate event IDs.
duplicate_count = (
    result.groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_count == 0, (
    f"Found {duplicate_count} duplicated event IDs"
)


# ============================================================
# 10. Test Result
# ============================================================

print(
    f"\nDEDUPLICATION PHASE {args.phase.upper()} PASSED",
    flush=True
)

spark.stop()

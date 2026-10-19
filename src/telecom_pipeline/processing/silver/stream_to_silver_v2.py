
import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
)


# ============================================================
# 1. Configuration
# ============================================================

BASE = "/opt/telecom/data"
CONFIG = "/opt/telecom/config/data_quality.yaml"

# Read the existing Bronze data.
BRONZE_PATH = f"{BASE}/processed/bronze"

# Use new output paths.
SILVER_PATH = (
    f"{BASE}/processed/silver/network_events_streaming_v2"
)

QUARANTINE_PATH = (
    f"{BASE}/processed/quarantine/network_events_streaming_v2"
)

# Use separate checkpoints for both queries.
SILVER_CHECKPOINT = (
    f"{BASE}/checkpoints/silver_network_events_v2"
)

QUARANTINE_CHECKPOINT = (
    f"{BASE}/checkpoints/quarantine_network_events_v2"
)

with open(CONFIG, encoding="utf-8") as file:
    rules = yaml.safe_load(file)["network_events"]


# ============================================================
# 2. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Telecom-Silver-Streaming-V2")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. Bronze Schema
# ============================================================

bronze_schema = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
])


# ============================================================
# 4. Network Event Schema
# ============================================================

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("tower_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("signal_strength_dbm", DoubleType(), True),
    StructField("call_drop_rate", DoubleType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("download_speed_mbps", DoubleType(), True),
    StructField("upload_speed_mbps", DoubleType(), True),
    StructField("packet_loss_pct", DoubleType(), True),
    StructField("network_status", StringType(), True),
    StructField("incident_type_code", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


# ============================================================
# 5. Read Bronze Streaming Data
# ============================================================

bronze = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .option("maxFilesPerTrigger", 50)
    .load(BRONZE_PATH)
)


# ============================================================
# 6. Parse Network Events
# ============================================================

parsed = (
    bronze
    .withColumn(
        "event",
        F.from_json(
            F.col("raw_json"),
            event_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ),
    )
    .select("*", "event.*")
    .drop("event")
    .withColumn(
        "event_time",
        F.to_timestamp(
            F.col("event_timestamp"),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    .withColumn(
        "ingestion_time",
        F.to_timestamp(
            F.col("ingestion_timestamp"),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
)


# ============================================================
# 7. Hard Data Quality Validation
# ============================================================

errors = []

# Detect malformed JSON.
errors.append(
    F.when(
        F.col("raw_json").isNull()
        | F.col("_corrupt_record").isNotNull(),
        F.lit("MALFORMED_JSON"),
    )
)

# Check required fields.
for field in rules["required_fields"]:
    missing = (
        F.col(field).isNull()
        | (F.trim(F.col(field).cast("string")) == "")
    )

    errors.append(
        F.when(
            missing,
            F.lit(f"MISSING_{field}"),
        )
    )

# Validate timestamps.
errors.extend([
    F.when(
        F.col("event_time").isNull(),
        F.lit("INVALID_EVENT_TIMESTAMP"),
    ),
    F.when(
        F.col("ingestion_time").isNull(),
        F.lit("INVALID_INGESTION_TIMESTAMP"),
    ),
])

# Validate network status.
errors.append(
    F.when(
        F.col("network_status").isNull()
        | ~F.col("network_status").isin(
            rules["valid_statuses"]
        ),
        F.lit("INVALID_NETWORK_STATUS"),
    )
)

# Validate numeric metrics.
for metric, limits in rules["hard_limits"].items():

    value = F.col(metric)

    raw_value = F.get_json_object(
        F.col("raw_json"),
        f"$.{metric}",
    )

    parsing_error = (
        raw_value.isNotNull()
        & value.isNull()
    )

    errors.append(
        F.when(
            parsing_error,
            F.lit(f"INVALID_NUMERIC_{metric}"),
        )
    )

    invalid_metric = (
        F.isnan(value)
        | (value == F.lit(float("inf")))
        | (value == F.lit(float("-inf")))
    )

    if "min" in limits:
        invalid_metric = (
            invalid_metric
            | (value < F.lit(limits["min"]))
        )

    if "max" in limits:
        invalid_metric = (
            invalid_metric
            | (value > F.lit(limits["max"]))
        )

    errors.append(
        F.when(
            invalid_metric,
            F.lit(f"INVALID_{metric}"),
        )
    )


# ============================================================
# 8. Soft Data Quality Flags
# ============================================================

flags = []

# Flag missing metrics.
for metric in rules["hard_limits"]:
    flags.append(
        F.when(
            F.col(metric).isNull(),
            F.lit(f"MISSING_{metric}"),
        )
    )

# Flag abnormal metrics.
for metric, limits in rules["normal_thresholds"].items():

    condition = F.lit(False)

    if "min" in limits:
        condition = (
            condition
            | (F.col(metric) < F.lit(limits["min"]))
        )

    if "max" in limits:
        condition = (
            condition
            | (F.col(metric) > F.lit(limits["max"]))
        )

    flags.append(
        F.when(
            condition,
            F.lit(f"ABNORMAL_{metric}"),
        )
    )


# ============================================================
# 9. Late Event Detection
# ============================================================

# This is simulated ingestion delay, not a watermark.

delay_minutes = (
    F.unix_timestamp(F.col("ingestion_time"))
    - F.unix_timestamp(F.col("event_time"))
) / 60.0

flags.append(
    F.when(
        delay_minutes >
        F.lit(rules["late_arrival"]["threshold_minutes"]),
        F.lit("LATE_ARRIVAL"),
    )
)


# ============================================================
# 10. Attach Validation Results
# ============================================================

validated = (
    parsed
    .withColumn(
        "validation_errors",
        F.filter(
            F.array(*errors),
            lambda x: x.isNotNull(),
        ),
    )
    .withColumn(
        "quality_flags",
        F.filter(
            F.array(*flags),
            lambda x: x.isNotNull(),
        ),
    )
    .withColumn(
        "ingestion_delay_minutes",
        delay_minutes,
    )
    .withColumn(
        "is_valid",
        F.size(F.col("validation_errors")) == 0,
    )
)


# ============================================================
# 11. Split Valid and Invalid Events
# ============================================================

valid_events = validated.filter(
    F.col("is_valid")
)

invalid_events = validated.filter(
    ~F.col("is_valid")
)


# ============================================================
# 12. Stateful Deduplication
# ============================================================

# Remove duplicate event IDs across micro-batches.
# No watermark: keep IDs in state until the query is reset.

silver_deduplicated = (
    valid_events
    .dropDuplicates(["event_id"])
)


# ============================================================
# 13. Write Silver Streaming
# ============================================================

silver_query = (
    silver_deduplicated.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print(
    "Silver V2 streaming started.",
    flush=True,
)


# ============================================================
# 14. Write Quarantine Streaming
# ============================================================

quarantine_query = (
    invalid_events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", QUARANTINE_PATH)
    .option("checkpointLocation", QUARANTINE_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print(
    "Quarantine V2 streaming started.",
    flush=True,
)


# ============================================================
# 15. Keep Both Queries Running
# ============================================================

print(
    "Telecom Silver Streaming V2 is running.",
    flush=True,
)

spark.streams.awaitAnyTermination()

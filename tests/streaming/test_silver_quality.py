
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Silver-Data-Quality-Test")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Test data only. No Kafka or checkpoint changes.
base_event = {
    "event_id": "TEST_001",
    "tower_id": "T001",
    "event_timestamp": "2026-10-11 10:00:00",
    "ingestion_timestamp": "2026-10-11 10:05:00",
    "signal_strength_dbm": -80.0,
    "call_drop_rate": 1.0,
    "latency_ms": 40.0,
    "download_speed_mbps": 50.0,
    "upload_speed_mbps": 15.0,
    "packet_loss_pct": 0.5,
    "network_status": "NORMAL",
    "incident_type_code": None,
}

cases = []

def add_case(name, changes=None, raw=None):
    event = base_event.copy()
    if changes:
        event.update(changes)
    cases.append((
        name,
        raw if raw is not None else json.dumps(event)
    ))

add_case("VALID")
add_case("MISSING_ID", {"event_id": None})
add_case("INVALID_STATUS", {"network_status": "UNKNOWN"})
add_case("INVALID_PACKET_LOSS", {"packet_loss_pct": 150})
add_case("ABNORMAL_LATENCY", {"latency_ms": 150})
add_case("MISSING_DOWNLOAD", {"download_speed_mbps": None})
add_case(
    "LATE_EVENT",
    {"ingestion_timestamp": "2026-10-11 12:00:00"}
)
add_case("MALFORMED_JSON", raw='{"event_id": "BROKEN",')

schema = StructType([
    StructField("test_name", StringType(), False),
    StructField("raw_json", StringType(), True),
])

df = spark.createDataFrame(cases, schema)

# Match the JSON schema used in Silver.
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("tower_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("signal_strength_dbm", DoubleType()),
    StructField("call_drop_rate", DoubleType()),
    StructField("latency_ms", DoubleType()),
    StructField("download_speed_mbps", DoubleType()),
    StructField("upload_speed_mbps", DoubleType()),
    StructField("packet_loss_pct", DoubleType()),
    StructField("network_status", StringType()),
    StructField("incident_type_code", StringType()),
    StructField("ingestion_timestamp", StringType()),
    StructField("_corrupt_record", StringType()),
])

parsed = (
    df.withColumn(
        "event",
        F.from_json(
            "raw_json",
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
        F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn(
        "ingestion_time",
        F.to_timestamp("ingestion_timestamp", "yyyy-MM-dd HH:mm:ss"),
    )
)

errors = F.array(
    F.when(
        F.col("_corrupt_record").isNotNull(),
        F.lit("MALFORMED_JSON"),
    ),
    F.when(
        F.col("event_id").isNull(),
        F.lit("MISSING_event_id"),
    ),
    F.when(
        ~F.col("network_status").isin("NORMAL", "DEGRADED", "DOWN"),
        F.lit("INVALID_NETWORK_STATUS"),
    ),
    F.when(
        F.col("packet_loss_pct") > 100,
        F.lit("INVALID_packet_loss_pct"),
    ),
)

flags = F.array(
    F.when(
        F.col("latency_ms") > 95,
        F.lit("ABNORMAL_latency_ms"),
    ),
    F.when(
        F.col("download_speed_mbps").isNull(),
        F.lit("MISSING_download_speed_mbps"),
    ),
    F.when(
        (
            F.unix_timestamp("ingestion_time")
            - F.unix_timestamp("event_time")
        ) / 60 > 60,
        F.lit("LATE_ARRIVAL"),
    ),
)

result = (
    parsed
    .withColumn(
        "validation_errors",
        F.filter(errors, lambda x: x.isNotNull()),
    )
    .withColumn(
        "quality_flags",
        F.filter(flags, lambda x: x.isNotNull()),
    )
    .withColumn(
        "destination",
        F.when(
            F.size("validation_errors") == 0,
            "SILVER",
        ).otherwise("QUARANTINE"),
    )
)

expected = {
    "VALID": ("SILVER", None),
    "MISSING_ID": ("QUARANTINE", "MISSING_event_id"),
    "INVALID_STATUS": ("QUARANTINE", "INVALID_NETWORK_STATUS"),
    "INVALID_PACKET_LOSS": (
        "QUARANTINE", "INVALID_packet_loss_pct"
    ),
    "ABNORMAL_LATENCY": ("SILVER", "ABNORMAL_latency_ms"),
    "MISSING_DOWNLOAD": (
        "SILVER", "MISSING_download_speed_mbps"
    ),
    "LATE_EVENT": ("SILVER", "LATE_ARRIVAL"),
    "MALFORMED_JSON": ("QUARANTINE", "MALFORMED_JSON"),
}

failures = []

for row in result.collect():
    destination, expected_code = expected[row.test_name]
    codes = row.validation_errors + row.quality_flags

    passed = (
        row.destination == destination
        and (
            expected_code is None
            or expected_code in codes
        )
    )

    print(
        f"{'PASS' if passed else 'FAIL'} | "
        f"{row.test_name} | {row.destination} | {codes}"
    )

    if not passed:
        failures.append(row.test_name)

spark.stop()

if failures:
    raise AssertionError(f"Failed cases: {failures}")

print("\nALL 8 DATA QUALITY TESTS PASSED")

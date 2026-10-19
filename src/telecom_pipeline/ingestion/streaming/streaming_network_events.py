#!/usr/bin/env python3
"""Generate ONLY network_events.csv from the existing telecom generator,
then optionally replay events into Kafka in ingestion-time order.

Dependencies: standard library for generation; kafka-python for publishing.
The original generator is imported without calling its main().
"""
import argparse
import csv
import importlib.util
import json
import logging
import sqlite3
import tempfile
import time
from pathlib import Path
from datetime import datetime

HERE = Path(__file__).resolve().parent
FIELDS = ["event_id", "tower_id", "event_timestamp", "signal_strength_dbm",
          "call_drop_rate", "latency_ms", "download_speed_mbps", "upload_speed_mbps",
          "packet_loss_pct", "network_status", "incident_type_code", "ingestion_timestamp"]
NUMERIC = set(FIELDS[3:9])


def generate(source: Path, csv_path: Path, overwrite: bool) -> None:
    if csv_path.exists() and not overwrite:
        raise FileExistsError(f"{csv_path} exists; use --overwrite to replace it")
    spec = importlib.util.spec_from_file_location("telecom_generator", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load generator: {source}")
    import sys
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=csv_path.parent) as temp:
        mod.generate_network_events(Path(temp))
        Path(temp, "network_events.csv").replace(csv_path)
    print(f"Generated {mod.N_NETWORK_EVENTS:,} network events: {csv_path}")


def prepare_index(csv_path: Path, db_path: Path) -> int:
    """Disk-based ordering by simulated ingestion time (not by event timestamp).
    Preserves late arrivals and avoids loading all rows into RAM.
    """
    db = sqlite3.connect(db_path)
    try:
        db.execute("CREATE TABLE events (arrival TEXT NOT NULL, seq INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        batch = []
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDS:
                raise ValueError(f"Unexpected CSV schema: {reader.fieldnames}")
            for seq, row in enumerate(reader):
                obj = {k: (float(v) if v != "" else None) if k in NUMERIC
                    else (v if v != "" else None) for k, v in row.items()}
                # ingestion_timestamp is simulated source-arrival metadata,
                # not the time this Kafka replay actually happens.
                arrival = row["ingestion_timestamp"]
                if not arrival:
                    raise ValueError(f"Missing ingestion_timestamp at row {seq + 2}")
                batch.append((arrival, seq, json.dumps(obj, separators=(",", ":"))))
                if len(batch) >= 5000:
                    db.executemany("INSERT INTO events VALUES (?,?,?)", batch)
                    batch.clear()
            if batch:
                db.executemany("INSERT INTO events VALUES (?,?,?)", batch)
        db.commit()
        db.execute("CREATE INDEX idx_arrival ON events(arrival,seq)")
        return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        db.close()


def publish(csv_path: Path, brokers: str, topic: str, rate: float,
            limit: int, start_row: int, acks: str, sort_arrival: bool):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit("Install dependency: python -m pip install kafka-python") from exc
    if rate <= 0:
        raise ValueError("--rate must be greater than zero")
    producer = KafkaProducer(
        bootstrap_servers=brokers.split(","),
        acks=acks, retries=10,
        enable_idempotence=(acks == "all"),
        max_in_flight_requests_per_connection=5,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    sent = 0
    pending = []
    start = time.monotonic()

    def send(obj):
        nonlocal sent
        future = producer.send(topic, key=obj["tower_id"], value=obj)
        pending.append(future)
        sent += 1
        if len(pending) >= 1000:
            for f in pending:
                f.get(timeout=60)
            pending.clear()
        deadline = start + sent / rate
        delay = deadline - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        if sent % 10000 == 0:
            logging.info("Published %s events", f"{sent:,}")

    try:
        if sort_arrival:
            with tempfile.TemporaryDirectory() as temp:
                db_path = Path(temp) / "replay.db"
                count = prepare_index(csv_path, db_path)
                logging.info("Indexed %s events by ingestion timestamp", f"{count:,}")
                db = sqlite3.connect(db_path)
                try:
                    cursor = db.execute("SELECT payload FROM events ORDER BY arrival,seq LIMIT ? OFFSET ?",
                                        (limit if limit else -1, start_row))
                    for (payload,) in cursor:
                        send(json.loads(payload))
                finally:
                    db.close()
        else:
            with csv_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                if reader.fieldnames != FIELDS:
                    raise ValueError("Unexpected CSV schema")
                for i, row in enumerate(reader):
                    if i < start_row:
                        continue
                    if limit and sent >= limit:
                        break
                    obj = {k: (float(v) if v else None) if k in NUMERIC
                           else (v if v else None) for k, v in row.items()}
                    send(obj)
        for f in pending:
            f.get(timeout=60)
        producer.flush(timeout=60)
        logging.info("Completed: %s events acknowledged by Kafka", f"{sent:,}")
    finally:
        producer.close(timeout=60)


def main():
    p = argparse.ArgumentParser(description="Generate and/or replay telecom network events")
    p.add_argument("mode", choices=["generate", "publish", "both"])
    p.add_argument(
    "--source-generator",
    type=Path,
    default=HERE.parent / "batch" / "generate_telecom_data_with_noise.py",)
    p.add_argument("--csv", type=Path, default=Path("data/raw/streaming/network_events.csv"))
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--topic", default="network_events")
    p.add_argument("--rate", type=float, default=100.0, help="Events/second")
    p.add_argument("--limit", type=int, default=0, help="0 means all events")
    p.add_argument("--start-row", type=int, default=0, help="Skip first N ordered rows; NOT exactly-once resume")
    p.add_argument("--acks", choices=["all", "1"], default="all")
    p.add_argument("--no-sort", action="store_true", help="Stream original tower-grouped CSV order (not recommended)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.limit < 0 or args.start_row < 0:
        p.error("--limit and --start-row must be non-negative")
    if args.mode in ("generate", "both"):
        generate(args.source_generator, args.csv, args.overwrite)
    if args.mode in ("publish", "both"):
        if not args.csv.is_file():
            p.error(f"CSV not found: {args.csv}")
        publish(args.csv, args.brokers, args.topic, args.rate, args.limit,
                args.start_row, args.acks, not args.no_sort)


if __name__ == "__main__":
    main()

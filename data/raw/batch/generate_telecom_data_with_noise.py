#!/usr/bin/env python3
"""
Consistent synthetic telecom data generator.

Default output sizes:
- 50 areas
- 500 towers
- 100,000 customers
- 1,000,000 network_events
- 10,000 network_incidents
- 750,000 bridge_incident_customer rows
- 75,000 complaints
- 150,000 customer_experience rows

Also generates small reference tables:
- plans
- services
- incident_types
- complaint_types

The generator is deterministic and uses only Python's standard library.
It writes CSV incrementally, so it does not need to keep millions of rows in RAM.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

# ---------------------------
# Configuration
# ---------------------------

SEED = 20261002

N_AREAS = 50
N_TOWERS = 500
N_CUSTOMERS = 100_000
N_NETWORK_EVENTS = 1_000_000
N_NETWORK_INCIDENTS = 10_000
N_BRIDGE_MAPPINGS = 750_000
N_COMPLAINTS = 75_000
N_CX = 150_000

# Exact structural ratios implied by requested volumes.
TOWERS_PER_AREA = N_TOWERS // N_AREAS                 # 10
CUSTOMERS_PER_AREA = N_CUSTOMERS // N_AREAS           # 2,000
INCIDENTS_PER_TOWER = N_NETWORK_INCIDENTS // N_TOWERS  # 20
EVENTS_PER_TOWER = N_NETWORK_EVENTS // N_TOWERS        # 2,000
AFFECTED_PER_INCIDENT = N_BRIDGE_MAPPINGS // N_NETWORK_INCIDENTS  # 75

# Complaints: 90% linked to incidents, 10% standalone.
N_LINKED_COMPLAINTS = 67_500
N_STANDALONE_COMPLAINTS = N_COMPLAINTS - N_LINKED_COMPLAINTS
BASE_LINKED_COMPLAINTS_PER_INCIDENT = N_LINKED_COMPLAINTS // N_NETWORK_INCIDENTS  # 6
EXTRA_LINKED_COMPLAINT_INCIDENTS = N_LINKED_COMPLAINTS % N_NETWORK_INCIDENTS      # 7,500

DATA_START = datetime(2026, 1, 1, 0, 0, 0)
DATA_END = datetime(2026, 9, 30, 23, 59, 59)
SPAN_SECONDS = int((DATA_END - DATA_START).total_seconds())


# ---------------------------
# Intentional data-quality noise
# ---------------------------
# Noise is added to non-key attributes so FK/PK consistency is preserved.
# Rates are deliberately small enough to keep the dataset usable for ETL/cleaning exercises.
ENABLE_NOISE = True

NOISE_RATES = {
    # Master/reference-style data
    "area_text_format": 0.04,          # casing + surrounding/duplicate whitespace
    "area_missing_optional": 0.02,     # governorate/region/population_density
    "tower_text_format": 0.035,        # tower_type/vendor formatting
    "tower_capacity_outlier": 0.008,   # extreme but CHECK-valid capacities
    "customer_name_format": 0.035,     # casing/whitespace
    "customer_missing_optional": 0.025,# name/age/age_group/segment
    "customer_age_group_mismatch": 0.015, # age_group intentionally disagrees with age

    # Network telemetry
    "event_missing_metric": 0.025,     # nullable telemetry metric becomes blank
    "event_metric_outlier": 0.012,     # extreme but schema-valid metric
    "event_status_mismatch": 0.006,    # status is business-inconsistent, but allowed by CHECK
    "event_ingestion_delay": 0.015,    # unusually late ingestion timestamp

    # Incident/CX/complaints
    "incident_missing_optional": 0.02, # nullable incident metrics/root_cause
    "incident_metric_outlier": 0.008,  # extreme but non-negative / CHECK-valid
    "complaint_text_format": 0.04,     # casing/whitespace/text dirtiness
    "complaint_missing_optional": 0.02,# channel/text/resolution fields where nullable
    "cx_missing_metric": 0.03,         # nullable CX metric becomes blank
    "cx_metric_edge_bias": 0.012,      # suspicious concentration at min/max valid boundaries
    "bridge_missing_optional": 0.015,  # impact_level/impact_score/end/duration become blank
}

# Separate RNG keeps noise reproducible and independent of the core generation logic.
NOISE_SEED = SEED + 909_090

CSV_KW = dict(
    delimiter=",",
    quotechar='"',
    quoting=csv.QUOTE_MINIMAL,
    lineterminator="\n",
)

# ---------------------------
# Static reference data
# ---------------------------

AREA_BASES = [
    ("Cairo", "Cairo", "Greater Cairo", 30.0444, 31.2357),
    ("Giza", "Giza", "Greater Cairo", 30.0131, 31.2089),
    ("Alexandria", "Alexandria", "North Coast", 31.2001, 29.9187),
    ("Mansoura", "Dakahlia", "Delta", 31.0409, 31.3785),
    ("Tanta", "Gharbia", "Delta", 30.7865, 31.0004),
    ("Zagazig", "Sharqia", "Delta", 30.5877, 31.5020),
    ("Ismailia", "Ismailia", "Canal", 30.5965, 32.2715),
    ("Port Said", "Port Said", "Canal", 31.2653, 32.3019),
    ("Suez", "Suez", "Canal", 29.9668, 32.5498),
    ("Fayoum", "Fayoum", "Upper Egypt", 29.3084, 30.8428),
]

PLANS = [
    ("PLAN-PRE-10", "Prepaid 10GB", "Prepaid", 120.00, 10.0, 300, 100, 25.0),
    ("PLAN-PRE-25", "Prepaid 25GB", "Prepaid", 220.00, 25.0, 700, 250, 50.0),
    ("PLAN-PRE-60", "Prepaid 60GB", "Prepaid", 390.00, 60.0, 1200, 500, 100.0),
    ("PLAN-POST-20", "Postpaid 20GB", "Postpaid", 280.00, 20.0, 1000, 500, 50.0),
    ("PLAN-POST-50", "Postpaid 50GB", "Postpaid", 480.00, 50.0, 2500, 1000, 100.0),
    ("PLAN-POST-100", "Postpaid 100GB", "Postpaid", 750.00, 100.0, 3000, 1000, 300.0),
    ("PLAN-BIZ-200", "Business 200GB", "Postpaid", 1350.00, 200.0, 6000, 2500, 500.0),
    ("PLAN-VIP-UNL", "VIP Unlimited", "Postpaid", 2200.00, None, 10000, 5000, 1000.0),
]

SERVICES = [
    ("SVC-VOICE", "Mobile Voice", "Voice", "LTE", "Mobile voice calling service"),
    ("SVC-MOBILE-DATA", "Mobile Data", "Data", "5G", "Mobile broadband data service"),
    ("SVC-LTE-DATA", "LTE Data", "Data", "LTE", "LTE mobile data service"),
    ("SVC-FWA", "Fixed Wireless Internet", "Internet", "5G", "5G fixed wireless access"),
    ("SVC-HOME", "Home Internet", "Internet", "Fiber", "Home internet service"),
]

INCIDENT_TYPES = [
    (1, "HIGH_LATENCY", "High Latency", "Performance", "HIGH",
     "Network response time exceeds accepted threshold"),
    (2, "CALL_DROP", "High Call Drop", "Quality", "HIGH",
     "Call-drop rate exceeds accepted threshold"),
    (3, "TOWER_DOWN", "Tower Outage", "Availability", "CRITICAL",
     "Tower/site is unavailable"),
    (4, "LOW_SIGNAL", "Low Signal Strength", "Quality", "MEDIUM",
     "Signal strength is below accepted level"),
    (5, "CONGESTION", "Network Congestion", "Performance", "HIGH",
     "Tower is congested and throughput is degraded"),
]

COMPLAINT_TYPES = [
    (1, "SLOW_INTERNET", "Slow Internet", "Network", "HIGH",
     "Customer reports slow internet performance"),
    (2, "NO_SIGNAL", "No Signal", "Network", "HIGH",
     "Customer reports no or very weak signal"),
    (3, "CALL_DROP", "Dropped Calls", "Network", "HIGH",
     "Customer reports calls dropping"),
    (4, "WRONG_BILL", "Incorrect Bill", "Billing", "MEDIUM",
     "Customer reports an incorrect bill"),
    (5, "SERVICE_DOWN", "Service Unavailable", "Service", "HIGH",
     "Customer reports service unavailability"),
]

INCIDENT_CODE_BY_ID = {r[0]: r[1] for r in INCIDENT_TYPES}
DEFAULT_SEVERITY_BY_ID = {r[0]: r[4] for r in INCIDENT_TYPES}

INCIDENT_TO_COMPLAINT_TYPES = {
    1: [1],       # HIGH_LATENCY -> SLOW_INTERNET
    2: [3],       # CALL_DROP -> CALL_DROP
    3: [2, 5],    # TOWER_DOWN -> NO_SIGNAL / SERVICE_DOWN
    4: [2],       # LOW_SIGNAL -> NO_SIGNAL
    5: [1],       # CONGESTION -> SLOW_INTERNET
}

ROOT_CAUSES = {
    1: ["Backhaul congestion", "Routing degradation", "Core network overload"],
    2: ["Radio interference", "Handover failure", "RAN parameter issue"],
    3: ["Power failure", "Site hardware failure", "Backhaul outage"],
    4: ["Antenna misalignment", "Coverage gap", "Radio interference"],
    5: ["High subscriber load", "Insufficient radio capacity", "Backhaul saturation"],
}

SEVERITY_LEVEL = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

# ---------------------------
# Helpers
# ---------------------------

@dataclass(frozen=True)
class IncidentMeta:
    index: int
    incident_id: str
    tower_idx: int
    tower_id: str
    area_idx: int
    area_id: str
    incident_type_id: int
    start: datetime
    end: datetime
    duration_minutes: float
    severity: str


def dt_str(value: Optional[datetime]) -> str:
    return "" if value is None else value.strftime("%Y-%m-%d %H:%M:%S")


def date_str(value: Optional[date]) -> str:
    return "" if value is None else value.isoformat()


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def noisy_text(rng: random.Random, value: str) -> str:
    """Formatting noise only: keeps identifiers/keys untouched."""
    choice = rng.choice(["upper", "lower", "lead_space", "trail_space", "double_space"])
    if choice == "upper":
        return value.upper()
    if choice == "lower":
        return value.lower()
    if choice == "lead_space":
        return "  " + value
    if choice == "trail_space":
        return value + "  "
    return value.replace(" ", "  ", 1) if " " in value else value + "  "


def maybe_blank(rng: random.Random, value, rate: float):
    if ENABLE_NOISE and rng.random() < rate:
        return ""
    return value


def maybe_noisy_text(rng: random.Random, value: str, rate: float) -> str:
    if ENABLE_NOISE and value and rng.random() < rate:
        return noisy_text(rng, value)
    return value


def write_noise_manifest(out: Path):
    rows = [
        ["Noise Type", "Rate", "Tables", "What is introduced", "Expected cleaning"],
        ["Text formatting", NOISE_RATES["area_text_format"], "areas",
         "upper/lower case, leading/trailing or duplicate spaces",
         "TRIM + standardize casing"],
        ["Missing optional values", NOISE_RATES["area_missing_optional"], "areas",
         "blank governorate/region/population_density",
         "null handling / imputation where appropriate"],
        ["Text formatting", NOISE_RATES["tower_text_format"], "towers",
         "dirty tower_type/vendor strings", "TRIM + casing/category normalization"],
        ["Capacity outliers", NOISE_RATES["tower_capacity_outlier"], "towers",
         "very large non-negative capacity values", "outlier detection/capping"],
        ["Customer text formatting", NOISE_RATES["customer_name_format"], "customers",
         "dirty customer names", "TRIM + casing normalization"],
        ["Missing customer attributes", NOISE_RATES["customer_missing_optional"], "customers",
         "blank nullable name/age/age_group/segment", "null profiling/imputation"],
        ["Age-group mismatch", NOISE_RATES["customer_age_group_mismatch"], "customers",
         "age_group does not match numeric age", "derive age_group again from age"],
        ["Missing telemetry metric", NOISE_RATES["event_missing_metric"], "network_events",
         "blank nullable signal/drop/latency/speed/loss metric", "null handling"],
        ["Telemetry outlier", NOISE_RATES["event_metric_outlier"], "network_events",
         "extreme but CHECK-valid values", "range/business-rule validation"],
        ["Status/metric mismatch", NOISE_RATES["event_status_mismatch"], "network_events",
         "NORMAL may carry bad metrics or DEGRADED may carry normal-looking metrics",
         "cross-field business validation"],
        ["Late ingestion", NOISE_RATES["event_ingestion_delay"], "network_events",
         "ingestion timestamp delayed by hours/days", "latency/anomaly checks"],
        ["Missing incident attributes", NOISE_RATES["incident_missing_optional"], "network_incidents",
         "blank nullable metrics/root_cause", "null handling"],
        ["Incident outlier", NOISE_RATES["incident_metric_outlier"], "network_incidents",
         "extreme positive metric", "outlier detection"],
        ["Complaint text dirtiness", NOISE_RATES["complaint_text_format"], "complaints",
         "case/spacing problems in text/channel", "text/category normalization"],
        ["Missing complaint optionals", NOISE_RATES["complaint_missing_optional"], "complaints",
         "blank channel/text or some nullable resolution attributes", "null handling"],
        ["Missing CX metric", NOISE_RATES["cx_missing_metric"], "customer_experience",
         "blank nullable scores", "null handling"],
        ["CX boundary bias", NOISE_RATES["cx_metric_edge_bias"], "customer_experience",
         "valid but suspicious repeated min/max score values", "distribution anomaly checks"],
        ["Missing bridge optionals", NOISE_RATES["bridge_missing_optional"], "bridge_incident_customer",
         "blank impact_end/duration/level/score", "null handling and derived-field repair"],
    ]
    with (out / "noise_manifest.csv").open("w", newline="", encoding="utf-8") as f:
        csv.writer(f, **CSV_KW).writerows(rows)



def weighted_choice(rng: random.Random, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def customer_id(customer_idx: int) -> str:
    return f"C{customer_idx + 1:06d}"


def tower_id(tower_idx: int) -> str:
    return f"T{tower_idx + 1:04d}"


def area_id(area_idx: int) -> str:
    return f"AREA-{area_idx + 1:03d}"


def incident_id(incident_idx: int) -> str:
    return f"INC-{incident_idx + 1:06d}"


def area_for_customer_idx(customer_idx: int) -> int:
    return customer_idx // CUSTOMERS_PER_AREA


def customer_indices_for_area(area_idx: int):
    start = area_idx * CUSTOMERS_PER_AREA
    return range(start, start + CUSTOMERS_PER_AREA)


def linked_complaint_count_for_incident(incident_idx: int) -> int:
    return BASE_LINKED_COMPLAINTS_PER_INCIDENT + (1 if incident_idx < EXTRA_LINKED_COMPLAINT_INCIDENTS else 0)


def incident_rng(incident_idx: int, salt: int = 0) -> random.Random:
    return random.Random(SEED * 1_000_003 + incident_idx * 9_973 + salt)


def affected_customer_indices(incident_idx: int, area_idx: int) -> list[int]:
    """Exactly 75 unique customers from the same area as the incident."""
    rng = incident_rng(incident_idx, salt=77)
    candidates = customer_indices_for_area(area_idx)
    return rng.sample(candidates, AFFECTED_PER_INCIDENT)


def write_csv(path: Path, header: list[str], rows: Iterable[Iterable]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, **CSV_KW)
        w.writerow(header)
        w.writerows(rows)


def random_datetime(rng: random.Random, start: datetime = DATA_START, end: datetime = DATA_END) -> datetime:
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, max(0, seconds)))


def age_group(age: int) -> str:
    if age <= 25:
        return "18-25"
    if age <= 35:
        return "26-35"
    if age <= 45:
        return "36-45"
    if age <= 55:
        return "46-55"
    if age <= 65:
        return "56-65"
    return "66+"


def make_incident_meta(incident_idx: int) -> IncidentMeta:
    """
    Deterministic mapping:
      20 incidents per tower
      tower -> area is deterministic (10 towers per area)
    """
    tw_idx = incident_idx // INCIDENTS_PER_TOWER
    ar_idx = tw_idx // TOWERS_PER_AREA
    rng = incident_rng(incident_idx, salt=11)

    # Keep enough room before DATA_END for the full incident duration.
    max_start = DATA_END - timedelta(hours=8)
    start = random_datetime(rng, DATA_START, max_start)

    incident_type_id = weighted_choice(rng, [1, 2, 3, 4, 5], [22, 18, 10, 20, 30])
    severity = DEFAULT_SEVERITY_BY_ID[incident_type_id]

    # Add some variation while keeping severity constrained.
    if incident_type_id in (1, 2, 5) and rng.random() < 0.25:
        severity = "MEDIUM"
    if incident_type_id == 3 and rng.random() < 0.20:
        severity = "HIGH"
    if incident_type_id == 4 and rng.random() < 0.12:
        severity = "LOW"

    duration = round(rng.uniform(20, 360), 2)
    if severity == "CRITICAL":
        duration = round(rng.uniform(60, 480), 2)
    end = start + timedelta(minutes=duration)

    return IncidentMeta(
        index=incident_idx,
        incident_id=incident_id(incident_idx),
        tower_idx=tw_idx,
        tower_id=tower_id(tw_idx),
        area_idx=ar_idx,
        area_id=area_id(ar_idx),
        incident_type_id=incident_type_id,
        start=start,
        end=end,
        duration_minutes=duration,
        severity=severity,
    )


# ---------------------------
# Generators
# ---------------------------

def generate_reference_tables(out: Path):
    write_csv(
        out / "plans.csv",
        ["plan_id", "plan_name", "plan_type", "monthly_fee", "data_limit_gb",
         "voice_minutes", "sms_limit", "speed_limit_mbps",
         "effective_from", "effective_to", "is_active"],
        (
            [pid, name, ptype, fee, "" if data is None else data, voice, sms, speed,
             "2026-01-01", "", True]
            for pid, name, ptype, fee, data, voice, sms, speed in PLANS
        ),
    )

    write_csv(
        out / "services.csv",
        ["service_id", "service_name", "service_category", "technology", "description", "is_active"],
        ([sid, name, cat, tech, desc, True] for sid, name, cat, tech, desc in SERVICES),
    )

    write_csv(
        out / "incident_types.csv",
        ["incident_type_id", "incident_type_code", "incident_type_name", "category",
         "default_severity", "description"],
        INCIDENT_TYPES,
    )

    write_csv(
        out / "complaint_types.csv",
        ["complaint_type_id", "complaint_code", "complaint_name", "category",
         "default_priority", "description"],
        COMPLAINT_TYPES,
    )


def generate_areas(out: Path):
    rng = random.Random(SEED + 1)
    noise_rng = random.Random(NOISE_SEED + 1)

    def rows():
        for i in range(N_AREAS):
            city, gov, region, base_lat, base_lon = AREA_BASES[i % len(AREA_BASES)]
            cluster = i // len(AREA_BASES) + 1
            lat = clamp(base_lat + rng.uniform(-0.12, 0.12), -90, 90)
            lon = clamp(base_lon + rng.uniform(-0.12, 0.12), -180, 180)
            density = round(rng.uniform(2_000, 30_000), 2)

            area_name = f"{city} Area {cluster:02d}"
            out_city = city
            out_gov = gov
            out_region = region

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["area_text_format"]:
                target = noise_rng.choice(["area_name", "city", "governorate", "region"])
                if target == "area_name":
                    area_name = noisy_text(noise_rng, area_name)
                elif target == "city":
                    out_city = noisy_text(noise_rng, out_city)
                elif target == "governorate":
                    out_gov = noisy_text(noise_rng, out_gov)
                else:
                    out_region = noisy_text(noise_rng, out_region)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["area_missing_optional"]:
                target = noise_rng.choice(["governorate", "region", "population_density"])
                if target == "governorate":
                    out_gov = ""
                elif target == "region":
                    out_region = ""
                else:
                    density = ""

            yield [
                area_id(i), area_name, out_city, out_gov, out_region, "Egypt",
                round(lat, 6), round(lon, 6), density,
            ]

    write_csv(
        out / "areas.csv",
        ["area_id", "area_name", "city", "governorate", "region", "country",
         "latitude", "longitude", "population_density"],
        rows(),
    )

def area_coordinates(area_idx: int) -> tuple[float, float]:
    """Same deterministic coordinates used by area generation."""
    rng = random.Random(SEED + 1)
    coords = None
    for i in range(area_idx + 1):
        city, gov, region, base_lat, base_lon = AREA_BASES[i % len(AREA_BASES)]
        lat = clamp(base_lat + rng.uniform(-0.12, 0.12), -90, 90)
        lon = clamp(base_lon + rng.uniform(-0.12, 0.12), -180, 180)
        _density = rng.uniform(2_000, 30_000)
        coords = (lat, lon)
    assert coords is not None
    return coords


def generate_towers(out: Path):
    rng = random.Random(SEED + 2)
    noise_rng = random.Random(NOISE_SEED + 2)
    tower_types = ["Macro", "Micro", "Small Cell"]
    technologies = ["4G", "5G", "LTE"]
    vendors = ["Ericsson", "Huawei", "Nokia", "ZTE"]
    statuses = ["ACTIVE", "INACTIVE", "MAINTENANCE", "DECOMMISSIONED"]

    area_coords = [area_coordinates(i) for i in range(N_AREAS)]

    def rows():
        for i in range(N_TOWERS):
            ar_idx = i // TOWERS_PER_AREA
            base_lat, base_lon = area_coords[ar_idx]
            lat = clamp(base_lat + rng.uniform(-0.025, 0.025), -90, 90)
            lon = clamp(base_lon + rng.uniform(-0.025, 0.025), -180, 180)
            installed = date(2017, 1, 1) + timedelta(days=rng.randint(0, 3400))
            status = weighted_choice(rng, statuses, [94, 1, 4, 1])
            tower_type = weighted_choice(rng, tower_types, [75, 15, 10])
            vendor = rng.choice(vendors)
            capacity = rng.randint(2_000, 12_000)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["tower_text_format"]:
                if noise_rng.random() < 0.5:
                    tower_type = noisy_text(noise_rng, tower_type)
                else:
                    vendor = noisy_text(noise_rng, vendor)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["tower_capacity_outlier"]:
                capacity = noise_rng.randint(50_000, 250_000)

            yield [
                tower_id(i), f"Tower {i + 1:04d}", area_id(ar_idx),
                round(lat, 6), round(lon, 6), tower_type,
                weighted_choice(rng, technologies, [35, 45, 20]),
                vendor, capacity, date_str(installed), status,
            ]

    write_csv(
        out / "towers.csv",
        ["tower_id", "tower_name", "area_id", "latitude", "longitude",
         "tower_type", "technology", "vendor", "capacity", "installation_date", "status"],
        rows(),
    )

def generate_customers(out: Path):
    rng = random.Random(SEED + 3)
    noise_rng = random.Random(NOISE_SEED + 3)
    first_names = ["Ahmed", "Mohamed", "Omar", "Youssef", "Karim", "Mariam", "Sara", "Nour", "Salma", "Hana"]
    last_names = ["Hassan", "Ali", "Mahmoud", "Ibrahim", "Mostafa", "Khaled", "Fathy", "Samir", "Adel", "Nassar"]
    plan_ids = [x[0] for x in PLANS]
    service_ids = [x[0] for x in SERVICES]
    segments = ["Consumer", "Business", "VIP"]
    statuses = ["Active", "Suspended", "Cancelled"]

    def rows():
        for i in range(N_CUSTOMERS):
            ar_idx = area_for_customer_idx(i)
            age = rng.randint(18, 78)
            segment = weighted_choice(rng, segments, [88, 9, 3])
            plan = weighted_choice(
                rng, plan_ids,
                [22, 20, 15, 14, 12, 10, 5, 2] if segment != "Business"
                else [3, 5, 7, 10, 16, 22, 27, 10],
            )
            service = weighted_choice(rng, service_ids, [15, 42, 16, 12, 15])
            status = weighted_choice(rng, statuses, [94, 3, 3])

            activation = date(2019, 1, 1) + timedelta(days=rng.randint(0, 2730))
            if activation > DATA_END.date():
                activation = DATA_END.date() - timedelta(days=rng.randint(1, 365))

            churn = None
            if status == "Cancelled":
                days_available = max(0, (DATA_END.date() - activation).days)
                churn = activation + timedelta(days=rng.randint(0, days_available))

            name = f"{rng.choice(first_names)} {rng.choice(last_names)}"
            out_age = age
            out_age_group = age_group(age)
            out_segment = segment

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["customer_name_format"]:
                name = noisy_text(noise_rng, name)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["customer_age_group_mismatch"]:
                valid_groups = ["18-25", "26-35", "36-45", "46-55", "56-65", "66+"]
                wrong = [g for g in valid_groups if g != out_age_group]
                out_age_group = noise_rng.choice(wrong)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["customer_missing_optional"]:
                target = noise_rng.choice(["name", "age", "age_group", "segment"])
                if target == "name":
                    name = ""
                elif target == "age":
                    out_age = ""
                elif target == "age_group":
                    out_age_group = ""
                else:
                    out_segment = ""

            yield [
                customer_id(i), name, area_id(ar_idx), plan, service,
                out_age, out_age_group, out_segment, status,
                date_str(activation), date_str(churn), dt_str(DATA_END),
            ]

    write_csv(
        out / "customers.csv",
        ["customer_id", "customer_name", "area_id", "plan_id", "service_id",
         "age", "age_group", "customer_segment", "subscription_status",
         "activation_date", "churn_date", "last_updated_at"],
        rows(),
    )

def incident_metrics(meta: IncidentMeta, rng: random.Random):
    sev = SEVERITY_LEVEL[meta.severity]
    if meta.incident_type_id == 1:  # latency
        avg_latency = rng.uniform(220, 650) + sev * 30
        max_latency = avg_latency + rng.uniform(80, 350)
        signal = rng.uniform(-102, -80)
        call_drop = rng.uniform(3, 14)
        dl = rng.uniform(4, 35)
    elif meta.incident_type_id == 2:  # call drop
        avg_latency = rng.uniform(80, 300)
        max_latency = avg_latency + rng.uniform(50, 220)
        signal = rng.uniform(-110, -88)
        call_drop = rng.uniform(15, 45)
        dl = rng.uniform(5, 55)
    elif meta.incident_type_id == 3:  # tower down
        avg_latency = rng.uniform(500, 2000)
        max_latency = avg_latency + rng.uniform(200, 1500)
        signal = rng.uniform(-120, -108)
        call_drop = rng.uniform(65, 100)
        dl = rng.uniform(0, 1.5)
    elif meta.incident_type_id == 4:  # low signal
        avg_latency = rng.uniform(120, 420)
        max_latency = avg_latency + rng.uniform(80, 300)
        signal = rng.uniform(-120, -103)
        call_drop = rng.uniform(8, 28)
        dl = rng.uniform(2, 25)
    else:  # congestion
        avg_latency = rng.uniform(180, 600)
        max_latency = avg_latency + rng.uniform(100, 400)
        signal = rng.uniform(-100, -78)
        call_drop = rng.uniform(5, 18)
        dl = rng.uniform(1.5, 18)

    return (
        round(max_latency, 2),
        round(avg_latency, 2),
        round(signal, 2),
        round(clamp(call_drop, 0, 100), 2),
        round(max(0, dl), 2),
    )


def generate_network_incidents(out: Path):
    def rows():
        for i in range(N_NETWORK_INCIDENTS):
            meta = make_incident_meta(i)
            rng = incident_rng(i, salt=31)
            max_latency, avg_latency, avg_signal, max_drop, avg_dl = incident_metrics(meta, rng)
            noise_rng = incident_rng(i, salt=9031)
            root_cause = rng.choice(ROOT_CAUSES[meta.incident_type_id])

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["incident_metric_outlier"]:
                target = noise_rng.choice(["max_latency", "avg_latency", "max_drop", "avg_dl"])
                if target == "max_latency":
                    max_latency = round(noise_rng.uniform(5_000, 25_000), 2)
                elif target == "avg_latency":
                    avg_latency = round(noise_rng.uniform(2_000, 8_000), 2)
                elif target == "max_drop":
                    max_drop = round(noise_rng.uniform(80, 100), 2)
                else:
                    avg_dl = round(noise_rng.uniform(800, 2_000), 2)

            if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["incident_missing_optional"]:
                target = noise_rng.choice(["max_latency", "avg_latency", "avg_signal", "max_drop", "avg_dl", "root_cause"])
                if target == "max_latency": max_latency = ""
                elif target == "avg_latency": avg_latency = ""
                elif target == "avg_signal": avg_signal = ""
                elif target == "max_drop": max_drop = ""
                elif target == "avg_dl": avg_dl = ""
                else: root_cause = ""

            yield [
                meta.incident_id,
                meta.tower_id,
                meta.area_id,
                meta.incident_type_id,
                dt_str(meta.start),
                dt_str(meta.end),
                meta.duration_minutes,
                meta.severity,
                max_latency,
                avg_latency,
                avg_signal,
                max_drop,
                avg_dl,
                AFFECTED_PER_INCIDENT,
                linked_complaint_count_for_incident(i),
                "RESOLVED",
                root_cause,
            ]

    write_csv(
        out / "network_incidents.csv",
        ["incident_id", "tower_id", "area_id", "incident_type_id",
         "start_timestamp", "end_timestamp", "duration_minutes", "severity",
         "max_latency_ms", "avg_latency_ms", "avg_signal_strength_dbm",
         "max_call_drop_rate", "avg_download_speed_mbps",
         "affected_customer_count", "complaint_count",
         "incident_status", "root_cause"],
        rows(),
    )


def generate_bridge(out: Path):
    def rows():
        for i in range(N_NETWORK_INCIDENTS):
            meta = make_incident_meta(i)
            affected = affected_customer_indices(i, meta.area_idx)
            rng = incident_rng(i, salt=44)

            for cust_idx in affected:
                start_offset = rng.uniform(0, min(15, meta.duration_minutes / 4))
                impact_start = meta.start + timedelta(minutes=start_offset)
                max_duration = max(1.0, (meta.end - impact_start).total_seconds() / 60)
                impact_duration = round(rng.uniform(max(1.0, max_duration * 0.45), max_duration), 2)
                impact_end = impact_start + timedelta(minutes=impact_duration)

                base = {"LOW": 28, "MEDIUM": 50, "HIGH": 72, "CRITICAL": 88}[meta.severity]
                score = round(clamp(rng.gauss(base, 10), 0, 100), 2)
                if score >= 75:
                    level = "HIGH"
                elif score >= 45:
                    level = "MEDIUM"
                else:
                    level = "LOW"

                out_end = dt_str(impact_end)
                out_duration = impact_duration
                out_level = level
                out_score = score
                if ENABLE_NOISE and rng.random() < NOISE_RATES["bridge_missing_optional"]:
                    target = rng.choice(["end", "duration", "level", "score"])
                    if target == "end": out_end = ""
                    elif target == "duration": out_duration = ""
                    elif target == "level": out_level = ""
                    else: out_score = ""

                yield [
                    meta.incident_id,
                    customer_id(cust_idx),
                    dt_str(impact_start),
                    out_end,
                    out_duration,
                    out_level,
                    out_score,
                    True,
                ]

    write_csv(
        out / "bridge_incident_customer.csv",
        ["incident_id", "customer_id", "impact_start_timestamp", "impact_end_timestamp",
         "impact_duration_minutes", "impact_level", "impact_score", "service_affected"],
        rows(),
    )


def bad_event_metrics(meta: IncidentMeta, rng: random.Random):
    """Metrics aligned with the incident type."""
    if meta.incident_type_id == 1:  # HIGH_LATENCY
        return (
            rng.uniform(-100, -78),
            rng.uniform(4, 18),
            rng.uniform(250, 1200),
            rng.uniform(2, 30),
            rng.uniform(0.5, 10),
            rng.uniform(1, 12),
            "DEGRADED",
        )
    if meta.incident_type_id == 2:  # CALL_DROP
        return (
            rng.uniform(-108, -85),
            rng.uniform(18, 65),
            rng.uniform(80, 450),
            rng.uniform(3, 45),
            rng.uniform(1, 15),
            rng.uniform(1, 18),
            "DEGRADED",
        )
    if meta.incident_type_id == 3:  # TOWER_DOWN
        return (
            rng.uniform(-120, -110),
            rng.uniform(75, 100),
            rng.uniform(900, 4000),
            rng.uniform(0, 0.8),
            rng.uniform(0, 0.3),
            rng.uniform(50, 100),
            "DOWN",
        )
    if meta.incident_type_id == 4:  # LOW_SIGNAL
        return (
            rng.uniform(-120, -104),
            rng.uniform(8, 35),
            rng.uniform(120, 600),
            rng.uniform(1, 22),
            rng.uniform(0.5, 8),
            rng.uniform(2, 20),
            "DEGRADED",
        )
    # CONGESTION
    return (
        rng.uniform(-102, -80),
        rng.uniform(5, 22),
        rng.uniform(200, 900),
        rng.uniform(1, 15),
        rng.uniform(0.5, 7),
        rng.uniform(3, 25),
        "DEGRADED",
    )


def normal_event_metrics(rng: random.Random):
    signal = rng.uniform(-95, -65)
    call_drop = rng.uniform(0, 3.5)
    latency = rng.uniform(15, 95)
    dl = rng.uniform(25, 350)
    ul = rng.uniform(8, 90)
    packet = rng.uniform(0, 1.5)
    return signal, call_drop, latency, dl, ul, packet, "NORMAL"


def generate_network_events(out: Path):
    """
    Exactly 2,000 events/tower = 1,000,000 total.
    Core incident correlation is kept, while small controlled noise is added to nullable
    metrics/status/timing for data-cleaning practice.
    """
    path = out / "network_events.csv"
    header = [
        "event_id", "tower_id", "event_timestamp", "signal_strength_dbm",
        "call_drop_rate", "latency_ms", "download_speed_mbps",
        "upload_speed_mbps", "packet_loss_pct", "network_status",
        "incident_type_code", "ingestion_timestamp",
    ]

    def add_event_noise(noise_rng, values, status, ingestion_ts):
        # values = [signal, drop, latency, download, upload, packet_loss]
        vals = list(values)

        if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["event_missing_metric"]:
            vals[noise_rng.randrange(len(vals))] = ""

        if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["event_metric_outlier"]:
            idx = noise_rng.randrange(len(vals))
            if idx == 0:
                vals[idx] = round(noise_rng.uniform(-120, -115), 2)  # still realistic extreme
            elif idx in (1, 5):
                vals[idx] = round(noise_rng.uniform(70, 100), 2)     # CHECK-valid %
            elif idx == 2:
                vals[idx] = round(noise_rng.uniform(4_000, 20_000), 2)
            else:
                vals[idx] = round(noise_rng.uniform(800, 2_500), 2)

        if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["event_status_mismatch"]:
            status = noise_rng.choice(["NORMAL", "DEGRADED", "DOWN"])

        if ENABLE_NOISE and noise_rng.random() < NOISE_RATES["event_ingestion_delay"]:
            ingestion_ts += timedelta(hours=noise_rng.randint(2, 72))

        return vals, status, ingestion_ts

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, **CSV_KW)
        w.writerow(header)
        event_counter = 0

        for tw_idx in range(N_TOWERS):
            rng = random.Random(SEED + 1000 + tw_idx)
            noise_rng = random.Random(NOISE_SEED + 1000 + tw_idx)
            tower_incident_start_idx = tw_idx * INCIDENTS_PER_TOWER

            for local_inc in range(INCIDENTS_PER_TOWER):
                inc_idx = tower_incident_start_idx + local_inc
                meta = make_incident_meta(inc_idx)
                for frac in (0.10, 0.50, 0.90):
                    ts = meta.start + timedelta(
                        seconds=int((meta.end - meta.start).total_seconds() * frac)
                    )
                    sig, drop, lat, dl, ul, pkt, status = bad_event_metrics(meta, rng)
                    ingestion = ts + timedelta(seconds=rng.randint(1, 30))
                    vals, status, ingestion = add_event_noise(
                        noise_rng,
                        [round(sig, 2), round(clamp(drop, 0, 100), 2),
                         round(max(0, lat), 2), round(max(0, dl), 2),
                         round(max(0, ul), 2), round(clamp(pkt, 0, 100), 2)],
                        status, ingestion,
                    )
                    event_counter += 1
                    w.writerow([
                        f"EVT-{event_counter:09d}", tower_id(tw_idx), dt_str(ts),
                        *vals, status, INCIDENT_CODE_BY_ID[meta.incident_type_id],
                        dt_str(ingestion),
                    ])

            background = EVENTS_PER_TOWER - INCIDENTS_PER_TOWER * 3
            for _ in range(background):
                ts = DATA_START + timedelta(seconds=rng.randint(0, SPAN_SECONDS))
                sig, drop, lat, dl, ul, pkt, status = normal_event_metrics(rng)
                ingestion = ts + timedelta(seconds=rng.randint(1, 30))
                vals, status, ingestion = add_event_noise(
                    noise_rng,
                    [round(sig, 2), round(drop, 2), round(lat, 2),
                     round(dl, 2), round(ul, 2), round(pkt, 2)],
                    status, ingestion,
                )
                event_counter += 1
                w.writerow([
                    f"EVT-{event_counter:09d}", tower_id(tw_idx), dt_str(ts),
                    *vals, status, "", dt_str(ingestion),
                ])

    if event_counter != N_NETWORK_EVENTS:
        raise RuntimeError(f"network_events count mismatch: {event_counter:,}")


def complaint_resolution(
    rng: random.Random,
    created: datetime,
    priority: str,
    force_closed_probability: float = 0.88,
):
    status = weighted_choice(rng, ["OPEN", "PENDING", "CLOSED"], [5, 7, 88])
    if rng.random() > force_closed_probability:
        status = rng.choice(["OPEN", "PENDING"])

    if status == "CLOSED":
        minutes = round(rng.uniform(10, 1440), 2)
        resolved = created + timedelta(minutes=minutes)
        if resolved > DATA_END + timedelta(days=2):
            resolved = DATA_END + timedelta(days=2)
            minutes = round(max(0, (resolved - created).total_seconds() / 60), 2)
        threshold = {"HIGH": 240, "MEDIUM": 480, "LOW": 720}[priority]
        breached = minutes > threshold
        return status, resolved, minutes, breached

    return status, None, None, False


def generate_complaints(out: Path):
    complaint_counter = 0
    path = out / "complaints.csv"
    header = [
        "complaint_id", "customer_id", "complaint_type_id", "incident_id",
        "created_timestamp", "resolved_timestamp", "status", "priority",
        "channel", "complaint_text", "resolution_minutes", "sla_breached",
    ]
    channels = ["Call Center", "Mobile App", "Web", "Branch"]
    complaint_texts = {
        1: "Internet is noticeably slow and unstable.",
        2: "There is no usable signal in my area.",
        3: "Calls keep dropping during conversations.",
        4: "The billed amount does not match my expected charges.",
        5: "The service is unavailable and cannot be used.",
    }
    priority_by_type = {r[0]: r[4] for r in COMPLAINT_TYPES}

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, **CSV_KW)
        w.writerow(header)

        # Linked complaints: customers are chosen from the actual bridge set.
        for inc_idx in range(N_NETWORK_INCIDENTS):
            meta = make_incident_meta(inc_idx)
            affected = affected_customer_indices(inc_idx, meta.area_idx)
            rng = incident_rng(inc_idx, salt=55)
            count = linked_complaint_count_for_incident(inc_idx)

            for slot in range(count):
                cust_idx = affected[(slot * 11 + inc_idx) % len(affected)]
                ctype = rng.choice(INCIDENT_TO_COMPLAINT_TYPES[meta.incident_type_id])
                priority = priority_by_type[ctype]

                # Complaint occurs during the incident or shortly after it.
                max_offset = int(meta.duration_minutes + 120)
                created = meta.start + timedelta(minutes=rng.randint(0, max(1, max_offset)))
                status, resolved, resolution_minutes, breached = complaint_resolution(
                    rng, created, priority
                )

                complaint_counter += 1
                w.writerow([
                    f"CMP-{complaint_counter:07d}",
                    customer_id(cust_idx),
                    ctype,
                    meta.incident_id,
                    dt_str(created),
                    dt_str(resolved),
                    status,
                    priority,
                    maybe_noisy_text(rng, rng.choice(channels), NOISE_RATES["complaint_text_format"]),
                    maybe_blank(rng, maybe_noisy_text(rng, complaint_texts[ctype], NOISE_RATES["complaint_text_format"]), NOISE_RATES["complaint_missing_optional"]),
                    "" if resolution_minutes is None else resolution_minutes,
                    breached,
                ])

        # Standalone complaints: no incident_id; mostly billing/service issues.
        rng = random.Random(SEED + 6000)
        for _ in range(N_STANDALONE_COMPLAINTS):
            cust_idx = rng.randrange(N_CUSTOMERS)
            ctype = weighted_choice(rng, [1, 2, 3, 4, 5], [10, 8, 7, 55, 20])
            priority = priority_by_type[ctype]
            created = random_datetime(rng)
            status, resolved, resolution_minutes, breached = complaint_resolution(
                rng, created, priority
            )

            complaint_counter += 1
            w.writerow([
                f"CMP-{complaint_counter:07d}",
                customer_id(cust_idx),
                ctype,
                "",
                dt_str(created),
                dt_str(resolved),
                status,
                priority,
                maybe_noisy_text(rng, rng.choice(channels), NOISE_RATES["complaint_text_format"]),
                maybe_blank(rng, maybe_noisy_text(rng, complaint_texts[ctype], NOISE_RATES["complaint_text_format"]), NOISE_RATES["complaint_missing_optional"]),
                "" if resolution_minutes is None else resolution_minutes,
                breached,
            ])

    if complaint_counter != N_COMPLAINTS:
        raise RuntimeError(f"complaints count mismatch: {complaint_counter:,}")


def scores_for_impacted_customer(rng: random.Random, severity: str):
    sev = SEVERITY_LEVEL[severity]
    # More severe incident -> lower expected experience.
    base_rating = {1: 3.7, 2: 3.1, 3: 2.4, 4: 1.8}[sev]
    rating = round(clamp(rng.gauss(base_rating, 0.65), 1, 5), 1)
    call_q = round(clamp(rng.gauss(base_rating + 0.1, 0.7), 1, 5), 1)
    internet_q = round(clamp(rng.gauss(base_rating - 0.2, 0.7), 1, 5), 1)
    network_q = round(clamp((call_q + internet_q + rating) / 3 + rng.uniform(-0.3, 0.3), 1, 5), 1)
    satisfaction = round(clamp((rating - 1) / 4 * 100 + rng.gauss(0, 8), 0, 100), 2)
    nps = int(round(clamp((satisfaction / 100) * 10 + rng.gauss(0, 1.2), 0, 10)))
    return rating, satisfaction, nps, call_q, internet_q, network_q


def scores_for_baseline_customer(rng: random.Random):
    rating = round(clamp(rng.gauss(4.0, 0.7), 1, 5), 1)
    call_q = round(clamp(rng.gauss(4.0, 0.7), 1, 5), 1)
    internet_q = round(clamp(rng.gauss(3.9, 0.75), 1, 5), 1)
    network_q = round(clamp((rating + call_q + internet_q) / 3 + rng.uniform(-0.25, 0.25), 1, 5), 1)
    satisfaction = round(clamp((rating - 1) / 4 * 100 + rng.gauss(0, 7), 0, 100), 2)
    nps = int(round(clamp((satisfaction / 100) * 10 + rng.gauss(0, 1.0), 0, 10)))
    return rating, satisfaction, nps, call_q, internet_q, network_q


def generate_customer_experience(out: Path):
    path = out / "customer_experience.csv"
    header = [
        "cx_id", "customer_id", "event_timestamp", "rating",
        "satisfaction_score", "nps_score", "call_quality_score",
        "internet_experience_score", "network_quality_score", "channel",
    ]
    channels = ["Mobile App", "SMS", "Call Center", "Survey"]
    counter = 0

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, **CSV_KW)
        w.writerow(header)

        # First half: incident-correlated CX records from actually affected customers.
        rng = random.Random(SEED + 7000)
        for i in range(N_CX // 2):
            inc_idx = i % N_NETWORK_INCIDENTS
            meta = make_incident_meta(inc_idx)
            affected = affected_customer_indices(inc_idx, meta.area_idx)
            cust_idx = affected[(i * 13 + inc_idx) % len(affected)]
            ts = meta.end + timedelta(minutes=rng.randint(0, 240))
            scores = list(scores_for_impacted_customer(rng, meta.severity))
            if ENABLE_NOISE and rng.random() < NOISE_RATES["cx_missing_metric"]:
                scores[rng.randrange(len(scores))] = ""
            elif ENABLE_NOISE and rng.random() < NOISE_RATES["cx_metric_edge_bias"]:
                idx = rng.randrange(len(scores))
                if idx in (0, 3, 4, 5): scores[idx] = rng.choice([1.0, 5.0])
                elif idx == 1: scores[idx] = rng.choice([0.0, 100.0])
                else: scores[idx] = rng.choice([0, 10])

            counter += 1
            w.writerow([
                f"CX-{counter:07d}", customer_id(cust_idx), dt_str(ts),
                *scores, rng.choice(channels),
            ])

        # Second half: normal/baseline experience from arbitrary customers.
        for _ in range(N_CX - N_CX // 2):
            cust_idx = rng.randrange(N_CUSTOMERS)
            ts = random_datetime(rng)
            scores = list(scores_for_baseline_customer(rng))
            if ENABLE_NOISE and rng.random() < NOISE_RATES["cx_missing_metric"]:
                scores[rng.randrange(len(scores))] = ""
            elif ENABLE_NOISE and rng.random() < NOISE_RATES["cx_metric_edge_bias"]:
                idx = rng.randrange(len(scores))
                if idx in (0, 3, 4, 5): scores[idx] = rng.choice([1.0, 5.0])
                elif idx == 1: scores[idx] = rng.choice([0.0, 100.0])
                else: scores[idx] = rng.choice([0, 10])

            counter += 1
            w.writerow([
                f"CX-{counter:07d}", customer_id(cust_idx), dt_str(ts),
                *scores, rng.choice(channels),
            ])

    if counter != N_CX:
        raise RuntimeError(f"customer_experience count mismatch: {counter:,}")


# ---------------------------
# Validation
# ---------------------------

EXPECTED_COUNTS = {
    "areas.csv": N_AREAS,
    "towers.csv": N_TOWERS,
    "customers.csv": N_CUSTOMERS,
    "network_events.csv": N_NETWORK_EVENTS,
    "network_incidents.csv": N_NETWORK_INCIDENTS,
    "bridge_incident_customer.csv": N_BRIDGE_MAPPINGS,
    "complaints.csv": N_COMPLAINTS,
    "customer_experience.csv": N_CX,
    "plans.csv": len(PLANS),
    "services.csv": len(SERVICES),
    "incident_types.csv": len(INCIDENT_TYPES),
    "complaint_types.csv": len(COMPLAINT_TYPES),
}


def count_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as f:
        # Minus header.
        return max(0, sum(1 for _ in f) - 1)


def validate_output(out: Path, deep: bool = False):
    print("\nValidation")
    print("-" * 72)

    all_ok = True
    for filename, expected in EXPECTED_COUNTS.items():
        actual = count_data_rows(out / filename)
        ok = actual == expected
        all_ok &= ok
        print(f"{filename:34s} {actual:>12,} rows  expected {expected:>12,}  {'OK' if ok else 'FAIL'}")

    # Structural consistency that follows from deterministic design.
    checks = [
        ("50 areas divide 500 towers exactly", N_TOWERS % N_AREAS == 0),
        ("50 areas divide 100k customers exactly", N_CUSTOMERS % N_AREAS == 0),
        ("500 towers divide 10k incidents exactly", N_NETWORK_INCIDENTS % N_TOWERS == 0),
        ("500 towers divide 1m events exactly", N_NETWORK_EVENTS % N_TOWERS == 0),
        ("10k incidents divide 750k bridge mappings exactly", N_BRIDGE_MAPPINGS % N_NETWORK_INCIDENTS == 0),
        ("Affected/customer sample fits inside each area", AFFECTED_PER_INCIDENT <= CUSTOMERS_PER_AREA),
        (
            "Linked complaint counts sum correctly",
            sum(linked_complaint_count_for_incident(i) for i in range(N_NETWORK_INCIDENTS))
            == N_LINKED_COMPLAINTS,
        ),
    ]
    for label, ok in checks:
        all_ok &= ok
        print(f"{label:58s} {'OK' if ok else 'FAIL'}")

    if deep:
        print("\nDeep consistency checks (streaming CSV scan)")
        deep_ok = deep_validate(out)
        all_ok &= deep_ok

    if not all_ok:
        raise RuntimeError("Validation failed. Review the FAIL entries above.")

    print("\nAll requested counts and structural checks passed.")


def deep_validate(out: Path) -> bool:
    """
    Extra scan intended for final QA.
    It validates key referential/business rules without loading huge tables into RAM.
    """
    ok = True

    # Build small/medium lookup sets.
    area_ids = {area_id(i) for i in range(N_AREAS)}
    tower_to_area = {
        tower_id(i): area_id(i // TOWERS_PER_AREA)
        for i in range(N_TOWERS)
    }
    customer_to_area = {
        customer_id(i): area_id(area_for_customer_idx(i))
        for i in range(N_CUSTOMERS)
    }

    incident_lookup = {}
    with (out / "network_incidents.csv").open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            incident_lookup[row["incident_id"]] = (
                row["tower_id"],
                row["area_id"],
                int(row["affected_customer_count"]),
                int(row["complaint_count"]),
            )
            if tower_to_area[row["tower_id"]] != row["area_id"]:
                ok = False
                print("FAIL: incident area does not match tower area:", row["incident_id"])
                break

    # Bridge: uniqueness of composite key + same-area customers.
    # 750k tuples is acceptable for an optional deep check, but it is not used in generation.
    seen = set()
    bridge_counts = Counter()
    with (out / "bridge_incident_customer.csv").open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["incident_id"], row["customer_id"])
            if key in seen:
                ok = False
                print("FAIL: duplicate bridge composite key:", key)
                break
            seen.add(key)
            bridge_counts[row["incident_id"]] += 1
            if customer_to_area[row["customer_id"]] != incident_lookup[row["incident_id"]][1]:
                ok = False
                print("FAIL: affected customer area mismatch:", key)
                break

    if ok:
        for iid, (_, _, affected_count, _) in incident_lookup.items():
            if bridge_counts[iid] != affected_count:
                ok = False
                print("FAIL: affected_customer_count mismatch:", iid)
                break

    complaint_counts = Counter()
    with (out / "complaints.csv").open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            iid = row["incident_id"]
            if iid:
                complaint_counts[iid] += 1
                # A linked complaint must come from an actually affected customer.
                if (iid, row["customer_id"]) not in seen:
                    ok = False
                    print("FAIL: complaint customer is not in incident bridge:", iid, row["customer_id"])
                    break

    if ok:
        for iid, (_, _, _, expected_complaints) in incident_lookup.items():
            if complaint_counts[iid] != expected_complaints:
                ok = False
                print("FAIL: incident complaint_count mismatch:", iid)
                break

    print("Deep referential/business consistency:", "OK" if ok else "FAIL")
    return ok


# ---------------------------
# Main
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate consistent synthetic telecom CSV datasets."
    )
    parser.add_argument(
        "--output-dir",
        default="telecom_generated_data",
        help="Directory for generated CSV files (default: telecom_generated_data)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete an existing output directory first.",
    )
    parser.add_argument(
        "--deep-validate",
        action="store_true",
        help="After generation, scan large CSVs for deeper relationship checks.",
    )
    args = parser.parse_args()

    out = Path(args.output_dir).resolve()
    if out.exists():
        if not args.overwrite:
            print(f"Output directory already exists: {out}")
            print("Use --overwrite to replace it.")
            sys.exit(2)
        shutil.rmtree(out)

    out.mkdir(parents=True, exist_ok=True)

    print("Generating consistent telecom synthetic data")
    print(f"Output: {out}")
    print(f"Seed:   {SEED}")
    print(f"Noise:  {'ENABLED' if ENABLE_NOISE else 'DISABLED'}")
    print("-" * 72)

    stages = [
        ("Reference tables", generate_reference_tables),
        ("Noise manifest", write_noise_manifest),
        ("50 areas", generate_areas),
        ("500 towers", generate_towers),
        ("100,000 customers", generate_customers),
        ("10,000 network incidents", generate_network_incidents),
        ("750,000 incident/customer mappings", generate_bridge),
        ("75,000 complaints", generate_complaints),
        ("150,000 CX records", generate_customer_experience),
        ("1,000,000 network events", generate_network_events),
    ]

    started = time.time()
    for label, fn in stages:
        t0 = time.time()
        print(f"[START] {label}")
        fn(out)
        print(f"[DONE ] {label} ({time.time() - t0:.1f}s)")

    validate_output(out, deep=args.deep_validate)

    print("-" * 72)
    print(f"Finished in {time.time() - started:.1f}s")
    print(f"CSV files are in: {out}")


if __name__ == "__main__":
    main()

"""Shared Spark Silver paths. TELECOM_RUN_ID is supplied by the orchestrator."""
import os
import re
from pathlib import Path

DATA_ROOT = Path(os.environ.get("TELECOM_DATA_ROOT", "/opt/telecom/data"))
RUN_ID = os.environ.get("TELECOM_RUN_ID", "").strip()
if RUN_ID and not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", RUN_ID):
    raise ValueError("TELECOM_RUN_ID may contain only letters, digits, _ and -")

RAW = DATA_ROOT / "raw" / "batch"
OUTPUT_ROOT = DATA_ROOT / "processed" / "runs" / RUN_ID if RUN_ID else DATA_ROOT / "processed"
SILVER = OUTPUT_ROOT / "silver"
SHARED_SILVER = DATA_ROOT / "processed" / "silver"
QUARANTINE = OUTPUT_ROOT / "quarantine"
REPORTS = (OUTPUT_ROOT / "reports" / "silver") if RUN_ID else (DATA_ROOT / "reports" / "silver")

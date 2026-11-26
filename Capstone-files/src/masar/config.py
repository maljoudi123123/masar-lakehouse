"""Paths shared by every module. Everything lives under the repo root."""
from pathlib import Path

# CAPSTONE: modified. This copy lives in Capstone-files/src/masar/, one folder deeper than
# src/masar/, so the repo root is parents[3] (was parents[2]). data/ and dbt/ are read from there.
ROOT = Path(__file__).resolve().parents[3]
# CAPSTONE: added. The lakehouse (bronze, silver, gold, metastore) is built inside Capstone-files/,
# so the capstone's tables sit next to its code: Capstone-files/lakehouse/.
CAPSTONE_HOME = Path(__file__).resolve().parents[2]
LAKEHOUSE = CAPSTONE_HOME / "lakehouse"   # was ROOT / "lakehouse"
RAW = ROOT / "data" / "raw"
FIXTURES = ROOT / "data" / "fixtures"

BRONZE = str(LAKEHOUSE / "bronze")
SILVER = str(LAKEHOUSE / "silver" / "trips")
GOLD = str(LAKEHOUSE / "gold")
QUARANTINE = str(LAKEHOUSE / "quarantine" / "trips")
CHECKPOINTS = str(LAKEHOUSE / "_checkpoints")

CITIES = ["Riyadh", "Jeddah", "Dammam", "Mecca", "Medina"]
KAFKA_BOOTSTRAP = "127.0.0.1:9092"  # not localhost: resolves to IPv6 ::1 first, which Colima drops
PING_TOPIC = "masar.gps.pings"

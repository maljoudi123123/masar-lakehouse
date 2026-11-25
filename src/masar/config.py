"""Paths shared by every module. Everything lives under the repo root."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LAKEHOUSE = ROOT / "lakehouse"
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

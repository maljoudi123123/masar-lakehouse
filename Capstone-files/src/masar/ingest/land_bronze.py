import sys
from pathlib import Path

from pyspark.sql import functions as F

if __name__ == "__main__" and not __package__:
    # Direct script execution puts ingest/ on the import path. Add src/ for masar.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from masar.config import BRONZE, RAW  # CAPSTONE: added
from masar.spark import get_spark
# CAPSTONE: modified. Was BRONZE = "./lakehouse/bronze", which only works when the current
# folder is the repo root. config.BRONZE is the same folder as an absolute path.
def _lineage(df):
    return (df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.input_file_name()))
def land_trips(source_glob: str) -> None:
    spark = get_spark("land-trips")
    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")   # keep malformed rows, do not drop
           .csv(source_glob))
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/trips")
def land_gps(source_glob: str) -> None:
    spark = get_spark("land-gps")
    raw = spark.read.json(source_glob)   # schema-on-read for nested JSON
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/gps_pings")
def land_drivers(source_glob: str) -> None:
    spark = get_spark("land-drivers")
    raw = spark.read.option("header", True).csv(source_glob)
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/drivers")
# CAPSTONE: added. The __main__ body as a function, so the pipeline can call it directly.
# Same three sources as before, as absolute paths under config.RAW (was "data/raw/...").
def land_all() -> None:
    land_trips(f"{RAW}/trips_2026-*.csv")
    land_drivers(f"{RAW}/drivers.csv")
    land_gps(f"{RAW}/gps/*.ndjson")
    print("landed")


if __name__ == "__main__":
    land_all()  # CAPSTONE: modified, was the three land_* calls inline

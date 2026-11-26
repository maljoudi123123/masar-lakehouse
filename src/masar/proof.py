"""Capstone step 7: prove it. Row counts per layer and one time-travel query on silver (T5).

CAPSTONE: new file (SUBMIT.md asks for `python -m masar.proof`). Built from:
  - counts:       the count-per-table pattern used across the tutorials
  - time travel:  time_travel.py (history + versionAsOf on silver)
  - --erase:      merge_ops.erase_rider (Day 2 right-to-erasure), which creates the
                  newer silver version whose count differs from the older one

Usage (from the repo root):
  python -m masar.proof --counts-only   counts per layer (the pipeline's last step)
  python -m masar.proof                 counts + time travel
  python -m masar.proof --erase         erase the rider in data/fixtures/erasure_request.json first,
                                        then counts + time travel (silver 44,371 -> 44,364)
"""
import argparse
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from masar.config import BRONZE, FIXTURES, GOLD, SILVER
from masar.spark import get_spark

# (layer, table, path, expected rows from capstone/checks/expected_counts.md)
LAYERS = [
    ("bronze", "trips",              f"{BRONZE}/trips",              48_200),
    ("bronze", "drivers",            f"{BRONZE}/drivers",            1_850),
    ("bronze", "gps_pings",          f"{BRONZE}/gps_pings",          120_000),
    ("silver", "trips",              SILVER,                         44_371),
    ("gold",   "zone_hourly_demand", f"{GOLD}/zone_hourly_demand",   120),
    ("gold",   "trip_features",      f"{GOLD}/trip_features",        44_371),
    ("gold",   "training_set",       f"{GOLD}/training_set",         44_371),
    ("gold",   "bi_trips_safe",      f"{GOLD}/bi_trips_safe",        44_371),
]


SILVER_AFTER_ERASE = 44_364   # 44,371 minus the 7 trips of the erased rider


def counts(spark) -> None:
    print(f"{'layer':7} {'table':20} {'rows':>9}  {'expected':>9}")
    for layer, name, path, expected in LAYERS:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"{layer:7} {name:20} {'missing':>9}  {expected:>9,}")
            continue
        n = spark.read.format("delta").load(path).count()
        mark = "ok" if n == expected else "differs"
        # Silver is 44,364 after the erase (--erase), as the CAPSTONE.md trainer notes say.
        if (layer, name) == ("silver", "trips") and n == SILVER_AFTER_ERASE:
            mark = "ok (after erase)"
        print(f"{layer:7} {name:20} {n:>9,}  {expected:>9,}  {mark}")


def time_travel(spark) -> None:
    # From time_travel.history(): one line per silver version.
    hist = (DeltaTable.forPath(spark, SILVER).history()
            .select("version", "timestamp", "operation",
                    F.col("operationMetrics")["numOutputRows"].alias("written"),
                    F.col("operationMetrics")["numDeletedRows"].alias("deleted"))
            .orderBy("version"))
    hist.show(truncate=False)
    versions = [r.version for r in hist.collect()]
    latest = versions[-1]
    # From time_travel.rider_by_version(): read an older version with versionAsOf.
    by_version = {v: spark.read.format("delta").option("versionAsOf", v).load(SILVER).count()
                  for v in versions}
    oldest = versions[0]
    print(f"time travel: version {oldest} = {by_version[oldest]:,} rows, "
          f"latest version {latest} = {by_version[latest]:,} rows")
    if len(versions) < 2:
        print("CHECK FAILED: silver has only one version")
    elif by_version[oldest] == by_version[latest]:
        print("CHECK FAILED: older and latest counts are equal. Run: python -m masar.proof --erase")
    else:
        print(f"CHECK PASSED: {len(versions)} versions, older count differs from latest "
              f"({by_version[latest] - by_version[oldest]:+,} rows since version {oldest})")


def erase(spark) -> None:
    # CAPSTONE: same call as `python -m masar.delta.merge_ops erase`, reused instead of copied.
    from masar.delta.merge_ops import erase_rider
    rider_id = json.loads((FIXTURES / "erasure_request.json").read_text())["rider_id"]
    erase_rider(spark, rider_id)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--counts-only", action="store_true", help="skip time travel (pipeline step)")
    p.add_argument("--erase", action="store_true", help="erase the fixture rider first")
    a = p.parse_args()
    spark = get_spark("proof")
    try:
        if a.erase:
            print("== erase (right to erasure)")
            erase(spark)
        print("== counts per layer")
        counts(spark)
        if not a.counts_only:
            print("== time travel on silver/trips")
            time_travel(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

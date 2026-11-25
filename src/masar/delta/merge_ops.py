import json
import sys
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark


def _version(spark) -> int:
    return DeltaTable.forPath(spark, SILVER).history(1).select("version").first()[0]


def upsert_corrections(spark, path: str) -> None:
    src = (spark.read.option("header", True).csv(path).select("trip_id", F.col("fare_sar").cast("double").alias("fare_sar")))
    before = _version(spark)
    (DeltaTable.forPath(spark, SILVER).alias("t")
        .merge(src.alias("s"), "t.trip_id = s.trip_id")           # the key: one row per trip
        .whenMatchedUpdate(condition="t.fare_sar <> s.fare_sar",  # only real changes
                           set={"fare_sar": "s.fare_sar"})        # the CSV has 2 columns
        .execute())
    after = _version(spark)
    print("no changes: nothing committed" if after == before else f"MERGE committed as v{after}")

def erase_rider(spark, rider_id: str) -> None:
    silver = DeltaTable.forPath(spark, SILVER)
    before = spark.read.format("delta").load(SILVER).count()
    silver.delete(F.col("rider_id") == rider_id)
    m = (silver.history(1).select("version", F.col("operationMetrics")["numDeletedRows"].alias("deleted")).first())
    after = spark.read.format("delta").load(SILVER).count()
    print(f"erased {rider_id}: v{m['version']} deleted={m['deleted']}  rows {before} -> {after}")

if __name__ == "__main__":
    spark = get_spark("merge-ops")
    if sys.argv[1] == "corrections":
        upsert_corrections(spark, "data/fixtures/corrections.csv")
    elif sys.argv[1] == "erase":
        rid = json.load(open("data/fixtures/erasure_request.json"))["rider_id"]
        erase_rider(spark, rid)

# run twice: python -m masar.delta.merge_ops corrections 2>&1 | grep -v WARN
# 1st: MERGE committed as v3 (5 fares updated)    2nd: no changes: nothing committed

import sys
from pathlib import Path

from pyspark.sql import functions as F

if __name__ == "__main__" and not __package__:
    # Direct script execution puts ingest/ on the import path. Add src/ for masar.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from masar.spark import get_spark


def profile(table_path: str) -> None:
    spark = get_spark("profile")
    df = spark.read.format("delta").load(table_path)
    n = df.count()
    print(f"rows={n:,}  columns={len(df.columns)}")
    df.select([
        (F.sum(F.col(c).isNull().cast("int")) / F.lit(n)).alias(c)
        for c in df.columns
    ]).show(truncate=False, vertical=True)
    if "fare_sar" in df.columns:
        number = r"^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*$"
        fare = F.col("fare_sar")
        invalid_fares = df.filter(fare.isNull() | ~fare.cast("string").rlike(number))
        invalid_count = invalid_fares.count()
        if invalid_count:
            print(f"WARNING: {invalid_count:,} rows have a missing or non-numeric fare_sar")
            columns = ["trip_id", "fare_sar"] if "trip_id" in df.columns else ["fare_sar"]
            invalid_fares.select(*columns).show(20, truncate=False)
        else:
            print("Invalid fare_sar rows: 0")
    if "trip_id" in df.columns:
        d = df.groupBy("trip_id").count().filter("count > 1").count()
        print(f"duplicate trip_id groups: {d:,}")
if __name__ == "__main__":
    profile("./lakehouse/bronze/trips")

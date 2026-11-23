"""L1: the same Masar trips as CSV, Parquet and Delta.
Run after Tutorial-1-style bronze exists (lakehouse/bronze/trips).
Copy this file to src/masar/econ/formats_lab.py, then:
    python -m masar.econ.formats_lab 2>&1 | grep -v WARN
It writes three copies under lakehouse/scratch/formats, then compares them:
size on disk, time for one narrow query, and what happens on a bad append.
Clean up afterwards:  rm -rf lakehouse/scratch/formats
"""
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

if __name__ == "__main__" and not __package__:
    # Direct execution puts econ/ on the import path. Add src/ for masar.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from masar.config import BRONZE, LAKEHOUSE
from masar.spark import get_spark


def dir_mb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6


def best_time(fn, runs: int = 3) -> float:
    """Fastest of a few runs, so a cold start does not decide the result."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times)


def bad_append(df, writer) -> str:
    """Try to append one row whose fare_sar is text. Does the format notice?"""
    bad = df.limit(1).withColumn("fare_sar", F.lit("not-a-number"))
    try:
        writer(bad)
        return "accepted silently"
    except Exception as e:  # noqa: BLE001  (any rejection counts)
        return f"REJECTED ({type(e).__name__})"


def main() -> None:
    s = get_spark("formats-lab")
    # Bronze keeps raw text; give fare_sar a real numeric type so a bad append is a real test.
    src = (s.read.format("delta").load(f"{BRONZE}/trips")
           .withColumn("fare_sar", F.col("fare_sar").cast("double")))
    out = LAKEHOUSE / "scratch" / "formats"
    shutil.rmtree(out, ignore_errors=True)
    src.write.option("header", True).csv(str(out / "csv"))
    src.write.parquet(str(out / "parquet"))
    src.write.format("delta").save(str(out / "delta"))
    readers = {
        "csv": lambda: s.read.option("header", True).option("inferSchema", True).csv(str(out / "csv")),
        "parquet": lambda: s.read.parquet(str(out / "parquet")),
        "delta": lambda: s.read.format("delta").load(str(out / "delta")),
    }
    query = lambda r: r().groupBy("city").agg(F.avg("fare_sar")).collect()  # noqa: E731
    writers = {
        "csv": lambda d: d.write.mode("append").option("header", True).csv(str(out / "csv")),
        "parquet": lambda d: d.write.mode("append").parquet(str(out / "parquet")),
        "delta": lambda d: d.write.mode("append").format("delta").save(str(out / "delta")),
    }
    rows = {k: readers[k]().count() for k in readers}
    print(f"\nrows written to each copy: {rows}\n")
    print(f"{'format':<8} {'size MB':>8} {'avg fare by city (s)':>22}  {'transaction log':<16} bad append")
    for k in ("csv", "parquet", "delta"):
        size = dir_mb(out / k)
        secs = best_time(lambda k=k: query(readers[k]))
        has_log = "yes" if (out / k / "_delta_log").exists() else "no"
        verdict = bad_append(src, writers[k])
        print(f"{k:<8} {size:>8.1f} {secs:>22.2f}  {has_log:<16} {verdict}")
    print("\nclean up:  rm -rf lakehouse/scratch/formats")


if __name__ == "__main__":
    main()

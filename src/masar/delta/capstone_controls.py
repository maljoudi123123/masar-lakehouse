"""Capstone step 3: one Delta constraint and one schema change on silver.trips (T4).

CAPSTONE: new file, assembled from the Day 2 tutorials:
  1. constraint    <- enforce_silver.py  (idempotent ALTER TABLE ... ADD CONSTRAINT)
  2. bad insert    <- break_it.py        (dirty_batch_bad_fares, cast to the silver schema)
  3. schema change <- evolve_demo.py     (an unplanned new column is rejected, then added safely)

Run after silver exists (after `make pipeline`):  python -m masar.delta.capstone_controls
Safe to re-run: the constraint and the column are only added when missing.
"""
import pandas as pd
from pyspark.sql import functions as F

from masar.config import FIXTURES, SILVER
from masar.spark import get_spark

CONSTRAINT = "positive_fare"      # CAPSTONE: the one constraint for step 3
NEW_COLUMN = "promo_code"         # CAPSTONE: the one schema change for step 3 (from evolve_demo.py)


def count(spark) -> int:
    return spark.read.format("delta").load(SILVER).count()


def add_constraint(spark) -> None:
    # Copied from enforce_silver.py: check TBLPROPERTIES first so the script stays re-runnable.
    props = {r[0] for r in spark.sql("SHOW TBLPROPERTIES silver.trips").collect()}
    if f"delta.constraints.{CONSTRAINT}" not in props:
        # CAPSTONE: modified. positive_fare instead of driver_id_not_null, so the
        # dirty_batch_bad_fares fixture from break_it.py is what proves it.
        spark.sql(f"ALTER TABLE silver.trips ADD CONSTRAINT {CONSTRAINT} CHECK (fare_sar > 0)")
        print(f"added constraint {CONSTRAINT}: CHECK (fare_sar > 0)")
    else:
        print(f"constraint {CONSTRAINT} already present")


def try_bad_insert(spark) -> None:
    # Copied from break_it.py: cast the fixture to the silver types so only the fares are wrong.
    before = count(spark)
    bad = spark.createDataFrame(pd.read_parquet(FIXTURES / "dirty_batch_bad_fares.parquet"))
    for f in spark.read.format("delta").load(SILVER).schema.fields:
        if f.name in bad.columns:
            bad = bad.withColumn(f.name, F.col(f.name).cast(f.dataType))
    spark.sparkContext.setLogLevel("OFF")   # CAPSTONE: hide the expected per-task ERROR logs
    try:
        bad.write.format("delta").mode("append").save(SILVER)
        raise SystemExit("UNEXPECTED: bad insert succeeded")   # CAPSTONE: fail loudly
    except SystemExit:
        raise
    except Exception as e:
        # CAPSTONE: modified. Show the Delta reason (not the Py4J wrapper) and prove that it was
        # the constraint that blocked the batch, not some other error.
        reason = next((ln for ln in str(e).splitlines() if "CHECK constraint" in ln), str(e).splitlines()[0])
        assert CONSTRAINT in reason, f"rejected, but not by {CONSTRAINT}: {reason}"
        print("bad insert REJECTED:", reason[reason.find("[DELTA"):].strip()[:150])
    finally:
        spark.sparkContext.setLogLevel("WARN")
    after = count(spark)
    assert after == before, f"row count changed {before} -> {after}"   # CAPSTONE: added
    print(f"rows before={before:,} after={after:,} (unchanged)")


def change_schema(spark) -> None:
    if NEW_COLUMN in spark.read.format("delta").load(SILVER).columns:
        print(f"column {NEW_COLUMN} already present")
        return
    # From evolve_demo.py: a write carrying an unplanned column is rejected by schema enforcement.
    # CAPSTONE: modified. limit(0) so the attempt can never add rows to silver, even if it passed.
    new = spark.read.format("delta").load(SILVER).limit(0).withColumn(NEW_COLUMN, F.lit("RAMADAN10"))
    try:
        new.write.format("delta").mode("append").save(SILVER)
        print("UNEXPECTED: write with an unknown column succeeded")
    except Exception as e:
        print("unplanned column REJECTED:", type(e).__name__, str(e).splitlines()[0][:120])
    # CAPSTONE: modified. evolve_demo.py used mergeSchema on a scratch copy. On silver itself
    # the safe change is an explicit, metadata-only ALTER: no files are rewritten, existing
    # rows read NULL, and the dbt model keeps working (on_schema_change='append_new_columns').
    spark.sql(f"ALTER TABLE silver.trips ADD COLUMNS ({NEW_COLUMN} STRING COMMENT 'capstone schema change')")
    print(f"added column {NEW_COLUMN} STRING (existing rows read NULL)")


def main() -> None:
    spark = get_spark("capstone-controls")
    try:
        print("== 1. constraint")
        add_constraint(spark)
        print("== 2. bad insert")
        try_bad_insert(spark)
        print("== 3. schema change")
        change_schema(spark)
        df = spark.read.format("delta").load(SILVER)
        assert NEW_COLUMN in df.columns
        print(f"new column visible: {NEW_COLUMN} in silver.trips "
              f"({df.filter(F.col(NEW_COLUMN).isNull()).count():,} NULL of {df.count():,})")
        spark.sql("SHOW TBLPROPERTIES silver.trips").filter("key LIKE 'delta.constraints%'").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

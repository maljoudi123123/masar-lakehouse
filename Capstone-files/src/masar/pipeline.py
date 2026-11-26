"""Capstone step 6: run the whole pipeline with `make pipeline`.

Fill in each TODO using what you built in the tutorials. Stop on the first failure.

CAPSTONE: every step is a function imported from this package (Capstone-files/src/masar)
and called directly in ONE Python process that shares ONE Spark session (the capstone rule
"one Spark session at a time"). dbt is called through its Python API (dbtRunner), in the
same process. Raw data and the dbt project are read from the repo root (config.ROOT); the
lakehouse (bronze, silver, gold) is built in Capstone-files/lakehouse (config.LAKEHOUSE).

Run from the repo root:  PYTHONPATH=Capstone-files/src python -m masar.pipeline

Start fresh with `rm -rf Capstone-files/lakehouse` (`make clean-lakehouse` empties the repo's
lakehouse/, not this one). Re-running without cleaning is safe: bronze is skipped once landed,
silver is an incremental MERGE on trip_id, gold and consumer tables overwrite.
"""
import os
import sys

from masar.config import LAKEHOUSE, ROOT

# dbt's profiles.yml and dbt_project.yml put silver and the metastore under $MASAR_HOME/lakehouse.
# CAPSTONE: modified. Point it at the folder that holds config.LAKEHOUSE (Capstone-files/), and
# override (not setdefault): the Makefile exports MASAR_HOME=<repo root>, which would send
# silver to the repo's lakehouse/ instead.
os.environ["MASAR_HOME"] = str(LAKEHOUSE.parent)

from dbt.cli.main import dbtRunner  # noqa: E402  (after MASAR_HOME is set)

from masar.ingest.land_bronze import land_all  # noqa: E402
from masar.proof import counts  # noqa: E402
from masar.quality.check_silver import quality_check  # noqa: E402
from masar.register import register_bronze  # noqa: E402
from masar.serve.bi_view import build_bi_safe_view  # noqa: E402
from masar.serve.training_join import save_training_set  # noqa: E402
from masar.spark import get_spark  # noqa: E402
from masar.transform.build_gold import build_trip_features, build_zone_hourly_demand  # noqa: E402

DBT_DIR = ROOT / "dbt" / "masar"


# CAPSTONE: replaces run(cmd). One named step = one function call; stop on the first failure.
def run(name, fn):
    print(f"\n=== {name}", flush=True)
    try:
        fn()
    except Exception as e:
        print(f"PIPELINE HALTED at {name}: {type(e).__name__}: {e}")
        sys.exit(1)
    print(f"[{name}] OK", flush=True)


# TODO 1: land bronze (masar.ingest.land_bronze)
def land_bronze():
    # Bronze appends, so landing twice would duplicate rows: skip when already landed (Day 4 land_bronze()).
    # LAKEHOUSE comes from config.py: Capstone-files/lakehouse (not the repo's lakehouse/).
    if (LAKEHOUSE / "bronze" / "trips" / "_delta_log").exists():
        print("bronze already landed: skipping")
        return
    land_all()


# TODO 3: dbt build for silver (cd dbt/masar && dbt build)
def dbt(*args):
    # In-process equivalent of `cd dbt/masar && dbt <args>`. dbt-spark's "session" method
    # reuses the Spark session this pipeline already opened (same metastore, same lakehouse).
    res = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    if not res.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {res.exception or 'see dbt output above'}")


def build_silver():
    if not (DBT_DIR / "dbt_packages" / "dbt_utils").exists():
        dbt("deps")
    # `+silver_trips` = stg_trips, stg_drivers and silver_trips, plus their tests (unique, not_null,
    # accepted_range, accepted_values, relationships). Other exercise models are not selected.
    dbt("build", "--select", "+silver_trips")


# TODO 4: build one gold mart
def build_gold():
    # zone_hourly_demand (the mart) and trip_features (needed by the training set in TODO 5).
    build_zone_hourly_demand()
    build_trip_features()


# TODO 6: print row counts per layer
def print_counts():
    counts(get_spark("proof"))


def main():
    get_spark("masar-pipeline")  # open the one shared Spark session up front
    run("land_bronze", land_bronze)                      # TODO 1
    run("register_bronze", register_bronze)              # TODO 2
    run("build_silver (dbt)", build_silver)              # TODO 3
    run("quality_gate", quality_check)                   # quality gate on silver (T7)
    run("gold", build_gold)                              # TODO 4
    # TODO 5: build the consumer table (BI-safe view or training set)
    # Both are built; each checks itself (no PII columns / no leaky features) and fails if not.
    run("bi_safe_view", build_bi_safe_view)
    run("training_set", save_training_set)
    run("counts", print_counts)                          # TODO 6
    print("\nPIPELINE OK")


if __name__ == "__main__":
    main()

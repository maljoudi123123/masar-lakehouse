"""Capstone step 6: run the whole pipeline with `make pipeline`.

Fill in each TODO using what you built in the tutorials. Stop on the first failure.

CAPSTONE: every step runs as its own subprocess (one Spark session at a time, as the
capstone rules ask), from the repo root, with the same PYTHONPATH / MASAR_HOME the
Makefile exports. Re-running without `make clean-lakehouse` is safe: bronze is skipped
once landed, silver is an incremental MERGE on trip_id, gold and consumer tables overwrite.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

# CAPSTONE: added. Paths and environment shared by every step (copied from the Day 4 _sh helper).
ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = ROOT / "dbt" / "masar"
PY = sys.executable
# dbt sits next to python in the venv; fall back to PATH (Codespace with .venv activated).
DBT = str(Path(PY).with_name("dbt")) if Path(PY).with_name("dbt").exists() else (shutil.which("dbt") or "dbt")
ENV = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "MASAR_HOME": str(ROOT)}


def run(cmd, cwd=ROOT):
    print("+", " ".join(cmd), flush=True)
    # CAPSTONE: modified. Pass cwd and env so every step sees the repo root and src/.
    if subprocess.run(cmd, cwd=cwd, env=ENV).returncode != 0:
        print(f"PIPELINE HALTED at: {' '.join(cmd)}")
        sys.exit(1)


def main():
    # TODO 1: land bronze (masar.ingest.land_bronze)
    # Bronze appends, so landing twice would duplicate rows: skip when already landed (Day 4 land_bronze()).
    if (ROOT / "lakehouse" / "bronze" / "trips" / "_delta_log").exists():
        print("+ bronze already landed: skipping")
    else:
        run([PY, "-m", "masar.ingest.land_bronze"])

    # TODO 2: register bronze tables (masar.register)
    run([PY, "-m", "masar.register"])

    # TODO 3: dbt build for silver (cd dbt/masar && dbt build)
    # `+silver_trips` = stg_trips, stg_drivers and silver_trips, plus their tests (unique, not_null,
    # accepted_range, accepted_values, relationships). Other exercise models are not selected.
    if not (DBT_DIR / "dbt_packages" / "dbt_utils").exists():
        run([DBT, "deps", "--profiles-dir", "."], cwd=DBT_DIR)
    run([DBT, "build", "--select", "+silver_trips", "--profiles-dir", "."], cwd=DBT_DIR)
    # Quality gate (T7): contract checks on silver; integrity failures fail fast.
    run([PY, "-m", "masar.quality.check_silver"])

    # TODO 4: build one gold mart
    # zone_hourly_demand (the mart) and trip_features (needed by the training set in TODO 5).
    run([PY, "-m", "masar.transform.build_gold"])

    # TODO 5: build the consumer table (BI-safe view or training set)
    # Both are built; each checks itself (no PII columns / no leaky features) and fails if not.
    run([PY, "-m", "masar.serve.bi_view"])
    run([PY, "-m", "masar.serve.training_join"])

    # TODO 6: print row counts per layer
    run([PY, "-m", "masar.proof", "--counts-only"])
    print("PIPELINE OK")


if __name__ == "__main__":
    main()

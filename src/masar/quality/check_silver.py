"""Capstone step 2: the quality gate on silver.

CAPSTONE: new file. quality_check() is copied literally from the Day 4 src/masar/pipeline.py
(T7). It is a module of its own so the capstone pipeline can run it as one step and stop
when it fails (contract.py on its own only prints the results).

Run:  python -m masar.quality.check_silver
"""
import sys

from masar.quality.contract import INTEGRITY, load, validate


def quality_check():
    results = validate(load("silver"))
    for name, r in results.items():
        print(f"  {name:18} passed={r['passed']!s:5} bad_rows={r['bad_rows']}")
    broken = [n for n in INTEGRITY if not results[n]["passed"]]
    if broken:
        raise RuntimeError(f"FAIL-FAST: integrity checks failed {broken}")
    failed = [n for n, r in results.items() if not r["passed"]]
    if failed:
        raise RuntimeError(f"quality checks failed: {failed}")


if __name__ == "__main__":
    # CAPSTONE: added. Non-zero exit so the pipeline halts on a failed gate.
    try:
        quality_check()
    except RuntimeError as e:
        print(e)
        sys.exit(1)
    print("quality gate passed")

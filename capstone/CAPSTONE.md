# Capstone: Assemble Your Mini-Lakehouse (SDA-DSC-214, Day 5)

Time: 200 minutes, including a break and a pair review (the Day 5 survey follows). Work in pairs, one repo per trainee.
Covers LO7 (build a working Mini-Lakehouse), with LO3, LO4, LO6, LO8 touched.

Rule: every step is a variation of a tutorial you already ran. Nothing here is new.
Expected counts come from a verified Codespace run (see capstone/checks/expected_counts.md).

## 0. Start clean

Stay in your own Codespace and on your own branch (do not switch branches, so your Day 1-5 files stay with you).

**A. Save your work first** (the next step replaces your Day 4 `src/masar/pipeline.py`; the old one stays safe in this commit):

```bash
git add -A && git commit -m "Day 1-5 work"
git push -u origin HEAD
```

**B. Bring in the capstone files.** Your trainer hands you `Capstone-files.zip`: upload it to the repo root and unzip it there (`unzip Capstone-files.zip`). No zip? Fetch the same files from the repo:

```bash
git fetch https://github.com/altherwy/masar-lakehouse.git capstone && git checkout FETCH_HEAD -- capstone src/masar/pipeline.py
```

Either way you now have `capstone/` (this file, `SUBMIT.md`, `checks/`) and the empty `src/masar/pipeline.py` skeleton. Step 6 is yours to write.

**C. Empty the lakehouse** so step 1 lands bronze exactly once and your counts match the checklist (`data/` is kept). Do not run this before step A:

```bash
make clean-lakehouse
```

Broken Codespace or joined late? Open a new Codespace on the `capstone` branch, run `make setup`, `source .venv/bin/activate`, `make doctor` and `make data`, then copy the Day 1-5 files again from the day folders your trainer shared.

## 1. Starter layout (already in this repo)

You build inside the existing packages. Nothing new to install.

```
src/masar/ingest/land_bronze.py     step 1 (T1)
dbt/masar/models/marts/silver/      step 2 (T3, T7)
src/masar/delta/                    step 3 (T4)
src/masar/serve/                    steps 4-5 (T8, T9)
src/masar/pipeline.py               step 6: TODO skeleton, run with: make pipeline
capstone/checks/expected_counts.md  trainer's expected outputs per step
```

Fallback: if you are stuck on a step for more than 10 minutes, ask your trainer. The trainer can
hand you the working files for that ONE step. Mark it "borrowed" in your README and move on.

## 2. Trainee checklist

Run each step, then compare the output with the checkpoint. Tick it only when it matches.

| # | Step | Based on | Checkpoint |
|---|------|----------|------------|
| 1 | Land bronze | T1 | bronze/trips = 48,200 rows (drivers 1,850; gps_pings 120,000) |
| 2 | Build silver with dbt and the quality gate | T3, T7 | silver/trips = 44,371 rows; dbt tests pass; quality checks all bad_rows=0 |
| 3 | Add one Delta constraint and one schema change | T4 | Bad insert is rejected; new column visible |
| 4 | Build one gold mart | T8 | zone_hourly_demand = 120 rows (or trip_features = 44,371) |
| 5 | Build a consumer table (BI-safe view OR training set) | T9 | No PII columns; leakage check passes |
| 6 | Run everything with one command | T8 | make pipeline exits 0 from a clean state |
| 7 | Prove it: counts per layer and one time-travel query | T5 | silver/trips has more than one version; an older version's count differs from the latest |
| 8 | Write the README and submit | SUBMIT.md | capstone/SUBMIT.md filled, branch pushed to your fork (see SUBMIT.md) |

Stretch (optional): second gold mart; quarantine table for null/duplicate trip_id.

Reminders: one Spark session at a time. If a step hangs, close other terminals running Spark.

## 3. Grading rubric (100 points)

| Area | Points | Full marks when |
|------|--------|-----------------|
| Pipeline runs end to end | 25 | make pipeline succeeds from clean state |
| Bronze to silver correctness | 15 | Counts match checkpoints; quality gate present |
| Delta controls | 15 | Constraint blocks bad data; schema change applied safely |
| Gold and consumer table | 15 | Mart correct; consumer table has no PII or leakage |
| Proof (counts, time travel) | 10 | Counts per layer shown; time-travel query correct |
| README and explanation | 20 | Trainee explains each layer and one design choice in own words |
| Borrowed steps | -5 each | Deduct per step the trainer handed over, max -15 |
| Stretch | +5 | Any one stretch item working, only if score is already 80 or more |

Pass mark: 70. Distinction: 90. Completing steps 1-6 correctly earns about 70 (the first four
rows above). Stretch bonus (+5) counts only if the score without it is at least 80, capped at 100.

## 4. Trainer timing (200 minutes)

| Minutes | Activity |
|---------|----------|
| 0-10 | Brief: goal, checklist, rules |
| 10-100 | Build steps 1-4; checkpoints after steps 2 and 4 |
| 100-110 | Break |
| 110-160 | Build steps 5-7; checkpoint after step 6 |
| 160-185 | README, commit, submit |
| 185-200 | Pair review: explain your pipeline to your partner |

## 5. Trainer notes

- Known open items (do NOT expect trainees to fix; mention only as discussion):
  erased trips returning after a bronze re-merge; quarantine write not idempotent.
- Counts were taken from a verified Codespace after `make data` (seed 42) and
  `make pipeline`. Silver = 44,371. It is 44,364 only if the Day 2 erase ran and the pipeline
  has not yet re-merged the erased trips (see DAY3.md).
- Delta version numbers depend on how many times a table was rebuilt, so the checklist
  uses relative checks (older vs latest), not fixed version numbers.

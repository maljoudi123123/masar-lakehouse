# Masar Mini-Lakehouse

A small lakehouse built as the project of the course **SDA-DSC-214: Modern Data Engineering for AI Systems** at SDAIA Academy.

- **Author:** Mohammed Aljoudi
- **Date:** November 26, 2026
- **Official SDAIA Academy GitHub:** [github.com/SDAIAAcademy](https://github.com/SDAIAAcademy)
- **Course starting repository:** [altherwy/masar-lakehouse](https://github.com/altherwy/masar-lakehouse)

## What this project is

Masar is a made-up ride-hailing company in Saudi Arabia. The data is synthetic: trips, drivers and GPS pings for five cities. This project turns that raw data into trusted tables that serve both dashboards (BI) and machine learning (AI), following the lakehouse pattern: open file formats, a transaction log for reliability, and three layers of increasing quality.

## Architecture

```
 CSV / JSON files ----\
                       +--> BRONZE (raw, kept as it arrived, Delta)
 Kafka GPS stream ----/            |
                                   v
                     dbt model + checks --> SILVER (clean, enforced, Delta)
                                   |
                  quality contract --+--> GOLD (marts and features)
                                                |
                                +---------------+---------------+
                                v                               v
                       BI-safe table                     Training set
                  (pseudonymised, no raw ids)         (point-in-time features)
```

## What I built

| Day | Topic | What is in the repository |
|---|---|---|
| 1 | Bronze, file formats and cost | Landing script, profiling, a formats lab (the same data in three formats), a scan-cost comparison and a platform cost model |
| 2 | Silver with dbt, enforced tables | dbt source, staging views, incremental silver model and tests; Delta constraints, a deliberately rejected write, the transaction log, schema evolution |
| 3 | Reliable changes and streaming | MERGE for corrections, a PDPL rider erasure, time travel, small files with OPTIMIZE and VACUUM, Kafka to Delta streaming with restart and windowed counts |
| 4 | Trust and one command | Data contract, quality gate, observability (freshness, volume, drift), PII retention, gold marts, one-command pipeline with retries and a failure drill |
| 5 | Serving and capstone | Training set, BI-safe table, leakage drill; then the capstone: the whole lakehouse rebuilt from a clean state with one command, plus a time-travel proof |

## Technology

- Python 3.11 and Java 17
- Apache Spark 3.5.1 and Delta Lake 3.2.0
- dbt 1.8 with the Spark adapter
- Apache Kafka 3.7.0 (single broker, run with Docker)
- Great Expectations 0.18.19
- GitHub Codespaces as the development environment

## Project structure

```
src/masar/
  ingest/       landing raw files into bronze
  econ/         formats lab, scan cost and cost model
  delta/        constraints, MERGE, time travel, small files
  stream/       Kafka producer and streaming ingest
  quality/      data contract, gate, observability, PII retention
  transform/    gold tables
  serve/        training set and BI-safe table
  pipeline.py   the one-command pipeline
dbt/masar/      dbt staging and silver models and tests
capstone/       capstone brief, submission notes and expected counts
Makefile        shortcuts such as make data and make pipeline
```

## How to run it

The data is not stored in this repository. You generate it with the steps below. Run each command from the repository root and wait for it to finish: only one Spark session can run at a time.

1. Open the repository in a GitHub Codespace (**Code** button, then **Codespaces**, then **Create**). Wait until the setup finishes.
2. Check the environment:
   ```bash
   make doctor
   ```
3. Generate the dataset:
   ```bash
   make data
   ```
4. Land the raw files into bronze (run this once only):
   ```bash
   python -m masar.ingest.land_bronze
   ```
5. Register the bronze tables and install dbt packages:
   ```bash
   make register
   make dbt-deps
   ```
6. Build and test the silver table, then add the Delta constraints:
   ```bash
   cd dbt/masar && dbt build --select +silver_trips && cd ../..
   python -m masar.delta.enforce_silver
   ```
7. Run the whole pipeline (silver, quality check, gold, maintenance):
   ```bash
   make pipeline
   ```
8. Serve the two consumers:
   ```bash
   python -m masar.serve.training_join
   python -m masar.serve.bi_view
   ```

Optional parts, each run on its own:

- **Day 3 streaming** needs Kafka: start it with `make kafka-up`, then follow the streaming files in `src/masar/stream/`. Stop it with `make kafka-down` when you are done.
- **Rebuild everything from scratch:** `make clean-lakehouse`, then run the steps above again from step 4. This deletes `lakehouse/` and keeps `data/`.

## Capstone

The capstone rebuilds the lakehouse from an empty `lakehouse/` folder and proves it. The brief is in [`capstone/CAPSTONE.md`](capstone/CAPSTONE.md) and the counts to compare against are in [`capstone/checks/expected_counts.md`](capstone/checks/expected_counts.md). In short:

1. Land bronze and register it.
2. Build silver with dbt and pass the quality gate.
3. Add one Delta constraint and one schema change.
4. Build a gold mart.
5. Build a consumer table (BI-safe table or training set).
6. Run everything with `make pipeline` from a clean state.
7. Show the counts per layer and one time-travel query on silver.
8. Fill in [`capstone/SUBMIT.md`](capstone/SUBMIT.md) and submit.

## Results

- **Bronze:** 48,200 trips, 1,850 drivers, 120,000 GPS pings
- **Silver:** about 44,371 trips (44,364 if you also ran the Day 3 erase of one rider and have not yet re-run the pipeline)
- **Data contract on silver:** 7 of 7 rules pass
- **Gold demand table:** 120 rows (5 cities x 24 hours); gold features: one row per trip
- **Pipeline:** runs twice with identical counts, and recovers after an injected failure
- **BI table:** no raw rider or driver ids; the same number of pseudonyms as riders
- **Time travel:** an older version of silver has a different row count from the latest one

## Limits of this project

This is a teaching lakehouse on one machine with synthetic data. It is not production-ready: it has no real catalog or access control, no scheduler or alerting, and the streaming part is not connected to the main pipeline.

## Acknowledgements

Built during SDA-DSC-214 at SDAIA Academy, starting from the course repository above.

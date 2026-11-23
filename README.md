# Masar Lakehouse: SDA-DSC-214 workshop starter

Modern Data Engineering for AI Systems, 5 days x 5 hours. You build a small but complete
Lakehouse for **Masar**, a (fictional) Saudi ride-hailing platform: bronze, silver and gold
Delta tables, a dbt silver model, Kafka streaming, quality gates and serving for AI and BI.

This repo is the **starting point (before Day 1)**. Fork it, clone your fork, and follow your trainer.

## Quick start

Requirements: Python 3.10-3.12, Java 17, git. Docker is needed from Day 3.

On macOS with Homebrew, install Java 17 once with `brew install openjdk@17`.
Select it in the shell where you run the workshop commands:

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

```bash
git clone <your-fork-url> masar-lakehouse && cd masar-lakehouse
make setup                      # creates .venv and installs pinned packages
source .venv/bin/activate
make doctor                     # every line should show a tick
make data                       # generates the dataset into data/raw and data/fixtures
```

`make doctor` must be all ticks before Day 1. The first Spark run downloads the Delta
(and later Kafka) jars once, so you need internet access for it.

## What is here

```
Makefile                 make doctor | data | register | kafka-up | pipeline | clean-lakehouse
scripts/make_data.py     seeded data generator: same seed, same data for everyone
scripts/doctor.py        checks your toolchain
src/masar/spark.py       get_spark(): Spark + Delta (and Kafka) session, ready to use
src/masar/config.py      shared paths and constants
src/masar/register.py    registers bronze tables so dbt can read them
src/masar/<package>/     empty packages you fill in during the tutorials
dbt/masar/               dbt project shell (profile, packages); you write the models
docker-compose.yml       single-broker Kafka for Day 3
tests/                   tests for the data generator
```

Generated data (git-ignored):

| File | What |
|---|---|
| `data/raw/trips_2026-06-01.csv` | 48,200 trips, 12 columns |
| `data/raw/drivers.csv` | 1,850 drivers |
| `data/raw/gps/sample.ndjson` | 120,000 GPS pings, in arrival order (a few arrive late) |
| `data/fixtures/` | dirty batches, a late trip, fare corrections, an erasure request |

## Rules of the workshop

- Tutorials **add** files. Your trainer shares each day's `scripts/` folder; copy every file to the path your day's commands sheet shows. Run commands from the repo root unless a slide says otherwise.
- If you fall behind, ask your trainer for help; do not skip a step, later steps depend on it.
- Everything you build lives under `lakehouse/`. Start over any time with `make clean-lakehouse`.
- Do not commit `lakehouse/` or generated data (already git-ignored).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make doctor` shows a cross for Java | Install Java 17 and make sure `java -version` prints 17 |
| First `get_spark()` is slow or fails | It downloads jars from Maven Central; check your network or proxy |
| `ModuleNotFoundError: masar` | Run commands through `make` or set `PYTHONPATH=src` |
| Port 9092 already in use | Stop the other Kafka, or `make kafka-down` |

# SDA-DSC-214 workshop helpers.  Usage: make <target>
PY  := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
DBT := $(if $(wildcard .venv/bin/dbt),$(CURDIR)/.venv/bin/dbt,dbt)
SEED ?= 42
ROWS ?= 100
ERROR_RATE ?= 0.1
export PYTHONPATH := $(CURDIR)/src
export MASAR_HOME := $(CURDIR)

.PHONY: help setup doctor data clean-data clean-lakehouse test kafka-up kafka-down kafka-topic \
        register dbt-deps dbt-debug pipeline user-sign-ins

help:
	@echo "make setup           create .venv and install pinned packages"
	@echo "make doctor          check Java, Python and every package"
	@echo "make data            generate the Masar dataset (seeded)"
	@echo "make user-sign-ins   append ROWS new sign-ins, land, and profile (SEED=42 ROWS=100 ERROR_RATE=0.1)"
	@echo "make register        register bronze Delta tables so dbt can read them (Day 2)"
	@echo "make dbt-deps|dbt-debug   dbt helpers (Day 2)"
	@echo "make kafka-up|kafka-topic|kafka-down   single-broker Kafka (Day 3)"
	@echo "make pipeline        run the whole pipeline (you write it on Day 4)"
	@echo "make clean-lakehouse delete everything you built under lakehouse/"
	@echo "make test            run the data-generator tests"

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@echo "Now run:  source .venv/bin/activate && make doctor"

doctor:
	$(PY) scripts/doctor.py

data:
	$(PY) scripts/make_data.py --seed 42

user-sign-ins:
	$(PY) scripts/user_sign_ins.py generate --seed "$(SEED)" --rows "$(ROWS)" --error-rate "$(ERROR_RATE)"
	$(PY) scripts/user_sign_ins.py land
	$(PY) scripts/user_sign_ins.py profile

clean-data:
	rm -rf data/raw/*.csv data/raw/gps data/fixtures/*.parquet data/fixtures/*.csv data/fixtures/*.json

clean-lakehouse:
	rm -rf lakehouse metastore_db derby.log spark-warehouse dbt/masar/target dbt/masar/logs

test:
	$(PY) -m pytest -q tests

register:
	$(PY) -m masar.register

dbt-deps:
	cd dbt/masar && $(DBT) deps

dbt-debug:
	cd dbt/masar && $(DBT) debug

kafka-up:
	docker compose up -d kafka

kafka-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
	  --topic masar.gps.pings --partitions 3 --bootstrap-server localhost:9092

kafka-down:
	docker compose down

pipeline:
	$(PY) -m masar.pipeline

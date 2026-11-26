# Masar capstone: submission

Name: Mohammed Aljoudi
Branch: main
Link: <https://github.com/maljoudi123123/masar-lakehouse/tree/main>

How to submit: `git add -A && git commit -m "Capstone" && git push`. You push to your own
fork (GitHub creates it on your first push). Send the link above to your trainer. If you
never pushed, send this file and the output of `python -m masar.proof` instead.

## What I built (one line per step)
1. Bronze: `masar.ingest.land_bronze` lands the raw trips CSV, drivers CSV and GPS NDJSON as-is into Delta tables, adding `_ingested_at` and `_source_file` for lineage; `masar.register` registers them so dbt can read them.
2. Silver (dbt): `dbt build --select +silver_trips` cleans and types the trips (staging), keeps the latest load per trip, joins drivers, keeps completed trips only, and MERGEs on `trip_id`; 8 dbt tests pass, then `masar.quality.check_silver` runs the contract checks (all `bad_rows=0`) and fails fast on a missing or duplicate key.
3. Delta control: `masar.delta.capstone_controls` adds the `positive_fare` CHECK constraint, which rejects a batch with 120 negative fares (count unchanged at 44,371), and safely adds a `promo_code` column with `ALTER TABLE ADD COLUMNS` after an unplanned write with that column was rejected.
4. Gold: `masar.transform.build_gold` builds `zone_hourly_demand` (one row per city, date and hour: 120 rows) and `trip_features` (one row per trip, demand from the previous hour only).
5. BI-safe table: `masar.serve.bi_view` builds `bi_trips_safe` with salted SHA-256 pseudonyms instead of rider and driver IDs and a coarsened dropoff area, and checks that no PII column is in the output; `masar.serve.training_join` builds the training set and checks that no post-trip column is used as a feature.
6. Pipeline (`make pipeline`): `masar.pipeline` runs every step above in order, each in its own process, and stops at the first failure; it exits 0 from a clean lakehouse in about 2 minutes and is safe to re-run.
7. Proof (counts + time travel): `masar.proof --erase` erases one rider (right to erasure), prints the row count of every table per layer, and compares silver version 0 with the latest version.

## Counts per layer
```
layer   table                     rows   expected
bronze  trips                   48,200     48,200  ok
bronze  drivers                  1,850      1,850  ok
bronze  gps_pings              120,000    120,000  ok
silver  trips                   44,364     44,371  ok (after erase)
gold    zone_hourly_demand         120        120  ok
gold    trip_features           44,371     44,371  ok
gold    training_set            44,371     44,371  ok
gold    bi_trips_safe           44,371     44,371  ok
```
Silver was 44,371 straight after `make pipeline`; it is 44,364 after the erase removed the 7 trips of one rider.

## Time travel
```
+-------+-----------------------+---------------------------------+-------+-------+
|version|timestamp              |operation                        |written|deleted|
+-------+-----------------------+---------------------------------+-------+-------+
|0      |2026-11-26 12:10:32.872|CREATE OR REPLACE TABLE AS SELECT|44371  |NULL   |
|1      |2026-11-26 12:11:59.629|ADD CONSTRAINT                   |NULL   |NULL   |
|2      |2026-11-26 12:12:05.093|ADD COLUMNS                      |NULL   |NULL   |
|3      |2026-11-26 12:12:16.57 |DELETE                           |NULL   |7      |
+-------+-----------------------+---------------------------------+-------+-------+

time travel: version 0 = 44,371 rows, latest version 3 = 44,364 rows
CHECK PASSED: 4 versions, older count differs from latest (-7 rows since version 0)
```

## Steps the trainer handed over (if any)
None.

## One thing I would improve
The erased rider's trips come back the next time the pipeline runs: bronze still holds them, and the
silver MERGE re-inserts them (silver goes from 44,364 back to 44,371). I would keep a table of erased
rider IDs and filter it out in `stg_trips`, so an erasure survives every rebuild. I also made the step 3
schema change survive re-runs: without enabling Delta schema auto-merge in the silver dbt model, the next
MERGE failed because the new column was not in the dbt query.

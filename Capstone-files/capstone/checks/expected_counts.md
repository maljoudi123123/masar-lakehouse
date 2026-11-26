# Expected outputs (from a Codespace run on day3-solutions, seed 42)

| Step | Table / output | Expected rows |
|------|----------------|---------------|
| 1 | bronze/trips | 48,200 |
| 1 | bronze/drivers | 1,850 |
| 1 | bronze/gps_pings | 120,000 |
| 2 | silver/trips | 44,371 |
| 4 | gold/zone_hourly_demand | 120 |
| 4 | gold/trip_features | 44,371 |
| 5 | gold/training_set | 44,371 |
| 5 | gold/bi_trips_safe | 44,371 |
| 7 | time travel on silver/trips | older version count differs from latest |

Quality checks (all passed, bad_rows=0): trip_id_unique, fare_is_double, fare_positive,
city_known, duration_sane, fare_mean_sane.

Silver count: 44,371 after `make data` and one `make pipeline`. The value 44,364 appears
only if the Day 2 erase was run and the pipeline has not yet re-merged (DAY3.md).

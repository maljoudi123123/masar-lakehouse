import pandas as pd
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark
from datetime import datetime
spark = get_spark("break-it")


def count():
    return spark.read.format("delta").load(SILVER).count()


try:
    print("before:", count())

    bad = spark.createDataFrame(pd.read_parquet("data/fixtures/dirty_batch_bad_fares.parquet"))
    for f in spark.read.format("delta").load(SILVER).schema.fields:
        if f.name in bad.columns:
            bad = bad.withColumn(f.name, F.col(f.name).cast(f.dataType))
    try:
        bad.write.format("delta").mode("append").save(SILVER)
        print("UNEXPECTED: write succeeded")
    except Exception as e:
        print("REJECTED:", type(e).__name__, str(e)[:300])
    print("after bad fares:", count())

    # A real driver_id, so the driver_id_not_null constraint and the dbt relationships test both pass.
    driver_id = spark.read.format("delta").load(SILVER).select("driver_id").first()[0]
    bad_type = (
        # 10.0, not 10: fare_sar is DOUBLE and Delta rejects an integer (BIGINT) column.
        spark.createDataFrame([("T-BAD-2", driver_id, "Riyadh", 10.0, datetime(2026,6,1,10,0), datetime(2027,6,1,10,0))], ["trip_id", "driver_id", "city", "fare_sar", "started_at", "ended_at"])
        # silver stores these as TIMESTAMP, not text
        .withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("ended_at", F.to_timestamp("ended_at"))
    )
    try:
        bad_type.write.format("delta").mode("append").save(SILVER)
        print("UNEXPECTED: write succeeded")
    except Exception as e:
        print("REJECTED:", type(e).__name__, str(e)[:300])
    print("after bad type:", count())
finally:
    spark.stop()

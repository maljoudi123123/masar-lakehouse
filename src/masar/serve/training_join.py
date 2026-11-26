from pyspark.sql import functions as F

from masar.config import GOLD, SILVER
from masar.spark import get_spark

# CAPSTONE: added. Columns only known after (or during) the trip. leakage_drill.py showed
# that fare_per_km and same-hour demand inflate R2 without being available at prediction time;
# the rest describe the finished trip or are the label itself.
LEAKY = {"duration_min", "ended_at", "fare_sar", "distance_km", "fare_per_km", "demand_same_hour"}


def build_training_set(label_col: str = "duration_min"):
    spark = get_spark("training-join")
    feats = spark.read.format("delta").load(f"{GOLD}/trip_features")
    labels = (spark.read.format("delta").load(SILVER)
              .select("trip_id", F.col(label_col).alias("label")))
    return (feats.join(labels, "trip_id", "inner")
                 .withColumn("_trained_on_feature_version", F.col("_feature_version")))


# CAPSTONE: added. Step 5 checkpoint "leakage check passes": no feature may be a post-trip
# column, and the label may appear only as `label`.
def check_leakage(df) -> None:
    leaked = LEAKY & set(df.columns)
    assert not leaked, f"leaky feature columns in the training set: {sorted(leaked)}"
    print("leakage check passed: only prior-hour features, label kept apart")


if __name__ == "__main__":
    df = build_training_set()
    check_leakage(df)  # CAPSTONE: added, runs before anything is written
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/training_set")
    print(f"training rows: {df.count():,}")
    print("columns:", df.columns)
    df.show(truncate=False)

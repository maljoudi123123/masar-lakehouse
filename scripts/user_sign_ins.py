"""Generate, land, and profile sample user sign-ins for the Masar workshop."""

import argparse
import csv
import fcntl
import random
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

RAW_FILE = ROOT / "data" / "raw" / "user_sign_ins.csv"
BASE_FIELDS = ("sign_in_id", "user_id", "user_name", "sign_in_date", "sign_in_time", "status", "device")
ADDED_FIELDS = ("response_ms", "mfa_used")
FIELDS = BASE_FIELDS + ADDED_FIELDS
USERS = (("U01", "noura"), ("U02", "omar"), ("U03", "sara"), ("U04", "faisal"))
STATUSES = ("success", "failed")


@contextmanager
def raw_file_lock():
    # Lock a separate file because publishing an updated CSV replaces its inode.
    RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RAW_FILE.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def copy_existing_rows(writer):
    if not RAW_FILE.exists():
        return 0
    with RAW_FILE.open(encoding="utf-8", newline="") as file:
        reader = csv.reader(file, strict=True)
        header = tuple(next(reader, ()))
        if not header:
            return 0
        if len(header) < len(BASE_FIELDS) or header != FIELDS[:len(header)]:
            raise ValueError(f"Unexpected header in {RAW_FILE}: {header}")
        previous_rows = 0
        for row in reader:
            if len(row) != len(header):
                raise ValueError(f"Unexpected field count at line {reader.line_num} in {RAW_FILE}")
            row.extend([""] * (len(FIELDS) - len(header)))
            writer.writerow(row)
            previous_rows += 1
        return previous_rows


def generate(seed: int, rows: int, error_rate: float):
    start = datetime(2026, 6, 1)
    errors = 0
    batch_id = uuid4().hex
    with raw_file_lock(), TemporaryDirectory(dir=RAW_FILE.parent) as temp:
        output = Path(temp) / RAW_FILE.name
        with output.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(FIELDS)
            previous_rows = copy_existing_rows(writer)
            rng = random.Random(seed if previous_rows == 0 else f"{seed}:{previous_rows}")
            for index in range(1, rows + 1):
                user_id, name = rng.choice(USERS)
                when = start + timedelta(seconds=rng.randrange(30 * 24 * 60 * 60))
                sign_in_date = when.date().isoformat()
                sign_in_time = when.time().isoformat()
                status = rng.choice(STATUSES)
                device = rng.choice(("mobile", "desktop", "tablet"))
                response_ms = str(rng.randint(15, 2500))
                mfa_used = rng.choice(("true", "false"))
                if rng.random() < error_rate:
                    errors += 1
                    error = rng.choice(("identity", "status", "response_ms", "mfa_used", "date", "time"))
                    if error == "identity":
                        name = rng.choice([other for _, other in USERS if other != name])
                    elif error == "status":
                        status = "pending"  # A completed attempt must have a final outcome.
                    elif error == "response_ms":
                        response_ms = rng.choice(("fast", "250ms", "NaN", "12.5", "2147483648"))
                    elif error == "mfa_used":
                        mfa_used = rng.choice(("sometimes", "2", "[true]"))
                    elif error == "date":
                        sign_in_date = rng.choice(("2026-02-30", "20260601", "2026"))
                    else:
                        sign_in_time = "25:61:00"
                event_id = f"S-{batch_id}-{index:04d}"
                writer.writerow((event_id, user_id, name, sign_in_date, sign_in_time, status, device,
                                 response_ms, mfa_used))
        output.replace(RAW_FILE)
    print(f"Appended {rows} sign-ins to {RAW_FILE} "
          f"(total={previous_rows + rows}, seed={seed}, injected errors={errors})")


def with_legacy_fields(df):
    """Older sign-in records have no value for fields added after the original schema."""
    from pyspark.sql import functions as F

    for name in ADDED_FIELDS:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
    return df


def land(spark, destination):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    raw = with_legacy_fields(spark.read.option("header", True).csv(str(RAW_FILE)))
    events = (
        raw.select(*FIELDS)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    # Keep the original strings in bronze. Replaying the CSV adds only new events.
    if not DeltaTable.isDeltaTable(spark, destination):
        events.limit(0).write.format("delta").mode("ignore").save(destination)
    elif set(ADDED_FIELDS).difference(spark.read.format("delta").load(destination).columns):
        events.limit(0).write.format("delta").mode("append").option("mergeSchema", "true").save(destination)
    (
        DeltaTable.forPath(spark, destination).alias("bronze")
        .merge(events.alias("raw"), "bronze.sign_in_id <=> raw.sign_in_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Landed sign-ins at {destination}")


def profile(spark, destination):
    from pyspark.sql import functions as F

    events = with_legacy_fields(spark.read.format("delta").load(destination))
    date = F.when(
        F.col("sign_in_date").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
        F.expr("try_cast(sign_in_date as date)"),
    )
    time = F.coalesce(F.col("sign_in_time"), F.lit(""))
    invalid_time = ~time.rlike(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")
    response_ms = F.col("response_ms")
    invalid_response = response_ms.isNotNull() & (
        ~response_ms.rlike(r"^[0-9]+$") | F.expr("try_cast(response_ms as int)").isNull()
    )
    mfa_used = F.col("mfa_used")
    invalid_mfa = mfa_used.isNotNull() & ~F.lower(mfa_used).isin("true", "false")
    missing_name = F.trim(F.coalesce(F.col("user_name"), F.lit(""))) == ""
    registered_names = F.create_map(*(F.lit(value) for pair in USERS for value in pair))
    expected_name = registered_names[F.col("user_id")]
    wrong_identity = expected_name.isNull() | ~expected_name.eqNullSafe(F.col("user_name"))
    invalid_status = F.col("status").isNull() | ~F.col("status").isin(*STATUSES)
    events.agg(
        F.count("*").alias("sign_in_attempts"),
        F.countDistinct("user_id").alias("distinct_users"),
        F.count_if(missing_name).alias("missing_user_names"),
        F.count_if(wrong_identity).alias("user_id_name_mismatches"),
        F.count_if(invalid_status).alias("invalid_sign_in_statuses"),
        F.count_if(date.isNull()).alias("missing_or_invalid_sign_in_dates"),
        F.count_if(invalid_time).alias("missing_or_invalid_sign_in_times"),
        F.count_if(invalid_response).alias("invalid_response_ms"),
        F.count_if(response_ms.isNull()).alias("missing_response_ms"),
        F.count_if(invalid_mfa).alias("invalid_mfa_used"),
        F.count_if(mfa_used.isNull()).alias("missing_mfa_used"),
        F.min(date).alias("first_sign_in_date"),
        F.max(date).alias("last_sign_in_date"),
    ).show(vertical=True)
    type_checks = (
        ("response_ms", "nonnegative 32-bit integer", invalid_response),
        ("mfa_used", "true or false", invalid_mfa),
        ("sign_in_date", "valid YYYY-MM-DD", F.col("sign_in_date").isNotNull() & date.isNull()),
        ("sign_in_time", "valid HH:MM:SS", F.col("sign_in_time").isNotNull() & invalid_time),
    )
    issues = [
        F.when(invalid, F.struct(F.lit(name).alias("field"), F.col(name).alias("raw_value"),
                                 F.lit(expected).alias("expected")))
        for name, expected, invalid in type_checks
    ]
    print("Type errors (up to 20 examples)")
    events.select("sign_in_id", F.explode(F.array(*issues)).alias("issue")).where(
        F.col("issue").isNotNull()
    ).select("sign_in_id", "issue.*").show(20, truncate=False)
    events.groupBy("status").count().orderBy("status").show()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    generator = actions.add_parser("generate")
    generator.add_argument("--seed", type=int, default=42)
    generator.add_argument("--rows", type=int, default=100, help="Number of new sign-ins to append")
    generator.add_argument("--error-rate", type=float, default=0.1,
                           help="Per-row probability of injecting one business or data-format error (0 to 1)")
    actions.add_parser("land")
    actions.add_parser("profile")
    args = parser.parse_args()
    action = args.action
    if action == "generate":
        if args.rows < 1 or not 0 <= args.error_rate <= 1:
            generator.error("--rows must be positive and --error-rate must be between 0 and 1")
        generate(args.seed, args.rows, args.error_rate)
        return

    from masar.config import BRONZE
    from masar.spark import get_spark

    destination = str(Path(BRONZE) / "user_sign_ins")
    spark = get_spark(f"user-sign-ins-{action}")
    try:
        if action == "land":
            with raw_file_lock():
                land(spark, destination)
        else:
            profile(spark, destination)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

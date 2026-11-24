from masar.spark import get_spark


def add_constraints() -> None:
    spark = get_spark("enforce-silver")
    try:
        # positive_fare and valid_duration already exist on silver.trips; re-adding them fails.
        # valid_distance has not been added yet: uncomment it to add it.
        # spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT positive_fare CHECK (fare_sar > 0)")
        # spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT valid_duration CHECK (ended_at > started_at)")
        # spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT valid_distance CHECK (distance_km > 0)")

        # Every trip must have a driver: Delta rejects any future write with a null driver_id.
        # A CHECK is used because open-source Delta cannot ALTER an existing column to NOT NULL.
        # Skipped when it already exists, so the script stays safe to re-run.
        props = {r[0] for r in spark.sql("SHOW TBLPROPERTIES silver.trips").collect()}
        if "delta.constraints.driver_id_not_null" not in props:
            spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT driver_id_not_null CHECK (driver_id IS NOT NULL)")

        # Every driver_id must exist in drivers. Delta has no foreign keys and a CHECK
        # cannot look at another table, so this is verified with a query instead.
        orphans = spark.sql("""
            SELECT t.trip_id, t.driver_id
            FROM silver.trips t
            LEFT ANTI JOIN default.stg_drivers d ON t.driver_id = d.driver_id
        """)
        n = orphans.count()
        if n:
            orphans.show(10, truncate=False)
            raise ValueError(f"{n} trips have a driver_id that is not in stg_drivers")
        print("OK: every driver_id in silver.trips exists in stg_drivers")

        spark.sql("SHOW TBLPROPERTIES silver.trips").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    add_constraints()

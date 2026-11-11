'''
1. When do pool price spikes happen, and what are the demand and wind doing here?
'''
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

SPIKE_MULTIPLIER = 3.0
SPIKE_FLOOR = 100.0


ONE_HOUR = F.expr("INTERVAL 1 HOUR")

@dlt.table(
    name="ab_power.gold.fact_hourly_market",
    comment=(
        "One row per UTC hour: pool price, Alberta Internal Load, forecast error, "
        "Alberta local-time attributes and the spike flag. "
        "Single source of truth for the spike rule."
    ),
    table_properties={"quality": "gold"},
)
def fact_hourly_market():
    pool_price = spark.read.table("ab_power.silver.pool_price").select(
        "begin_datetime_utc", "pool_price", "rolling_30day_avg"
    )
    internal_load = spark.read.table("ab_power.silver.alberta_internal_load").select(
        "begin_datetime_utc", "alberta_internal_load", "forecast_alberta_internal_load"
    )

    return (
        # LEFT JOIN: every price hour is kept even if load is missing
        pool_price.join(internal_load, on="begin_datetime_utc", how="left")
        # Local time is derived from the UTC key, so it can never disagree with it
        .withColumn("begin_datetime_local",
                    F.from_utc_timestamp("begin_datetime_utc", "America/Edmonton"))
        .withColumn("hour_of_day_local", F.hour("begin_datetime_local"))
        .withColumn("day_of_week_local", F.date_format("begin_datetime_local", "EEEE"))
        # Positive = demand came in above forecast
        .withColumn("forecast_error_mw",
                    F.col("alberta_internal_load") - F.col("forecast_alberta_internal_load"))
        # Missing 30-day average -> not a spike (False, not NULL), so downstream logic is predictable
        .withColumn(
            "is_spike",
            F.coalesce(
                (F.col("pool_price") > SPIKE_MULTIPLIER * F.col("rolling_30day_avg"))
                & (F.col("pool_price") > SPIKE_FLOOR),
                F.lit(False),
            ),
        )
        .select(
            "begin_datetime_utc", "begin_datetime_local", "hour_of_day_local", "day_of_week_local",
            "pool_price", "rolling_30day_avg",
            "alberta_internal_load", "forecast_alberta_internal_load", "forecast_error_mw",
            "is_spike",
        )
    )


@dlt.table(
    name='ab_power.gold.agg_price_spike_events',
    comment=(
        f"One row per price spike event: a run of consecutive spike hours as flagged "
        f"in gold.fact_hourly_market (see that table for the spike rule). "
        f"Includes demand and forecast error during the event. "
        f"event_start_utc is the stable identifier; event_id is recalculated on each refresh."
    ),
    table_properties={
        "quality":"gold"
    }
)
def agg_price_spike_events():

    flagged_data = (
        spark.read.table("ab_power.gold.fact_hourly_market")
        .select(
            "begin_datetime_utc",
            "begin_datetime_local",
            "pool_price",
            "alberta_internal_load",
            "forecast_alberta_internal_load",
            "forecast_error_mw",
            "is_spike",
        )
    )

    by_hour = Window.orderBy("begin_datetime_utc")
    with_prev = (
        flagged_data
        .withColumn("prev_is_spike", F.lag("is_spike").over(by_hour))
        .withColumn("prev_datetime_utc", F.lag("begin_datetime_utc").over(by_hour))
    )

    event_starts = with_prev.withColumn(
        "is_event_start",
        F.col("is_spike")
        & (
            F.col("prev_datetime_utc").isNull()
            | ~F.col("prev_is_spike")
            | (F.col("prev_datetime_utc") != F.col("begin_datetime_utc") - ONE_HOUR)
        ),
    )

    running = by_hour.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    event_ids = event_starts.withColumn(
        "event_id",
        F.sum(F.when(F.col("is_event_start"), 1).otherwise(0)).over(running),
    )

    return (
        event_ids
        .filter(F.col("is_spike"))
        .groupBy("event_id")
        .agg(
            F.min("begin_datetime_utc").alias("event_start_utc"),
            F.min("begin_datetime_local").alias("event_start_local"),
            (F.max("begin_datetime_utc") + ONE_HOUR).alias("event_end_utc"),
            F.count("*").alias("duration_hours"),
            F.max("pool_price").alias("peak_price"),
            F.round(F.avg("pool_price"), 2).alias("avg_price"),
            F.round(F.avg("alberta_internal_load"), 0).alias("avg_load_mw"),
            F.max("alberta_internal_load").alias("peak_load_mw"),
            F.round(
                F.avg(F.col("alberta_internal_load") - F.col("forecast_alberta_internal_load")),
                0,
            ).alias("avg_forecast_error_mw"),
        )
    )

@dlt.table(
    name="ab_power.gold.dim_asset_current",
    comment=(
        "One row per asset, current state only: asset attributes, current participants, "
        "and fuel type and capability from the asset's latest generation snapshot. "
        "History is kept in Silver's SCD2 tables."
    ),
    table_properties={"quality": "gold"},
)
def dim_asset_current():
    # Step 1: current version of every asset
    current_assets = (
        spark.read.table("ab_power.silver.dim_asset")
        .filter(F.col("__END_AT").isNull())
        .select(
            "asset_id", "asset_name", "asset_type", "operating_status",
            "net_to_grid_asset_flag", "asset_incl_storage_flag",
            F.col("__START_AT").alias("current_version_since"),
        )
    )

    # Step 2: current participants, aggregated to ONE row per asset before joining (no fan-out)
    current_participants = (
        spark.read.table("ab_power.silver.dim_participant")
        .filter(F.col("__END_AT").isNull())
        .select("pool_participant_id", "pool_participant_name")
    )
    participants_per_asset = (
        spark.read.table("ab_power.silver.bridge_asset_participant")
        .filter(F.col("__END_AT").isNull())
        .select("asset_id", "pool_participant_id")
        .join(current_participants, on="pool_participant_id", how="left")
        .groupBy("asset_id")
        .agg(
            F.count("pool_participant_id").alias("participant_count"),
            F.array_sort(F.collect_set("pool_participant_id")).alias("pool_participant_ids"),
            F.array_sort(F.collect_set("pool_participant_name")).alias("pool_participant_names"),
        )
    )

    # Step 3: each asset's most recent generation snapshot (row_number: exactly one row, no ties)
    latest_by_asset = Window.partitionBy("asset_id").orderBy(F.col("snapshot_utc").desc())
    latest_gen_row = (
        spark.read.table("ab_power.silver.generation_snapshot")
        .withColumn("rn", F.row_number().over(latest_by_asset))
        .filter(F.col("rn") == 1)
        .select(
            "asset_id", "fuel_type", "sub_fuel_type",
            F.col("maximum_capability").alias("maximum_capability_mw"),
            F.col("snapshot_utc").alias("last_generation_snapshot_utc"),
        )
    )

    # Step 4: LEFT JOINs keep every current asset, even with no participants or generation data
    return (
        current_assets
        .join(participants_per_asset, on="asset_id", how="left")
        .join(latest_gen_row, on="asset_id", how="left")
        .select(
            "asset_id", "asset_name", "asset_type", "operating_status",
            "net_to_grid_asset_flag", "asset_incl_storage_flag", "current_version_since",
            # No participants -> 0 and empty arrays, not NULL
            F.coalesce("participant_count", F.lit(0)).alias("participant_count"),
            F.coalesce("pool_participant_ids", F.array().cast("array<string>")).alias("pool_participant_ids"),
            F.coalesce("pool_participant_names", F.array().cast("array<string>")).alias("pool_participant_names"),
            "fuel_type", "sub_fuel_type", "maximum_capability_mw", "last_generation_snapshot_utc",
        )
    )
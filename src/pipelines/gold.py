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
    name='pp_spike_data',
    comment=f"One row per price spike event: a run of consecutive hours with price > "
        f"{SPIKE_MULTIPLIER:g}x the rolling 30-day average and > ${SPIKE_FLOOR:g}/MWh. "
        f"Includes demand and forecast error during the event.",
    table_properties={
        "quality":"gold"
    }
)
def agg_price_spike_events():
    price = (
        spark.read.table("ab_power.silver.pool_price")
        .select('begin_datetime_utc', 'pool_price', 'rolling_30day_avg')
    )

    load = spark.read.table("ab_power.silver.alberta_internal_load").select(
        "begin_datetime_utc", "alberta_internal_load", "forecast_alberta_internal_load"
    )

    flagged_data = (
        price.join(
            load,
            on="begin_datetime_utc",
            how='left'
        )
        .withColumn("is_spike",
        (F.col("pool_price") > SPIKE_MULTIPLIER * F.col("rolling_30day_avg"))
        & (F.col("pool_price") > SPIKE_FLOOR))
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
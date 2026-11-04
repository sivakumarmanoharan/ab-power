"""
Silver layer: clean, typed, deduplicated AESO data.

Pattern per feed:
  1. A view explodes the Bronze response into one row per record, casts types,
     and enforces data quality expectations (the data contract).
  2. AUTO CDC writes the view into a Silver table keyed on the business key, so
     overlapping pulls and AESO's late revisions collapse to one current row per key.

The asset list is a full snapshot on every pull, so AUTO CDC FROM SNAPSHOT compares
snapshots and keeps history as SCD Type 2, split into:
  dim_asset                 one row per asset
  dim_participant           one row per pool participant
  bridge_asset_participant  which participants offer which assets (many-to-many)

Requires pipeline config: spark.sql.session.timeZone = UTC
"""

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


# =====================================================================
# Helpers
# =====================================================================

def num(col: str):
    """AESO sends numbers as strings, sometimes empty. try_cast turns bad values
    into NULL instead of failing the run (serverless runs with ANSI mode on)."""
    return F.expr(f"try_cast({col} AS DOUBLE)")


def ts_utc(col: str):
    """UTC timestamp. Relies on the pipeline session timezone being UTC."""
    return F.expr(f"try_cast({col} AS TIMESTAMP)")


def ts_local(col: str):
    """Mountain Prevailing Time as wall-clock time, no timezone attached.
    Not unique during the November fall-back hour, so never use it as a key."""
    return F.expr(f"try_cast({col} AS TIMESTAMP_NTZ)")


def batch_ts():
    """Ingestion run time parsed from _batch_id. Orders versions: latest pull wins."""
    return F.to_timestamp(F.col("_batch_id"), "yyyyMMdd'T'HHmmss'Z'")


# =====================================================================
# Pool price: one row per settlement hour
# =====================================================================

@dlt.view(name="pool_price_parsed")
@dlt.expect_or_fail("valid_utc_timestamp", "begin_datetime_utc IS NOT NULL")
@dlt.expect_or_fail(
    "no_settled_price_in_future",
    "pool_price IS NULL OR begin_datetime_utc <= current_timestamp()",
)
@dlt.expect_or_drop("settled_price_present", "pool_price IS NOT NULL")
@dlt.expect("price_within_market_bounds", "pool_price BETWEEN 0 AND 999.99")
def pool_price_parsed():
    return (
        spark.readStream.table("ab_power.bronze.pool_price")
        .select(
            F.explode("return.pool_price_report").alias("r"),
            "_batch_id",
            "_source_file",
        )
        .select(
            ts_utc("r.begin_datetime_utc").alias("begin_datetime_utc"),
            ts_local("r.begin_datetime_mpt").alias("begin_datetime_mpt"),
            num("r.pool_price").alias("pool_price"),
            num("r.forecast_pool_price").alias("forecast_pool_price"),
            num("r.rolling_30day_avg").alias("rolling_30day_avg"),
            "_batch_id",
            "_source_file",
            batch_ts().alias("_batch_ts"),
        )
    )


dlt.create_streaming_table(
    name="ab_power.silver.pool_price",
    comment="Hourly settled pool price ($/MWh). One row per hour. Latest AESO revision wins.",
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)

dlt.create_auto_cdc_flow(
    target="ab_power.silver.pool_price",
    source="pool_price_parsed",
    keys=["begin_datetime_utc"],
    sequence_by=F.col("_batch_ts"),
    stored_as_scd_type=1,
)


# =====================================================================
# Alberta Internal Load: actual + forecast, one row per hour
# =====================================================================

@dlt.view(name="load_parsed")
@dlt.expect_or_fail("valid_utc_timestamp", "begin_datetime_utc IS NOT NULL")
@dlt.expect_or_drop(
    "has_actual_or_forecast",
    "alberta_internal_load IS NOT NULL OR forecast_alberta_internal_load IS NOT NULL",
)
# Physically impossible
@dlt.expect("non_negative_load",
            "alberta_internal_load IS NULL OR alberta_internal_load >= 0")
@dlt.expect("non_negative_forecast",
            "forecast_alberta_internal_load IS NULL OR forecast_alberta_internal_load >= 0")
# Implausible for Alberta: likely a units or parsing problem
@dlt.expect("load_within_plausible_range",
            "alberta_internal_load IS NULL OR alberta_internal_load BETWEEN 5000 AND 15000")
# Completeness: a finished hour should have an actual value
@dlt.expect("actual_present_for_settled_hours",
            "begin_datetime_utc > current_timestamp() - INTERVAL 3 HOURS "
            "OR alberta_internal_load IS NOT NULL")
# Forecast accuracy: big misses are worth seeing, not hiding
@dlt.expect("forecast_within_10pct_of_actual",
            "alberta_internal_load IS NULL OR forecast_alberta_internal_load IS NULL "
            "OR abs(alberta_internal_load - forecast_alberta_internal_load) "
            "<= 0.10 * alberta_internal_load")
def load_parsed():
    return (
        spark.readStream.table("ab_power.bronze.load")
        .select(
            F.explode("return.actual_forecast_report").alias("r"),
            "_batch_id",
            "_source_file",
        )
        .select(
            ts_utc("r.begin_datetime_utc").alias("begin_datetime_utc"),
            ts_local("r.begin_datetime_mpt").alias("begin_datetime_mpt"),
            num("r.alberta_internal_load").alias("alberta_internal_load"),
            num("r.forecast_alberta_internal_load").alias("forecast_alberta_internal_load"),
            "_batch_id",
            "_source_file",
            batch_ts().alias("_batch_ts"),
        )
    )


dlt.create_streaming_table(
    name="ab_power.silver.alberta_internal_load",
    comment="Hourly Alberta Internal Load (MW), actual and forecast. Latest AESO revision wins.",
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)

dlt.create_auto_cdc_flow(
    target="ab_power.silver.alberta_internal_load",
    source="load_parsed",
    keys=["begin_datetime_utc"],
    sequence_by=F.col("_batch_ts"),
    stored_as_scd_type=1,
)


# =====================================================================
# Asset list: SCD Type 2, normalized into asset, participant and bridge.
# An asset can be offered by several participants (e.g. NX01), so the
# participant is a many-to-many relationship, not an asset attribute.
# =====================================================================

def latest_asset_rows():
    """Latest asset-list snapshot, exploded to one row per entry."""
    bronze = spark.read.table("ab_power.bronze.assets")
    latest = bronze.agg(F.max("_batch_id").alias("_batch_id"))
    return (
        bronze.join(latest, "_batch_id")
        .select(F.explode("return").alias("a"))
        .select(
            F.trim("a.asset_id").alias("asset_id"),
            F.trim("a.asset_name").alias("asset_name"),
            F.trim("a.asset_type").alias("asset_type"),
            F.trim("a.operating_status").alias("operating_status"),
            F.trim("a.net_to_grid_asset_flag").alias("net_to_grid_asset_flag"),
            F.trim("a.asset_incl_storage_flag").alias("asset_incl_storage_flag"),
            F.trim("a.pool_participant_id").alias("pool_participant_id"),
            F.trim("a.pool_participant_name").alias("pool_participant_name"),
        )
    )


# Snapshot views hold business columns only. Metadata like _batch_id must NOT be
# included, or every snapshot would look like a change and create new SCD2 versions.
# .distinct() collapses identical rows; a real contradiction (same key, different
# attributes) still fails the run with a duplicate key error, which is intended.

@dlt.view(name="assets_latest_snapshot")
@dlt.expect_or_drop("asset_id_present", "asset_id IS NOT NULL")
def assets_latest_snapshot():
    return latest_asset_rows().select(
        "asset_id", "asset_name", "asset_type", "operating_status",
        "net_to_grid_asset_flag", "asset_incl_storage_flag",
    ).distinct()


@dlt.view(name="participants_latest_snapshot")
@dlt.expect_or_drop("participant_id_present", "pool_participant_id IS NOT NULL")
def participants_latest_snapshot():
    # One participant ID can carry several names in a single snapshot. Non-retired
    # assets show the current name; retired assets keep the name from when they
    # retired (e.g. NXI: Nexen -> CNOOC; APC: ATCO Power -> Heartland Generation;
    # PCES: EnCana -> Ovintiv). About 43% of registered assets are retired.
    # Rule: names on non-retired assets win, then the most-used name, then
    # alphabetical, so the result is deterministic on every run.
    name_counts = (
        latest_asset_rows()
        .groupBy("pool_participant_id", "pool_participant_name")
        .agg(
            F.countDistinct(
                F.when(F.lower("operating_status") != "retired", F.col("asset_id"))
            ).alias("current_asset_count"),
            F.countDistinct("asset_id").alias("asset_count"),
        )
    )
    w = Window.partitionBy("pool_participant_id").orderBy(
        F.col("current_asset_count").desc(),
        F.col("asset_count").desc(),
        F.col("pool_participant_name").asc(),
    )
    return (
        name_counts
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select("pool_participant_id", "pool_participant_name")
    )


@dlt.view(name="asset_participants_latest_snapshot")
@dlt.expect_or_drop("pair_keys_present",
                    "asset_id IS NOT NULL AND pool_participant_id IS NOT NULL")
def asset_participants_latest_snapshot():
    return latest_asset_rows().select(
        "asset_id", "pool_participant_id",
    ).distinct()


# SCD2 history cannot be rebuilt from Bronze, so these tables are protected from
# full refresh. All three are temporarily "true" so they can be rebuilt once after
# the earlier failed runs. Set all three back to "false" after the first successful run.

dlt.create_streaming_table(
    name="ab_power.silver.dim_asset",
    comment="AESO assets, one row per asset, SCD Type 2 (__START_AT / __END_AT).",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "false"},  
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="ab_power.silver.dim_asset",
    source="assets_latest_snapshot",
    keys=["asset_id"],
    stored_as_scd_type=2,
)

dlt.create_streaming_table(
    name="ab_power.silver.dim_participant",
    comment="AESO pool participants, one row per participant, SCD Type 2. Current name chosen by rule; see participants_latest_snapshot.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "false"},  
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="ab_power.silver.dim_participant",
    source="participants_latest_snapshot",
    keys=["pool_participant_id"],
    stored_as_scd_type=2,
)

dlt.create_streaming_table(
    name="ab_power.silver.bridge_asset_participant",
    comment="Which participants offer which assets, SCD Type 2. Keys only; attributes live in the dimensions.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "false"},  
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="ab_power.silver.bridge_asset_participant",
    source="asset_participants_latest_snapshot",
    keys=["asset_id", "pool_participant_id"],
    stored_as_scd_type=2,
)


# =====================================================================
# Generation: per-asset snapshot, one row per asset per AESO update
# =====================================================================

@dlt.view(name="generation_parsed")
@dlt.expect_or_fail("valid_snapshot_timestamp", "snapshot_utc IS NOT NULL")
@dlt.expect_or_drop("asset_id_present", "asset_id IS NOT NULL")
@dlt.expect("non_negative_capability",
            "maximum_capability IS NULL OR maximum_capability >= 0")
# Storage assets go negative while charging: warn, never drop.
@dlt.expect("generation_within_capability",
            "net_generation IS NULL OR maximum_capability IS NULL "
            "OR net_generation <= maximum_capability * 1.05")
def generation_parsed():
    return (
        spark.readStream.table("ab_power.bronze.generation")
        .select(
            ts_utc("`return`.last_updated_datetime_utc").alias("snapshot_utc"),
            ts_local("`return`.last_updated_datetime_mpt").alias("snapshot_mpt"),
            F.explode("return.asset_list").alias("g"),
            "_batch_id",
            "_source_file",
        )
        .select(
            F.trim("g.asset").alias("asset_id"),
            F.trim("g.fuel_type").alias("fuel_type"),
            F.trim("g.sub_fuel_type").alias("sub_fuel_type"),
            num("g.maximum_capability").alias("maximum_capability"),
            num("g.net_generation").alias("net_generation"),
            num("g.dispatched_contingency_reserve").alias("dispatched_contingency_reserve"),
            "snapshot_utc",
            "snapshot_mpt",
            "_batch_id",
            "_source_file",
            batch_ts().alias("_batch_ts"),
        )
    )


dlt.create_streaming_table(
    name="ab_power.silver.generation_snapshot",
    comment="Per-asset net generation (MW) at each AESO CSD update. One row per asset per snapshot.",
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)

dlt.create_auto_cdc_flow(
    target="ab_power.silver.generation_snapshot",
    source="generation_parsed",
    keys=["asset_id", "snapshot_utc"],
    sequence_by=F.col("_batch_ts"),
    stored_as_scd_type=1,
)
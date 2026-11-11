"""
Data quality monitoring for the Gold layer.

These tables don't answer business questions; they measure whether the tables that
do can be trusted. Each refreshes with every pipeline run (materialized views), so a
problem shows up in the pipeline UI as an expectation result instead of being found
later in a wrong answer.

One summary table per Gold table, one row per refresh:
  dq_fact_hourly_market      -> gold.fact_hourly_market
  dq_price_spike_events      -> gold.agg_price_spike_events
  dq_asset_coverage          -> gold.dim_asset_current (+ registry vs generation coverage)
Plus one detail table:
  dq_unmatched_generation_assets -> which generation assets are missing from the registry

Severity follows ADR-008:
  FAIL  = downstream answers would be wrong (duplicates, lost rows, broken reconciliation)
  WARN  = suspicious but possibly real; keep visible (missing load, gaps, coverage)
"""

import dlt
from pyspark.sql import functions as F

# =====================================================================
# Thresholds (warn levels). Set just below the measured normal values,
# so warnings fire when quality gets WORSE, not on the first run.
# =====================================================================
MAX_MISSING_LOAD_PCT = 5.0        # % of price hours allowed without a load value
MIN_GENERATION_MATCH_PCT = 95.0   # % of generation-feed assets that must exist in the registry


def hours_between(start_col, end_col):
    """Whole hours between two timestamp columns."""
    return ((F.unix_timestamp(end_col) - F.unix_timestamp(start_col)) / 3600).cast("long")


def current_asset_ids():
    return (
        spark.read.table("silver.dim_asset")
        .filter(F.col("__END_AT").isNull())
        .select("asset_id")
    )


def generation_asset_ids():
    return spark.read.table("silver.generation_snapshot").select("asset_id").distinct()


# =====================================================================
# 1. fact_hourly_market
# =====================================================================

@dlt.table(
    name="gold.dq_fact_hourly_market",
    comment=(
        "Data quality for gold.fact_hourly_market: uniqueness, reconciliation with Silver, "
        "missing load, gaps in the hourly sequence, and local time vs AESO's own local time. "
        "One row per pipeline refresh."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("no_duplicate_hours", "duplicate_hours = 0")
@dlt.expect_or_fail("all_silver_price_hours_present", "fact_rows = silver_price_hours")
@dlt.expect("load_mostly_present", f"missing_load_pct <= {MAX_MISSING_LOAD_PCT}")
@dlt.expect("no_gaps_in_hourly_sequence", "missing_hours_in_range = 0")
# A mismatch means the platform's time zone rules differ from AESO's (ADR-007)
@dlt.expect("local_time_matches_aeso", "local_time_mismatches = 0")
def dq_fact_hourly_market():
    fact = spark.read.table("gold.fact_hourly_market")
    silver_price = spark.read.table("silver.pool_price").select(
        "begin_datetime_utc", "begin_datetime_mpt"
    )

    fact_stats = fact.agg(
        F.count("*").alias("fact_rows"),
        F.countDistinct("begin_datetime_utc").alias("distinct_hours"),
        F.min("begin_datetime_utc").alias("first_hour_utc"),
        F.max("begin_datetime_utc").alias("last_hour_utc"),
        F.sum(F.when(F.col("alberta_internal_load").isNull(), 1).otherwise(0)).alias("missing_load_hours"),
        F.sum(F.when(F.col("is_spike"), 1).otherwise(0)).alias("spike_hours"),
    )

    silver_stats = silver_price.agg(F.count("*").alias("silver_price_hours"))

    # Gold derives local time from UTC; Silver holds AESO's own local time. They must agree.
    local_check = (
        fact.select("begin_datetime_utc", "begin_datetime_local")
        .join(silver_price, on="begin_datetime_utc", how="inner")
        .agg(
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("begin_datetime_mpt").isNotNull()
                        & (F.col("begin_datetime_local").cast("timestamp_ntz") != F.col("begin_datetime_mpt")),
                        1,
                    ).otherwise(0)
                ),
                F.lit(0),
            ).alias("local_time_mismatches")
        )
    )

    return (
        fact_stats.crossJoin(silver_stats).crossJoin(local_check)
        .withColumn("duplicate_hours", F.col("fact_rows") - F.col("distinct_hours"))
        .withColumn("missing_load_hours", F.coalesce("missing_load_hours", F.lit(0)))
        .withColumn("spike_hours", F.coalesce("spike_hours", F.lit(0)))
        .withColumn(
            "missing_load_pct",
            F.coalesce(F.round(100 * F.col("missing_load_hours") / F.col("fact_rows"), 2), F.lit(0.0)),
        )
        .withColumn(
            "missing_hours_in_range",
            F.coalesce(
                hours_between(F.col("first_hour_utc"), F.col("last_hour_utc")) + 1 - F.col("distinct_hours"),
                F.lit(0),
            ),
        )
        .withColumn("measured_at_utc", F.current_timestamp())
        .select(
            "measured_at_utc", "first_hour_utc", "last_hour_utc",
            "fact_rows", "distinct_hours", "duplicate_hours", "silver_price_hours",
            "missing_hours_in_range", "missing_load_hours", "missing_load_pct",
            "local_time_mismatches", "spike_hours",
        )
    )


# =====================================================================
# 2. agg_price_spike_events
# =====================================================================

@dlt.table(
    name="gold.dq_price_spike_events",
    comment=(
        "Data quality for gold.agg_price_spike_events: every spike hour belongs to exactly one "
        "event, event identifiers are unique, and each event's time span matches its duration. "
        "One row per pipeline refresh."
    ),
    table_properties={"quality": "gold"},
)
# Reconciliation: spike hours counted in events == spike hours flagged in fact_hourly_market
@dlt.expect_or_fail("events_cover_all_spike_hours", "event_hours = fact_spike_hours")
@dlt.expect_or_fail("event_start_is_unique", "duplicate_event_starts = 0")
# Events are runs of CONSECUTIVE hours, so end - start must equal the duration exactly
@dlt.expect_or_fail("event_span_matches_duration", "span_mismatches = 0")
@dlt.expect_or_fail("events_well_formed", "invalid_events = 0")
def dq_price_spike_events():
    events = spark.read.table("gold.agg_price_spike_events")
    fact = spark.read.table("gold.fact_hourly_market")

    event_stats = events.agg(
        F.count("*").alias("events"),
        F.countDistinct("event_start_utc").alias("distinct_event_starts"),
        F.coalesce(F.sum("duration_hours"), F.lit(0)).alias("event_hours"),
        F.coalesce(
            F.sum(
                F.when(
                    hours_between(F.col("event_start_utc"), F.col("event_end_utc")) != F.col("duration_hours"), 1
                ).otherwise(0)
            ),
            F.lit(0),
        ).alias("span_mismatches"),
        F.coalesce(
            F.sum(
                F.when(
                    (F.col("duration_hours") < 1)
                    | (F.col("event_end_utc") <= F.col("event_start_utc"))
                    | F.col("peak_price").isNull()
                    | (F.col("avg_price") > F.col("peak_price")),
                    1,
                ).otherwise(0)
            ),
            F.lit(0),
        ).alias("invalid_events"),
        F.max("duration_hours").alias("longest_event_hours"),
        F.max("peak_price").alias("highest_peak_price"),
    )

    fact_stats = fact.agg(
        F.coalesce(F.sum(F.when(F.col("is_spike"), 1).otherwise(0)), F.lit(0)).alias("fact_spike_hours")
    )

    return (
        event_stats.crossJoin(fact_stats)
        .withColumn("duplicate_event_starts", F.col("events") - F.col("distinct_event_starts"))
        .withColumn("measured_at_utc", F.current_timestamp())
        .select(
            "measured_at_utc", "events", "duplicate_event_starts",
            "event_hours", "fact_spike_hours",
            "span_mismatches", "invalid_events",
            "longest_event_hours", "highest_peak_price",
        )
    )


# =====================================================================
# 3. dim_asset_current + coverage between registry and generation feed
#    Coverage is measured from BOTH directions:
#      generation side: of the assets AESO reports generation for, how many exist in
#        the registry? Low = asset codes don't match; must be fixed before
#        agg_generator_performance.
#      registry side: of the active SOURCE assets, how many appear in the generation
#        feed? Low can be normal: the registry is broader than the generation report.
# =====================================================================

@dlt.table(
    name="gold.dq_asset_coverage",
    comment=(
        "Data quality for gold.dim_asset_current (uniqueness, reconciliation with Silver, "
        "participant arrays) and coverage between silver.dim_asset and "
        "silver.generation_snapshot, measured from both directions. One row per pipeline refresh."
    ),
    table_properties={"quality": "gold"},
)
# A duplicated asset would double-count generation in every downstream table
@dlt.expect_or_fail("dim_asset_current_is_unique", "duplicate_asset_rows = 0")
@dlt.expect_or_fail("all_current_assets_present", "dim_rows = silver_current_assets")
@dlt.expect_or_fail("participant_count_matches_ids", "participant_count_mismatches = 0")
# Fewer names than ids = a participant id with no current row in dim_participant
@dlt.expect("every_participant_has_a_name", "participants_without_name = 0")
@dlt.expect("generation_assets_match_registry", f"generation_match_pct >= {MIN_GENERATION_MATCH_PCT}")
def dq_asset_coverage():
    dim = spark.read.table("gold.dim_asset_current")
    registry = current_asset_ids()
    generation = generation_asset_ids()

    dim_stats = dim.agg(
        F.count("*").alias("dim_rows"),
        F.countDistinct("asset_id").alias("distinct_assets"),
        F.coalesce(
            F.sum(F.when(F.col("participant_count") != F.size("pool_participant_ids"), 1).otherwise(0)),
            F.lit(0),
        ).alias("participant_count_mismatches"),
        F.coalesce(
            F.sum(F.size("pool_participant_ids") - F.size("pool_participant_names")), F.lit(0)
        ).alias("participants_without_name"),
    )

    silver_stats = registry.agg(F.count("*").alias("silver_current_assets"))

    gen_side = (
        generation.join(registry.withColumn("in_registry", F.lit(True)), on="asset_id", how="left")
        .agg(
            F.count("*").alias("generation_assets"),
            F.count("in_registry").alias("generation_assets_matched"),
        )
    )

    active_sources = (
        spark.read.table("silver.dim_asset")
        .filter(
            F.col("__END_AT").isNull()
            & (F.lower("operating_status") == "active")
            & (F.col("asset_type") == "SOURCE")
        )
        .select("asset_id")
    )
    registry_side = (
        active_sources.join(generation.withColumn("in_generation", F.lit(True)), on="asset_id", how="left")
        .agg(
            F.count("*").alias("active_source_assets"),
            F.count("in_generation").alias("active_sources_with_generation"),
        )
    )

    return (
        dim_stats.crossJoin(silver_stats).crossJoin(gen_side).crossJoin(registry_side)
        .withColumn("duplicate_asset_rows", F.col("dim_rows") - F.col("distinct_assets"))
        .withColumn(
            "generation_assets_unmatched",
            F.col("generation_assets") - F.col("generation_assets_matched"),
        )
        .withColumn(
            "generation_match_pct",
            # No generation data at all -> 0%, not NULL, so the expectation still evaluates
            F.coalesce(
                F.round(100 * F.col("generation_assets_matched") / F.col("generation_assets"), 1),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "active_source_coverage_pct",
            F.round(100 * F.col("active_sources_with_generation") / F.col("active_source_assets"), 1),
        )
        .withColumn("measured_at_utc", F.current_timestamp())
        .select(
            "measured_at_utc",
            "dim_rows", "distinct_assets", "duplicate_asset_rows", "silver_current_assets",
            "participant_count_mismatches", "participants_without_name",
            "generation_assets", "generation_assets_matched", "generation_assets_unmatched",
            "generation_match_pct",
            "active_source_assets", "active_sources_with_generation", "active_source_coverage_pct",
        )
    )


# =====================================================================
# 4. Detail: generation-feed assets with no match in the registry
#    Empty is the healthy state. Anything here is invisible to asset-level analysis.
# =====================================================================

@dlt.table(
    name="gold.dq_unmatched_generation_assets",
    comment=(
        "Data quality: assets that appear in silver.generation_snapshot but have no current "
        "row in silver.dim_asset. Empty is the healthy state."
    ),
    table_properties={"quality": "gold"},
)
def dq_unmatched_generation_assets():
    return (
        spark.read.table("silver.generation_snapshot")
        .join(current_asset_ids(), on="asset_id", how="left_anti")
        .groupBy("asset_id")
        .agg(
            F.max("fuel_type").alias("fuel_type"),
            F.max("sub_fuel_type").alias("sub_fuel_type"),
            F.max("maximum_capability").alias("maximum_capability_mw"),
            F.min("snapshot_utc").alias("first_seen_utc"),
            F.max("snapshot_utc").alias("last_seen_utc"),
            F.count("*").alias("snapshot_rows"),
        )
    )
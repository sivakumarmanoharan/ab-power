"""
Gold spike logic: fact_hourly_market (ADR-015, ADR-016) and the gaps-and-islands
event derivation in agg_price_spike_events (ADR-017).

Test data uses July 2026 so results don't depend on the test machine's time zone
database (ADR-007): Alberta is UTC-6 then under both old and new rules.

Timestamps are compared as strings formatted by Spark in the session time zone (UTC).
PySpark's collect() converts timestamps to the *machine's* local time zone, so comparing
collected datetimes would pass on a UTC server and fail on a laptop in Calgary.
"""

from datetime import datetime, timedelta
import importlib

from pyspark.sql import functions as F  # type: ignore[reportMissingImports]

pytest = importlib.import_module("pytest")

from conftest import load_pipeline_module, values_df

BASE = datetime(2026, 7, 1, 0, 0)  # UTC
PRICE_SCHEMA = "begin_datetime_utc timestamp, pool_price double, rolling_30day_avg double"
LOAD_SCHEMA = "begin_datetime_utc timestamp, alberta_internal_load double, forecast_alberta_internal_load double"


def hour(n):
    return BASE + timedelta(hours=n)


def ts(n, offset_hours=0):
    """Expected timestamp for hour n as a UTC string, e.g. '2026-07-01 01:00:00'."""
    return (hour(n) + timedelta(hours=offset_hours)).strftime("%Y-%m-%d %H:%M:%S")


def collect_utc(df):
    """Collect rows with every timestamp column formatted by Spark (session time zone UTC),
    so results don't depend on the time zone of the machine running the tests."""
    formatted = [
        F.date_format(f.name, "yyyy-MM-dd HH:mm:ss").alias(f.name)
        if f.dataType.typeName() == "timestamp" else F.col(f.name)
        for f in df.schema.fields
    ]
    return df.select(formatted).collect()


@pytest.fixture
def gold(fake_spark):
    return load_pipeline_module("gold.py", fake_spark)


def register_hours(fake_spark, spark, prices, avg=50.0, missing_hours=(), missing_load=()):
    """prices: list of pool prices, one per consecutive UTC hour starting at BASE."""
    price_rows, load_rows = [], []
    for i, p in enumerate(prices):
        if i in missing_hours:
            continue
        price_rows.append((hour(i), float(p), avg))
        if i not in missing_load:
            load_rows.append((hour(i), 10000.0, 10050.0))
    fake_spark.register("silver.pool_price", values_df(spark, PRICE_SCHEMA, price_rows))
    fake_spark.register("silver.alberta_internal_load", values_df(spark, LOAD_SCHEMA, load_rows))


def events(fake_spark):
    return sorted(
        collect_utc(fake_spark.table("gold.agg_price_spike_events")),
        key=lambda r: r["event_start_utc"],
    )


# ---------------- fact_hourly_market ----------------

def test_spike_needs_both_relative_and_absolute_condition(gold, fake_spark, spark):
    # avg 50: 3x = 150. 160 passes both; 120 fails relative.
    # avg 20: 3x = 60. 90 passes relative but fails the $100 floor.
    rows = [(hour(0), 160.0, 50.0), (hour(1), 120.0, 50.0), (hour(2), 90.0, 20.0)]
    fake_spark.register("silver.pool_price", values_df(spark, PRICE_SCHEMA, rows))
    fake_spark.register("silver.alberta_internal_load", values_df(spark, LOAD_SCHEMA, []))
    flags = [r["is_spike"] for r in fake_spark.table("gold.fact_hourly_market").orderBy("begin_datetime_utc").collect()]
    assert flags == [True, False, False]


def test_missing_average_is_not_a_spike_and_never_null(gold, fake_spark, spark):
    fake_spark.register("silver.pool_price", values_df(spark, PRICE_SCHEMA, [(hour(0), 900.0, None)]))
    fake_spark.register("silver.alberta_internal_load", values_df(spark, LOAD_SCHEMA, []))
    assert fake_spark.table("gold.fact_hourly_market").collect()[0]["is_spike"] is False


def test_hour_without_load_is_kept(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [40, 40, 40], missing_load={1})
    fact = fake_spark.table("gold.fact_hourly_market")
    assert fact.count() == 3
    row = fact.filter(f"begin_datetime_utc = '{hour(1)}'").collect()[0]
    assert row["alberta_internal_load"] is None and row["forecast_error_mw"] is None


def test_forecast_error_is_actual_minus_forecast(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [40])
    assert fake_spark.table("gold.fact_hourly_market").collect()[0]["forecast_error_mw"] == -50.0


# ---------------- agg_price_spike_events ----------------

def test_consecutive_spike_hours_form_one_event(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [40, 300, 400, 350, 40])
    [e] = events(fake_spark)
    assert e["event_start_utc"] == ts(1)
    assert e["event_end_utc"] == ts(4)          # last spike hour starts at 3, ends at 4
    assert e["duration_hours"] == 3
    assert e["peak_price"] == 400.0
    assert e["avg_price"] == 350.0


def test_normal_hour_between_spikes_makes_two_events(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [300, 40, 300])
    assert [e["duration_hours"] for e in events(fake_spark)] == [1, 1]


def test_gap_in_data_splits_events(gold, fake_spark, spark):
    """Hour 2 is missing entirely: spikes at 1 and 3 must NOT merge into one event."""
    register_hours(fake_spark, spark, [40, 300, 300, 300, 40], missing_hours={2})
    result = events(fake_spark)
    assert [(e["event_start_utc"], e["duration_hours"]) for e in result] == [(ts(1), 1), (ts(3), 1)]


def test_event_crossing_midnight_utc_stays_one_event(gold, fake_spark, spark):
    prices = [40] * 22 + [300, 300, 300, 300] + [40]   # hours 22..25 cross midnight
    register_hours(fake_spark, spark, prices)
    [e] = events(fake_spark)
    assert e["duration_hours"] == 4
    assert e["event_start_utc"] == ts(22)


def test_spike_in_first_hour_starts_an_event(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [300, 40])
    [e] = events(fake_spark)
    assert e["event_start_utc"] == ts(0)


def test_no_spikes_gives_no_events(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [40, 45, 50])
    assert events(fake_spark) == []


def test_event_hours_reconcile_with_spike_hours(gold, fake_spark, spark):
    """Same reconciliation as dq_price_spike_events, as a unit test."""
    register_hours(fake_spark, spark, [300, 300, 40, 300, 40, 300, 300, 300], missing_hours={4})
    spike_hours = fake_spark.table("gold.fact_hourly_market").filter("is_spike").count()
    assert sum(e["duration_hours"] for e in events(fake_spark)) == spike_hours


def test_event_start_local_is_alberta_time(gold, fake_spark, spark):
    register_hours(fake_spark, spark, [40, 300])
    [e] = events(fake_spark)
    assert e["event_start_local"] == ts(1, offset_hours=-6)   # July: UTC-6
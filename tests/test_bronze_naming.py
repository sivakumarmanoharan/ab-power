"""Bronze column-name normalization (ADR-005) and configuration fallback (ADR-020)."""

import importlib


pytest = importlib.import_module("pytest")
_spark_types = importlib.import_module("pyspark.sql.types")
ArrayType = _spark_types.ArrayType
MapType = _spark_types.MapType
StringType = _spark_types.StringType
StructField = _spark_types.StructField
StructType = _spark_types.StructType

from conftest import FakeSpark, load_pipeline_module


@pytest.fixture
def bronze(spark):
    return load_pipeline_module("bronze.py", FakeSpark(spark))


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Actual Forecast Report", "actual_forecast_report"),
        ("asset_ID", "asset_id"),
        ("  Pool Price Report ", "pool_price_report"),
        ("a,b;c{d}e(f)g=h", "a_b_c_d_e_f_g_h"),
        ("_rescued_data", "_rescued_data"),          # leading underscore kept
        ("already_clean", "already_clean"),
    ],
)
def test_clean_name(bronze, raw, expected):
    assert bronze.clean_name(raw) == expected


def test_clean_type_renames_nested_fields(bronze):
    raw = StructType([
        StructField("Actual Forecast Report", ArrayType(StructType([
            StructField("begin datetime utc", StringType()),
            StructField("asset_ID", StringType()),
        ]))),
        StructField("Lookup", MapType(StringType(), StructType([StructField("Max Cap", StringType())]))),
    ])
    cleaned = bronze.clean_type(raw)
    report = cleaned["actual_forecast_report"].dataType.elementType
    assert report.fieldNames() == ["begin_datetime_utc", "asset_id"]
    assert cleaned["lookup"].dataType.valueType.fieldNames() == ["max_cap"]


def test_clean_columns_keeps_data_and_renames(bronze, spark):
    # Built with SQL rather than createDataFrame, so no Python worker is started
    df = spark.sql("SELECT named_struct('Begin Datetime UTC', '2026-11-01 00:00') AS `return`")
    out = bronze.clean_columns(df)
    assert out.schema["return"].dataType.fieldNames() == ["begin_datetime_utc"]
    assert out.collect()[0]["return"]["begin_datetime_utc"] == "2026-11-01 00:00"


def test_landing_falls_back_to_prod_when_conf_unset(spark):
    """On serverless, reading an unset key raises; the code must fall back (ADR-020)."""
    module = load_pipeline_module("bronze.py", FakeSpark(spark, conf={}))
    assert module.LANDING == "/Volumes/ab_power/landing/raw_files"


def test_landing_uses_pipeline_conf_when_set(spark):
    module = load_pipeline_module(
        "bronze.py", FakeSpark(spark, conf={"landing_root": "/Volumes/ab_power_dev/landing/raw_files"})
    )
    assert module.LANDING == "/Volumes/ab_power_dev/landing/raw_files"
"""
Shared test setup.

Pipeline files import `dlt` and use a global `spark`, both of which only exist inside a
Lakeflow pipeline. These fixtures provide stand-ins so the pipeline code can be imported
and its table functions run locally against small, hand-built DataFrames:

  - a stub `dlt` module whose decorators register each table function by name;
  - a local SparkSession (session timezone UTC, as in the real pipeline);
  - FakeSpark: behaves like `spark`, except `spark.read.table(name)` returns test data
    registered for that name, or the output of another registered table function.
"""

import importlib.util
import os
import sys
import types
from datetime import datetime
from pathlib import Path

# Make Spark's worker processes use the same Python as pytest (the virtual environment).
# Without this, Windows workers pick up whichever python.exe is first on PATH.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

pytest = importlib.import_module("pytest")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingestion"))


# ---------------------------------------------------------------------------
# Stub dlt module: records table functions, ignores expectations
# ---------------------------------------------------------------------------
TABLE_FUNCTIONS = {}


def _table(name=None, **_kwargs):
    def register(func):
        TABLE_FUNCTIONS[name or func.__name__] = func
        return func
    return register


def _passthrough(*_args, **_kwargs):
    return lambda func: func


dlt_stub = types.ModuleType("dlt")
dlt_stub.table = _table
dlt_stub.view = _table
dlt_stub.expect = dlt_stub.expect_or_drop = dlt_stub.expect_or_fail = _passthrough
dlt_stub.create_streaming_table = lambda *a, **k: None
dlt_stub.create_auto_cdc_flow = lambda *a, **k: None
dlt_stub.create_auto_cdc_from_snapshot_flow = lambda *a, **k: None
sys.modules["dlt"] = dlt_stub


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession  # type: ignore[reportMissingImports]

    session = (
        SparkSession.builder.master("local[1]")
        .appName("ab-power-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # If a Python worker ever crashes, print its real Python traceback
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


class _FakeReader:
    def __init__(self, fake):
        self._fake = fake

    def table(self, name):
        return self._fake.table(name)


class _FakeConf:
    """Serverless behaviour: reading an unset key raises, even with a default."""

    def __init__(self, values):
        self._values = values

    def get(self, key, *_default):
        if key in self._values:
            return self._values[key]
        raise Exception(f"[CONFIG_NOT_AVAILABLE] {key}")


class FakeSpark:
    def __init__(self, real_spark, conf=None):
        self._spark = real_spark
        self._tables = {}
        self._cache = {}
        self.read = _FakeReader(self)
        self.conf = _FakeConf(conf or {})

    def register(self, name, df):
        self._tables[name] = df

    def table(self, name):
        if name in self._tables:
            return self._tables[name]
        if name not in self._cache:
            if name not in TABLE_FUNCTIONS:
                raise KeyError(f"No test data or table function registered for '{name}'")
            self._cache[name] = TABLE_FUNCTIONS[name]()
        return self._cache[name]

    def __getattr__(self, attr):
        return getattr(self._spark, attr)


def load_pipeline_module(filename, fake_spark):
    """Import a file from src/pipelines with `spark` set to the given FakeSpark."""
    path = ROOT / "src" / "pipelines" / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    module.spark = fake_spark
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fake_spark(spark):
    TABLE_FUNCTIONS.clear()
    return FakeSpark(spark)


# ---------------------------------------------------------------------------
# Test data built inside the JVM (SQL VALUES), not from Python objects.
# spark.createDataFrame(<python list>) runs Python worker processes, which are
# fragile on Windows. SQL literals never start a Python worker, so tests behave
# the same on Windows, macOS, Linux and CI.
# ---------------------------------------------------------------------------
def _literal(value, sql_type):
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return f"CAST({value!r} AS {sql_type})"
    escaped = str(value).replace("'", "\\'")
    return f"'{escaped}'"


def values_df(spark, schema, rows):
    """Build a DataFrame from rows using SQL only.

    schema: "col_a timestamp, col_b double"   rows: list of tuples in that order.
    """
    columns = [c.strip().split(None, 1) for c in schema.split(",")]
    names = [n for n, _ in columns]
    if not rows:
        select = ", ".join(f"CAST(NULL AS {t}) AS {n}" for n, t in columns)
        return spark.sql(f"SELECT {select} WHERE 1 = 0")
    tuples = ", ".join(
        "(" + ", ".join(_literal(v, t) for v, (_, t) in zip(row, columns)) + ")" for row in rows
    )
    casts = ", ".join(f"CAST({n} AS {t}) AS {n}" for n, t in columns)
    return spark.sql(f"SELECT {casts} FROM VALUES {tuples} AS t({', '.join(names)})")
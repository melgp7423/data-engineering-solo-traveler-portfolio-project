import json
import sys
import types

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)


def _stub_module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module


_stub_module("awsglue")
_stub_module("awsglue.transforms")
_stub_module("awsglue.utils", getResolvedOptions=lambda *_a, **_k: {})
_stub_module("awsglue.context", GlueContext=object)
_stub_module("awsglue.job", Job=object)

from load.load_gold_layer import handler
from load.load_gold_layer.handler import (
    CLUSTER_PARAMS,
    TARGET_TABLE,
    boolean_column_types,
    optional_arg,
    read_latest_partition,
    route_destinations,
    to_jdbc_rows,
    write_cluster,
)

SCHEMA = (
    "city string, country string, "
    "activity_examples array<struct<name:string,category:string,longitude:double,latitude:double>>, "
    "activity_categories array<string>"
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-load-gold-layer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def activity(lon, lat, name="Spot"):
    return (name, "tourism", lon, lat)


@pytest.fixture(scope="module")
def routed(spark):
    rows = [
        ("seattle", "US", [activity(-122.33, 47.61)], ["tourism"]),
        ("miami", "US", [activity(-80.19, 25.76)], ["tourism"]),
        ("vancouver", "CA", [activity(-123.12, 49.28)], ["tourism"]),
        ("toronto", "CA", [activity(-79.38, 43.65)], ["tourism"]),
        ("meridian", "US", [activity(-100.0, 40.0)], ["tourism"]),
        ("monterrey", "MX", [activity(-100.31, 25.69)], ["tourism"]),
        ("cancun", "MX", [activity(-86.85, 21.16)], ["tourism"]),
        (
            "cabo san lucas",
            "MX",
            [activity(-109.91, 22.89), activity(-110.0, 30.0)],
            ["tourism"],
        ),
        ("denver", " us ", [activity(-104.99, 39.74)], ["tourism"]),
        ("no coordinates", "US", [], []),
        ("null coordinates", "MX", [activity(None, None)], ["tourism"]),
        ("paris", "FR", [activity(2.35, 48.86)], ["tourism"]),
    ]
    df = route_destinations(spark.createDataFrame(rows, SCHEMA))
    return {r.city: r for r in df.collect()}


@pytest.mark.parametrize(
    "city, cluster",
    [
        ("seattle", "us_ca_west"),
        ("vancouver", "us_ca_west"),
        ("denver", "us_ca_west"),
        ("miami", "us_ca_east"),
        ("toronto", "us_ca_east"),
        ("meridian", "us_ca_east"),
        ("monterrey", "mx_north"),
        ("cancun", "mx_south"),
        ("cabo san lucas", "mx_south"),
        ("no coordinates", None),
        ("null coordinates", None),
        ("paris", None),
    ],
)
def test_cluster(routed, city, cluster):
    assert routed[city].cluster == cluster


def test_coordinates_come_from_first_activity(routed):
    assert routed["cabo san lucas"].longitude == -109.91
    assert routed["cabo san lucas"].latitude == 22.89
    assert routed["no coordinates"].longitude is None


def test_to_jdbc_rows_serializes_nested_columns(spark):
    df = spark.createDataFrame(
        [("seattle", "US", [activity(-122.33, 47.61, "Space Needle")], ["tourism"])],
        SCHEMA,
    )
    row = to_jdbc_rows(df).first()
    assert json.loads(row.activity_examples) == [
        {
            "name": "Space Needle",
            "category": "tourism",
            "longitude": -122.33,
            "latitude": 47.61,
        }
    ]
    assert json.loads(row.activity_categories) == ["tourism"]


def test_optional_arg_resolves_when_present(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["job", "--COMBINED_DATE", "2026-10-05"])
    monkeypatch.setattr(
        handler,
        "getResolvedOptions",
        lambda argv, names: {names[0]: argv[argv.index(f"--{names[0]}") + 1]},
    )
    assert optional_arg("COMBINED_DATE") == "2026-10-05"


def test_optional_arg_is_none_when_absent(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["job", "--JOB_NAME", "load"])

    def fail(*_a, **_k):
        raise AssertionError("getResolvedOptions should not be called")

    monkeypatch.setattr(handler, "getResolvedOptions", fail)
    assert optional_arg("COMBINED_DATE") is None


def test_boolean_column_types_lists_only_boolean_columns(spark):
    df = spark.createDataFrame(
        [("seattle", True, 1, False)],
        "city string, is_beach boolean, rank int, is_ski boolean",
    )
    assert boolean_column_types(df) == "is_beach BOOLEAN, is_ski BOOLEAN"


def test_boolean_column_types_is_empty_without_booleans(spark):
    df = spark.createDataFrame([("seattle",)], "city string")
    assert boolean_column_types(df) == ""


class FakeJdbcWriter:

    def __init__(self):
        self.format_name = None
        self.options = {}
        self.save_mode = None
        self.saved = False

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def mode(self, mode):
        self.save_mode = mode
        return self

    def save(self):
        self.saved = True


def fake_frame(spark, ddl):
    return types.SimpleNamespace(
        schema=spark.createDataFrame([], ddl).schema, write=FakeJdbcWriter()
    )


def test_write_cluster_overwrites_dsql_table(spark):
    df = fake_frame(spark, "city string, is_beach boolean")

    write_cluster(df, "west.dsql.aws", "software.amazon.dsql.Driver", "travel", "admin")

    writer = df.write
    assert writer.format_name == "jdbc"
    assert writer.options == {
        "createTableColumnTypes": "is_beach BOOLEAN",
        "url": "jdbc:aws-dsql:postgresql://west.dsql.aws/travel",
        "driver": "software.amazon.dsql.Driver",
        "dbtable": TARGET_TABLE,
        "user": "admin",
        "isolationLevel": "REPEATABLE_READ",
    }
    assert writer.save_mode == "overwrite"
    assert writer.saved


def test_write_cluster_skips_column_types_without_booleans(spark):
    df = fake_frame(spark, "city string")
    write_cluster(df, "west.dsql.aws", "driver", "travel", "admin")
    assert "createTableColumnTypes" not in df.write.options
    assert df.write.saved


class FakeGlue:
    def __init__(self, columns, partitions):
        self._columns = columns
        self._partitions = partitions
        self.calls = []

    def get_table(self, DatabaseName, Name):
        self.calls.append(("get_table", DatabaseName, Name))
        return {"Table": {"StorageDescriptor": {"Columns": self._columns}}}

    def get_paginator(self, name):
        assert name == "get_partitions"
        fake = self

        class Paginator:
            def paginate(self, DatabaseName, TableName):
                fake.calls.append(("get_partitions", DatabaseName, TableName))
                # Split across pages to make sure every page is read.
                for p in fake._partitions:
                    yield {"Partitions": [p]}

        return Paginator()


COLUMNS = [{"Name": "city", "Type": "string"}, {"Name": "country", "Type": "string"}]


def partition(value, location):
    return {"Values": [value], "StorageDescriptor": {"Location": str(location)}}


@pytest.fixture
def combined_partitions(spark, tmp_path):
    locations = {}
    for value, city in [("2026-10-04", "old"), ("2026-10-05", "new")]:
        location = tmp_path / f"combined_date={value}"
        spark.createDataFrame(
            [(city, "US", "dropped")], "city string, country string, extra string"
        ).write.parquet(str(location))
        locations[value] = location
    return locations


def catalog(locations):
    return FakeGlue(COLUMNS, [partition(v, loc) for v, loc in locations.items()])


def use_glue(monkeypatch, glue):
    monkeypatch.setattr(handler.boto3, "client", lambda name: glue)


def test_read_latest_partition_reads_newest_partition_with_catalog_schema(
    spark, monkeypatch, combined_partitions
):
    glue = catalog(combined_partitions)
    use_glue(monkeypatch, glue)

    df = read_latest_partition(spark, "travel_db", "combined")

    assert df.columns == ["city", "country", "combined_date"]
    assert [tuple(r) for r in df.collect()] == [("new", "US", "2026-10-05")]
    assert glue.calls == [
        ("get_table", "travel_db", "combined"),
        ("get_partitions", "travel_db", "combined"),
    ]


def test_read_latest_partition_reads_requested_date(
    spark, monkeypatch, combined_partitions
):
    use_glue(monkeypatch, catalog(combined_partitions))
    df = read_latest_partition(spark, "travel_db", "combined", "2026-10-04")
    assert [tuple(r) for r in df.collect()] == [("old", "US", "2026-10-04")]


def test_read_latest_partition_rejects_unknown_date(
    spark, monkeypatch, combined_partitions
):
    use_glue(monkeypatch, catalog(combined_partitions))
    with pytest.raises(ValueError, match="No combined_date=2026-10-01 partition"):
        read_latest_partition(spark, "travel_db", "combined", "2026-10-01")


def test_read_latest_partition_rejects_table_without_partitions(spark, monkeypatch):
    use_glue(monkeypatch, FakeGlue(COLUMNS, []))
    with pytest.raises(ValueError, match="travel_db.combined has no partitions"):
        read_latest_partition(spark, "travel_db", "combined")


ARGS = {
    "JOB_NAME": "load-gold-layer",
    "CATALOG_DATABASE": "travel_db",
    "CATALOG_TABLE": "combined",
    "DSQL_DRIVER": "driver",
    "DSQL_DATABASE": "travel",
    "DSQL_USER": "admin",
    **{param: f"{cluster}.dsql.aws" for cluster, param in CLUSTER_PARAMS.items()},
}


def test_main_writes_each_cluster_and_skips_empty_ones(spark, monkeypatch):
    rows = [
        ("seattle", "US", [activity(-122.33, 47.61)], ["tourism"]),
        ("portland", "US", [activity(-122.68, 45.52)], ["tourism"]),
        ("cancun", "MX", [activity(-86.85, 21.16)], ["tourism"]),
        ("paris", "FR", [activity(2.35, 48.86)], ["tourism"]),
    ]
    combined = spark.createDataFrame(rows, SCHEMA)
    reads, writes, jobs = [], {}, []

    class FakeJob:
        def __init__(self, glue_context):
            jobs.append(self)
            self.committed = False

        def init(self, name, args):
            self.name = name

        def commit(self):
            self.committed = True

    def fake_read(spark_session, database, table, combined_date):
        reads.append((database, table, combined_date))
        return combined

    def fake_write(df, endpoint, driver, database, user):
        writes[endpoint] = (df.collect(), df.columns, driver, database, user)

    monkeypatch.setattr(sys, "argv", ["job"])
    monkeypatch.setattr(handler, "getResolvedOptions", lambda *_a: ARGS)
    monkeypatch.setattr(handler, "SparkContext", lambda: None)
    monkeypatch.setattr(
        handler, "GlueContext", lambda sc: types.SimpleNamespace(spark_session=spark)
    )
    monkeypatch.setattr(handler, "Job", FakeJob)
    monkeypatch.setattr(handler, "read_latest_partition", fake_read)
    monkeypatch.setattr(handler, "write_cluster", fake_write)

    handler.main()

    assert reads == [("travel_db", "combined", None)]
    assert set(writes) == {"us_ca_west.dsql.aws", "mx_south.dsql.aws"}

    west_rows, west_columns, driver, database, user = writes["us_ca_west.dsql.aws"]
    assert sorted(r.city for r in west_rows) == ["portland", "seattle"]
    assert "cluster" not in west_columns
    assert (driver, database, user) == ("driver", "travel", "admin")
    assert isinstance(west_rows[0].activity_examples, str)

    mx_rows = writes["mx_south.dsql.aws"][0]
    assert [r.city for r in mx_rows] == ["cancun"]

    assert jobs[0].name == "load-gold-layer"
    assert jobs[0].committed

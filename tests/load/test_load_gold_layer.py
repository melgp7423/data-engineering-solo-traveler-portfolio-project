import json
import sys
import types

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

# boto3 ships with the Glue runtime but isn't in requirements.txt, so CI may
# not have it. The routing functions never call it, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)


# awsglue only exists inside the Glue runtime; the routing functions only use
# plain Spark DataFrames.
def _stub_module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module


_stub_module("awsglue")
_stub_module("awsglue.transforms")
_stub_module("awsglue.utils", getResolvedOptions=lambda *_a, **_k: {})
_stub_module("awsglue.context", GlueContext=object)
_stub_module("awsglue.job", Job=object)

from load.load_gold_layer.handler import (  # noqa: E402
    route_destinations,
    to_jdbc_rows,
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
        # Exactly on the 100th meridian counts as east.
        ("meridian", "US", [activity(-100.0, 40.0)], ["tourism"]),
        ("monterrey", "MX", [activity(-100.31, 25.69)], ["tourism"]),
        ("cancun", "MX", [activity(-86.85, 21.16)], ["tourism"]),
        # Only the first struct is used: the second would put Cabo north.
        (
            "cabo san lucas",
            "MX",
            [activity(-109.91, 22.89), activity(-110.0, 30.0)],
            ["tourism"],
        ),
        # Lowercase / padded country codes still route.
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

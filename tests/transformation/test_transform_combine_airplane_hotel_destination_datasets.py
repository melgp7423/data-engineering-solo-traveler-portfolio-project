import sys
import types
from datetime import date

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import types as T

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)


class FakeDynamicFrame:
    def __init__(self, df):
        self._df = df

    @classmethod
    def fromDF(cls, df, _glue_context, _name):
        return cls(df)

    def toDF(self):
        return self._df


class FakeDynamicFrameCollection:
    def __init__(self, frames, _glue_context):
        self._frames = frames

    def keys(self):
        return self._frames.keys()

    def select(self, key):
        return self._frames[key]


def _get_resolved_options(argv, names):
    return {name: argv[argv.index(f"--{name}") + 1] for name in names}


def _stub_module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module


_stub_module("awsglue")
_stub_module("awsglue.transforms", SelectFromCollection=object)
_stub_module("awsglue.utils", getResolvedOptions=_get_resolved_options)
_stub_module("awsglue.context", GlueContext=object)
_stub_module("awsglue.job", Job=object)
_stub_module(
    "awsglue.dynamicframe",
    DynamicFrame=FakeDynamicFrame,
    DynamicFrameCollection=FakeDynamicFrameCollection,
)

from transformation.transform_combine_airplane_hotel_destination_datasets.handler import (  # noqa: E402
    MyTransform,
)

EXTRACT_DATE = "2026-10-16"
OLD_EXTRACT_DATE = "2026-10-09"

FX_SCHEMA = "base_currency string, currency string, rate double, rate_date string"
FLIGHTS_SCHEMA = (
    "name string, price double, flight_duration int, outbound_date string, "
    "return_date string, extract_date string"
)
HOTELS_SCHEMA = (
    "search_parameters_q string, search_parameters_currency string, "
    "total_rate_extracted_lowest double, search_parameters_check_in_date string, "
    "search_parameters_check_out_date string, overall_rating double, "
    "extracted_hotel_class int, name string, extract_date string"
)
ACTIVITIES_SCHEMA = T.StructType(
    [
        T.StructField("destination", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("formatted", T.StringType()),
        T.StructField("categories", T.ArrayType(T.StringType())),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("country_code", T.StringType()),
        T.StructField("extract_date", T.StringType()),
    ]
)

ARTWORK = ["tourism", "tourism.attraction", "tourism.attraction.artwork"]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-combine-datasets")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def plain_argv(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["handler.py"])


def fx_rows():
    return [
        ("USD", "CAD", 1.25, "2026-10-15"),
        ("USD", "CAD", 1.40, "2026-10-01"),
        ("USD", "MXN", 20.0, "2026-10-15"),
    ]


def flight_rows():
    return [
        ("Washington, D.C.", 250.0, 300, "2026-10-23", "2026-10-30", EXTRACT_DATE),
        ("Washington, D.C.", 200.0, 320, "2026-10-24", "2026-10-31", EXTRACT_DATE),
        ("Vancouver", 400.0, 180, "2026-10-23", "2026-10-30", EXTRACT_DATE),
        ("Page", 300.0, 90, "2026-10-23", "2026-10-30", EXTRACT_DATE),
        ("Austin", 180.0, 150, "2026-10-23", "2026-10-30", EXTRACT_DATE),
        ("Boise", 220.0, 110, "2026-10-23", "2026-10-30", EXTRACT_DATE),
        ("Denver", 99.0, 120, "2026-10-16", "2026-10-23", OLD_EXTRACT_DATE),
    ]


def hotel_rows():
    return [
        (
            "Washington, D.C., United States",
            "USD",
            700.0,
            "2026-10-23",
            "2026-10-30",
            4.5,
            4,
            "Capitol Inn",
            EXTRACT_DATE,
        ),
        (
            "Washington, D.C., United States",
            "USD",
            1400.0,
            "2026-10-23",
            "2026-10-30",
            4.8,
            5,
            "Monument Hotel",
            EXTRACT_DATE,
        ),
        (
            "Vancouver, Canada",
            "CAD",
            1000.0,
            "2026-10-23",
            "2026-10-27",
            4.2,
            4,
            "Harbour Hotel",
            EXTRACT_DATE,
        ),
        (
            "Austin, United States",
            "USD",
            600.0,
            "2026-10-23",
            "2026-10-30",
            4.0,
            3,
            "Congress Lodge",
            EXTRACT_DATE,
        ),
        (
            "Denver, United States",
            "USD",
            500.0,
            "2026-10-16",
            "2026-10-23",
            4.1,
            3,
            "Old Extract Inn",
            OLD_EXTRACT_DATE,
        ),
    ]


def activity_rows():
    dc = "washington_d_c_united_states"
    return [
        (
            dc,
            "Lincoln Memorial",
            "Lincoln Memorial, 2 Lincoln Memorial Circle Northwest, Washington, DC",
            ["access", "access.yes", "tourism", "tourism.sights"],
            -77.05,
            38.89,
            "us",
            EXTRACT_DATE,
        ),
        # Same place found twice: counted once.
        (
            dc,
            "Lincoln Memorial",
            "Lincoln Memorial, Washington, DC",
            ["tourism"],
            -77.05,
            38.89,
            "us",
            EXTRACT_DATE,
        ),
        (
            dc,
            "Gypsy",
            "Gypsy, North Carolina Avenue Northeast, Washington, DC",
            ARTWORK,
            -76.98,
            38.89,
            "us",
            EXTRACT_DATE,
        ),
        # Not real place names: dropped.
        (
            dc,
            "1285 Elmwood Avenue",
            "1285 Elmwood Avenue, Washington, DC",
            ARTWORK,
            -77.0,
            38.9,
            "us",
            EXTRACT_DATE,
        ),
        (
            dc,
            "Holbrook Street, Washington, DC",
            "Holbrook Street, Washington, DC",
            ARTWORK,
            -77.0,
            38.9,
            "us",
            EXTRACT_DATE,
        ),
        (
            dc,
            "Quincy Street",
            "Quincy Street, Washington, DC",
            ARTWORK,
            -77.0,
            38.9,
            "us",
            EXTRACT_DATE,
        ),
        (dc, "X", "X, Washington, DC", ARTWORK, -77.0, 38.9, "us", EXTRACT_DATE),
        (
            "vancouver_canada",
            "Stanley Park",
            "Stanley Park, Vancouver, BC",
            ["leisure", "leisure.park"],
            -123.14,
            49.30,
            "ca",
            EXTRACT_DATE,
        ),
        (
            "vancouver_canada",
            "Old Spot",
            "Old Spot, Vancouver, BC",
            ["tourism"],
            -123.1,
            49.2,
            "ca",
            OLD_EXTRACT_DATE,
        ),
        (
            "boise_united_states",
            "Boise Depot",
            "Boise Depot, Boise, ID",
            ["tourism"],
            -116.2,
            43.6,
            "us",
            EXTRACT_DATE,
        ),
        (
            "omaha_united_states",
            "Old Market",
            "Old Market, Omaha, NE",
            ["tourism"],
            -95.93,
            41.26,
            "us",
            EXTRACT_DATE,
        ),
        (
            "denver_united_states",
            "Red Rocks",
            "Red Rocks, Morrison, CO",
            ["tourism"],
            -105.2,
            39.66,
            "us",
            OLD_EXTRACT_DATE,
        ),
    ]


def run_transform(spark, flights=None, hotels=None, activities=None):
    frames = {
        "ExchangeRatesCatalog_node": spark.createDataFrame(fx_rows(), FX_SCHEMA),
        "AirplaneTicketRates_node": spark.createDataFrame(
            flight_rows() if flights is None else flights, FLIGHTS_SCHEMA
        ),
        "HotelRoomRates_node": spark.createDataFrame(
            hotel_rows() if hotels is None else hotels, HOTELS_SCHEMA
        ),
        "DestinationActivities_node": spark.createDataFrame(
            activity_rows() if activities is None else activities, ACTIVITIES_SCHEMA
        ),
    }
    dfc = FakeDynamicFrameCollection(
        {k: FakeDynamicFrame(v) for k, v in frames.items()}, None
    )
    combined = MyTransform(None, dfc).select("combined").toDF()
    return {row.city: row for row in combined.collect()}


@pytest.fixture(scope="module")
def rows(spark):
    return run_transform(spark)


def test_one_row_per_city_with_hotels_and_activities_from_the_latest_extract(rows):
    assert sorted(rows) == ["vancouver", "washington"]


def test_washington_dc_activities_join_the_washington_row(rows):
    dc = rows["washington"]
    assert dc.has_activities is True
    assert dc.activity_count == 2
    assert [a.name for a in dc.activity_examples] == ["Gypsy", "Lincoln Memorial"]


def test_activity_category_skips_access_flags(rows):
    examples = {a.name: a for a in rows["washington"].activity_examples}
    assert examples["Lincoln Memorial"].category == "tourism"
    assert rows["washington"].activity_categories == ["tourism"]


@pytest.mark.parametrize(
    "city",
    [
        "page",
        "austin",
        "boise",
        "omaha",
    ],
)
def test_cities_missing_hotels_or_activities_are_dropped(rows, city):
    assert city not in rows


def test_every_row_has_hotels_and_activities(rows):
    assert all(r.has_hotels and r.has_activities for r in rows.values())


def test_flight_and_hotel_summaries(rows):
    dc = rows["washington"]
    assert dc.destination_name == "Washington, D.C."
    assert dc.flight_options == 2
    assert dc.flight_min_usd == 200.0
    assert dc.flight_avg_usd == 225.0
    assert dc.flight_outbound_date == date(2026, 10, 24)
    assert dc.hotel_options == 2
    assert dc.hotel_nights == 7
    assert dc.hotel_min_stay_usd == 700.0
    assert dc.hotel_min_nightly_usd == 100.0
    assert dc.cheapest_hotel == "Capitol Inn"
    assert dc.est_min_trip_cost_usd == 900.0
    assert dc.est_avg_trip_cost_usd == 1275.0


def test_us_city_uses_usd(rows):
    dc = rows["washington"]
    assert dc.country == "US"
    assert dc.country_source == "hotel_search"
    assert dc.local_currency == "USD"
    assert dc.local_per_usd == 1.0
    assert dc.est_min_trip_cost_local == dc.est_min_trip_cost_usd


def test_foreign_hotel_prices_convert_with_the_latest_rate(rows):
    van = rows["vancouver"]
    assert van.country == "CA"
    assert van.local_currency == "CAD"
    assert van.local_per_usd == 1.25
    assert van.hotel_min_stay_usd == 800.0
    assert van.hotel_min_nightly_usd == 200.0
    assert van.hotel_min_stay_local == 1000.0
    assert van.est_min_trip_cost_usd == 1200.0
    assert van.est_min_trip_cost_local == 1500.0
    assert van.activity_count == 1


def test_country_falls_back_to_activities_then_known_city_list(spark):
    def hotel(q):
        return (
            q,
            "USD",
            500.0,
            "2026-10-23",
            "2026-10-30",
            4.0,
            3,
            "Inn",
            EXTRACT_DATE,
        )

    def activity(destination, country_code):
        return (
            destination,
            "Main Plaza",
            "Main Plaza",
            ["tourism"],
            0.0,
            0.0,
            country_code,
            EXTRACT_DATE,
        )

    rows = run_transform(
        spark,
        flights=[],
        hotels=[hotel("Vancouver"), hotel("Cancun"), hotel("Page")],
        activities=[
            activity("vancouver_canada", "ca"),
            activity("cancun", None),
            activity("page", None),
        ],
    )
    assert (rows["vancouver"].country, rows["vancouver"].country_source) == (
        "CA",
        "activities",
    )
    assert (rows["cancun"].country, rows["cancun"].country_source) == (
        "MX",
        "known_city_list",
    )
    assert rows["cancun"].local_currency == "MXN"
    assert (rows["page"].country, rows["page"].country_source) == ("US", "default")


def test_city_filter_job_arg(spark, monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["handler.py", "--CITY_FILTER", "Washington, D.C."]
    )
    assert sorted(run_transform(spark)) == ["washington"]


def test_extract_date_job_arg_selects_that_extract(spark, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["handler.py", "--EXTRACT_DATE", OLD_EXTRACT_DATE])
    rows = run_transform(spark)
    assert sorted(rows) == ["denver"]
    assert rows["denver"].flight_min_usd == 99.0
    assert rows["denver"].cheapest_hotel == "Old Extract Inn"

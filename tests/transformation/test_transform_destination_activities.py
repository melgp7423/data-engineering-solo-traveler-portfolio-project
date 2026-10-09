import io
import json
import sys
import types
from datetime import date

import pyarrow.parquet as pq
import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from transformation.transform_destination_activities.handler import (
    ActivitiesTransformer,
    CityGrouper,
    InvalidRawPayloadError,
    RawObjectLocator,
    S3ParquetWriter,
    S3RawReader,
    TransformConfig,
    TransformedKeyBuilder,
    TransformError,
    TransformService,
)

PAGE_0 = "ingest_date=2026-10-09/places_021651_fde87987_page000.json"
PAGE_1 = "ingest_date=2026-10-09/places_021651_fde87987_page001.json"
GEOCODE = "ingest_date=2026-10-09/geocode_021651_fde87987.json"


def make_feature(**overrides):
    properties = {
        "name": "DTE",
        "country": "United States",
        "country_code": "us",
        "state": "Michigan",
        "city": "Detroit",
        "postcode": "48226",
        "street": "3rd Avenue",
        "lon": -83.0576393,
        "lat": 42.3325668,
        "formatted": "DTE, 3rd Avenue, Detroit, MI 48226, United States of America",
        "categories": [
            "tourism",
            "tourism.attraction",
            "tourism.attraction.artwork",
            "tourism.attraction.artwork.sculpture",
        ],
        "details": ["details.artwork"],
        "datasource": {"sourcename": "openstreetmap", "raw": {"osm_id": 12094489902}},
        "place_id": "51b81dbf5cb0c354c05921de848c912a4540f00103f9012e45e3d002000000920303445445",
    }
    properties.update(overrides)
    properties = {k: v for k, v in properties.items() if v is not ...}
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "Point", "coordinates": [-83.0576393, 42.3325668]},
    }


def make_payload(features):
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class FakeS3:
    def __init__(self, objects=None, pages=None):
        self.objects = dict(objects or {})
        self.puts = []
        self.paginator = FakePaginator(pages or [])

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


def build_service(s3, today=date(2026, 10, 9), transformed_prefix=""):
    return TransformService(
        locator=RawObjectLocator("raw-bucket", "", client=s3, today=lambda: today),
        reader=S3RawReader(client=s3),
        transformer=ActivitiesTransformer(),
        grouper=CityGrouper(),
        key_builder=TransformedKeyBuilder("", transformed_prefix),
        writer=S3ParquetWriter("transformed-bucket", client=s3),
    )


def read_parquet(put):
    return pq.read_table(io.BytesIO(put["Body"]))


def raw(payload):
    return json.dumps(payload).encode()


# ---------------------------------------------------------------------------
# ActivitiesTransformer
# ---------------------------------------------------------------------------
def test_keeps_only_requested_fields_and_every_category():
    rows = ActivitiesTransformer().transform(make_payload([make_feature()]))

    assert rows == [{
        "name": "DTE",
        "country_code": "us",
        "city": "Detroit",
        "formatted": "DTE, 3rd Avenue, Detroit, MI 48226, United States of America",
        "categories": [
            "tourism",
            "tourism.attraction",
            "tourism.attraction.artwork",
            "tourism.attraction.artwork.sculpture",
        ],
    }]


@pytest.mark.parametrize("field", ["name", "country_code", "city", "formatted", "categories"])
def test_drops_feature_missing_a_required_field(field):
    rows = ActivitiesTransformer().transform(make_payload([make_feature(**{field: ...}), make_feature()]))

    assert len(rows) == 1


@pytest.mark.parametrize("field,value", [
    ("name", ""),
    ("name", "   "),
    ("name", None),
    ("city", ""),
    ("country_code", None),
    ("formatted", ""),
    ("categories", []),
    ("categories", None),
    ("categories", "tourism"),
])
def test_drops_feature_with_empty_required_field(field, value):
    rows = ActivitiesTransformer().transform(make_payload([make_feature(**{field: value})]))

    assert rows == []


def test_skips_features_without_properties():
    rows = ActivitiesTransformer().transform(make_payload([{"type": "Feature"}, "junk", make_feature()]))

    assert len(rows) == 1


def test_rejects_payload_without_features():
    with pytest.raises(InvalidRawPayloadError):
        ActivitiesTransformer().transform({"type": "FeatureCollection"})


# ---------------------------------------------------------------------------
# CityGrouper
# ---------------------------------------------------------------------------
def test_groups_rows_by_city_one_row_per_activity_and_drops_duplicates():
    transformer = ActivitiesTransformer()
    rows = transformer.transform(make_payload([
        make_feature(name="DTE"),
        make_feature(name="Spirit of Detroit", formatted="Spirit of Detroit, Detroit, MI"),
        make_feature(name="DTE"),  # same place again
        make_feature(name="The Alamo", city="San Antonio", formatted="The Alamo, San Antonio, TX"),
    ]))

    tables = CityGrouper().group({"ingest_date=2026-10-09": rows})

    assert sorted(tables) == [("ingest_date=2026-10-09", "Detroit"), ("ingest_date=2026-10-09", "San Antonio")]
    detroit = tables[("ingest_date=2026-10-09", "Detroit")]
    assert detroit.schema == ActivitiesTransformer.SCHEMA
    assert detroit.column("name").to_pylist() == ["DTE", "Spirit of Detroit"]
    assert tables[("ingest_date=2026-10-09", "San Antonio")].num_rows == 1


# ---------------------------------------------------------------------------
# TransformedKeyBuilder
# ---------------------------------------------------------------------------
def test_key_builder_names_file_after_city_in_same_partition():
    builder = TransformedKeyBuilder("destination_activities/", "activities/")
    partition = builder.partition(f"destination_activities/{PAGE_0}")

    assert partition == "ingest_date=2026-10-09"
    assert builder.build(partition, "San Antonio") == "activities/ingest_date=2026-10-09/activities_san_antonio.parquet"
    assert builder.build(partition, "Winston-Salem") == "activities/ingest_date=2026-10-09/activities_winston_salem.parquet"


# ---------------------------------------------------------------------------
# RawObjectLocator
# ---------------------------------------------------------------------------
def test_locator_uses_places_keys_from_extract_output_and_skips_geocode():
    locator = RawObjectLocator("raw-bucket")
    event = {"Payload": {"bucket": "b", "geocode_key": GEOCODE, "places_keys": [PAGE_0, PAGE_1]}}

    assert locator.locate(event) == ("b", [PAGE_0, PAGE_1])


def test_locator_lists_only_places_pages_in_partition():
    s3 = FakeS3(pages=[{"Contents": [{"Key": GEOCODE}, {"Key": PAGE_1}, {"Key": PAGE_0}]}])
    locator = RawObjectLocator("raw-bucket", "", client=s3, today=lambda: date(2026, 10, 9))

    assert locator.locate({}) == ("raw-bucket", [PAGE_0, PAGE_1])
    assert s3.paginator.calls == [{"Bucket": "raw-bucket", "Prefix": "ingest_date=2026-10-09/"}]


def test_locator_decodes_s3_notification_keys():
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": PAGE_0.replace("=", "%3D")}}}]}

    assert RawObjectLocator("raw-bucket").locate(event) == ("b", [PAGE_0])


def test_locator_errors_when_partition_has_no_places_pages():
    s3 = FakeS3(pages=[{"Contents": [{"Key": GEOCODE}]}])
    locator = RawObjectLocator("raw-bucket", "", client=s3, today=lambda: date(2026, 10, 9))

    with pytest.raises(ValueError):
        locator.locate({})


# ---------------------------------------------------------------------------
# TransformService
# ---------------------------------------------------------------------------
def test_service_combines_pages_and_writes_one_parquet_per_city():
    s3 = FakeS3(objects={
        ("raw-bucket", PAGE_0): raw(make_payload([
            make_feature(name="DTE"),
            make_feature(name=...),  # no name -> dropped
            make_feature(name="The Alamo", city="San Antonio", formatted="The Alamo, San Antonio, TX"),
        ])),
        ("raw-bucket", PAGE_1): raw(make_payload([
            make_feature(name="Spirit of Detroit", formatted="Spirit of Detroit, Detroit, MI"),
        ])),
    })

    result = build_service(s3, transformed_prefix="activities").run({"places_keys": [PAGE_0, PAGE_1]})

    assert result["status"] == "SUCCEEDED"
    assert result["file_count"] == 2
    assert result["row_count"] == 3
    puts = {p["Key"]: p for p in s3.puts}
    assert set(puts) == {
        "activities/ingest_date=2026-10-09/activities_detroit.parquet",
        "activities/ingest_date=2026-10-09/activities_san_antonio.parquet",
    }
    detroit = read_parquet(puts["activities/ingest_date=2026-10-09/activities_detroit.parquet"])
    assert detroit.column_names == ["name", "country_code", "city", "formatted", "categories"]
    assert detroit.column("name").to_pylist() == ["DTE", "Spirit of Detroit"]
    assert all(p["Bucket"] == "transformed-bucket" for p in s3.puts)


def test_service_reports_partial_when_one_page_is_bad():
    s3 = FakeS3(objects={
        ("raw-bucket", PAGE_0): raw(make_payload([make_feature()])),
        ("raw-bucket", PAGE_1): b"not json",
    })

    result = build_service(s3).run({"places_keys": [PAGE_0, PAGE_1]})

    assert result["status"] == "PARTIAL"
    assert [f["source_key"] for f in result["failed"]] == [PAGE_1]
    assert result["file_count"] == 1


def test_service_raises_when_every_page_fails():
    s3 = FakeS3(objects={("raw-bucket", PAGE_0): raw({"type": "FeatureCollection"})})

    with pytest.raises(TransformError):
        build_service(s3).run({"places_keys": [PAGE_0]})


def test_service_raises_when_no_feature_is_complete():
    s3 = FakeS3(objects={("raw-bucket", PAGE_0): raw(make_payload([make_feature(name=...)]))})

    with pytest.raises(TransformError):
        build_service(s3).run({"places_keys": [PAGE_0]})
    assert s3.puts == []


# ---------------------------------------------------------------------------
# TransformConfig
# ---------------------------------------------------------------------------
def test_config_accepts_bucket_arns(monkeypatch):
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "arn:aws:s3:::raw-bucket")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET", "transformed-bucket")

    config = TransformConfig.from_env()

    assert config.aws_destination_activities_raw_data_s3_bucket == "raw-bucket"
    assert config.aws_destination_activities_transformed_data_s3_bucket == "transformed-bucket"


def test_config_requires_both_buckets(monkeypatch):
    monkeypatch.delenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", raising=False)
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET", "transformed-bucket")

    with pytest.raises(ValueError):
        TransformConfig.from_env()

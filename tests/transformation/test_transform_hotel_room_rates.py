import io
import json
import sys
import types
from datetime import date, datetime, timezone

import pyarrow.parquet as pq
import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from transformation.transform_hotel_room_rates.handler import (
    HotelsTransformer,
    InvalidRawPayloadError,
    RawObjectLocator,
    S3ParquetWriter,
    S3RawReader,
    TransformConfig,
    TransformedKeyBuilder,
    TransformError,
    TransformService,
)

RAW_KEY = "ingest_date=2026-10-09/hotels_SAT_2026-10-16_2026-10-23_021651_fde87987.json"
OTHER_KEY = "ingest_date=2026-10-09/hotels_ABQ_2026-10-16_2026-10-23_021651_fde87987.json"

SEARCH_METADATA = {
    "id": "6ac84eebb8b4a5d8e3c9e593",
    "status": "Success",
    "json_endpoint": "https://serpapi.com/searches/x/6ac84eebb8b4a5d8e3c9e593.json",
    "markdown_endpoint": "https://serpapi.com/searches/x/6ac84eebb8b4a5d8e3c9e593.md",
    "created_at": "2026-10-09T02:18:19.193Z",
    "processed_at": "2026-10-09T02:18:19.195Z",
    "google_hotels_url": "https://www.google.com/travel/search?q=San+Antonio%2C+United+States&hl=en&gl=us",
    "raw_html_file": "https://serpapi.com/searches/x/6ac84eebb8b4a5d8e3c9e593.html",
    "prettify_html_file": "https://serpapi.com/searches/x/6ac84eebb8b4a5d8e3c9e593.prettify",
    "total_time_taken": {"float": 2.2552249431610107},
}

SEARCH_PARAMETERS = {
    "engine": "google_hotels",
    "q": "San Antonio, United States",
    "gl": "us",
    "hl": "en",
    "currency": "USD",
    "check_in_date": "2026-10-16",
    "check_out_date": "2026-10-23",
    "adults": 1,
    "children": 0,
    "hotel_class": "3,4,5",
}


def make_property(**overrides):
    prop = {
        "type": "hotel",
        "name": "InTown Suites Extended Stay San Antonio TX - Nacogdoches Road",
        "description": "Modest studios with kitchens.",
        "link": "https://www.intownsuites.com/extended-stay-hotels/texas/san-antonio/nacogdoches-road/",
        "property_token": "ChgIirvSsILihod2GgwvZy8xMWgxODFsN2QQAQ",
        "check_in_time": "3:00 PM",
        "check_out_time": "12:00 PM",
        "rate_per_night": {"lowest": "$41", "extracted_lowest": 41},
        "total_rate": {
            "lowest": "$290",
            "extracted_lowest": 290,
            "before_taxes_fees": "$244",
            "extracted_before_taxes_fees": 244,
        },
        "hotel_class": "3-star hotel",
        "extracted_hotel_class": 3,
        "overall_rating": 4,
        "reviews": 491,
        "amenities": ["Free Wi-Fi", "Free parking", "Air conditioning", "Kitchen"],
    }
    prop.update(overrides)
    return prop


def make_payload(properties=None):
    return {
        "search_metadata": dict(SEARCH_METADATA),
        "search_parameters": dict(SEARCH_PARAMETERS),
        "search_information": {"total_results": 254},
        "brands": [{"id": 18, "name": "Best Western International"}],
        "ads": [{"name": "Kimpton Santo", "extracted_price": 278}],
        "properties": properties if properties is not None else [make_property()],
    }


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


def build_service(s3, today=date(2026, 10, 9)):
    return TransformService(
        locator=RawObjectLocator("raw-bucket", "", client=s3, today=lambda: today),
        reader=S3RawReader(client=s3),
        transformer=HotelsTransformer(),
        key_builder=TransformedKeyBuilder("", ""),
        writer=S3ParquetWriter("transformed-bucket", client=s3),
    )


# ---------------------------------------------------------------------------
# HotelsTransformer
# ---------------------------------------------------------------------------
def test_keeps_all_metadata_parameters_and_only_requested_property_fields():
    table = HotelsTransformer().transform(make_payload())

    assert table.column_names == [
        "search_metadata_id",
        "search_metadata_status",
        "search_metadata_json_endpoint",
        "search_metadata_markdown_endpoint",
        "search_metadata_created_at",
        "search_metadata_processed_at",
        "search_metadata_google_hotels_url",
        "search_metadata_raw_html_file",
        "search_metadata_prettify_html_file",
        "search_metadata_total_time_taken",
        "search_parameters_engine",
        "search_parameters_q",
        "search_parameters_gl",
        "search_parameters_hl",
        "search_parameters_currency",
        "search_parameters_check_in_date",
        "search_parameters_check_out_date",
        "search_parameters_adults",
        "search_parameters_children",
        "search_parameters_hotel_class",
        "name",
        "link",
        "check_in_time",
        "check_out_time",
        "total_rate_extracted_lowest",
        "extracted_hotel_class",
        "overall_rating",
        "amenities",
    ]
    row = table.to_pylist()[0]
    assert row["search_metadata_id"] == "6ac84eebb8b4a5d8e3c9e593"
    assert row["search_metadata_created_at"] == datetime(2026, 10, 9, 2, 18, 19, 193000, tzinfo=timezone.utc)
    assert row["search_metadata_total_time_taken"] == pytest.approx(2.2552249431610107)
    assert row["search_parameters_q"] == "San Antonio, United States"
    assert row["search_parameters_check_in_date"] == "2026-10-16"
    assert row["search_parameters_adults"] == 1
    assert row["search_parameters_children"] == 0
    assert row["search_parameters_hotel_class"] == "3,4,5"
    assert row["name"] == "InTown Suites Extended Stay San Antonio TX - Nacogdoches Road"
    assert row["check_in_time"] == "3:00 PM"
    assert row["check_out_time"] == "12:00 PM"
    assert row["total_rate_extracted_lowest"] == 290
    assert row["extracted_hotel_class"] == 3
    assert row["overall_rating"] == 4.0
    assert row["amenities"] == ["Free Wi-Fi", "Free parking", "Air conditioning", "Kitchen"]


def test_search_info_repeated_on_every_property_row():
    props = [make_property(name="A"), make_property(name="B"), make_property(name="C")]
    table = HotelsTransformer().transform(make_payload(props))

    assert table.num_rows == 3
    assert table.column("search_metadata_id").to_pylist() == ["6ac84eebb8b4a5d8e3c9e593"] * 3
    assert table.column("search_parameters_q").to_pylist() == ["San Antonio, United States"] * 3
    assert table.column("name").to_pylist() == ["A", "B", "C"]


def test_missing_optional_property_fields_become_nulls():
    prop = make_property()
    for field in ("link", "check_in_time", "total_rate", "overall_rating", "amenities"):
        del prop[field]
    row = HotelsTransformer().transform(make_payload([prop])).to_pylist()[0]

    assert row["link"] is None
    assert row["check_in_time"] is None
    assert row["total_rate_extracted_lowest"] is None
    assert row["overall_rating"] is None
    assert row["amenities"] == []


def test_schema_is_the_same_with_no_properties():
    full = HotelsTransformer().transform(make_payload())
    empty = HotelsTransformer().transform(make_payload([]))

    assert empty.num_rows == 0
    assert empty.schema == full.schema


def test_parses_legacy_utc_timestamp_format():
    payload = make_payload()
    payload["search_metadata"]["created_at"] = "2026-10-09 02:18:19 UTC"
    row = HotelsTransformer().transform(payload).to_pylist()[0]

    assert row["search_metadata_created_at"] == datetime(2026, 10, 9, 2, 18, 19, tzinfo=timezone.utc)


def test_unexpected_fields_kept_as_strings():
    payload = make_payload()
    payload["search_parameters"]["sort_by"] = 3
    table = HotelsTransformer().transform(payload)

    assert table.column("search_parameters_sort_by").to_pylist() == ["3"]


@pytest.mark.parametrize("missing", ["search_metadata", "search_parameters", "properties"])
def test_rejects_payload_without_required_section(missing):
    payload = make_payload()
    del payload[missing]

    with pytest.raises(InvalidRawPayloadError):
        HotelsTransformer().transform(payload)


# ---------------------------------------------------------------------------
# RawObjectLocator
# ---------------------------------------------------------------------------
def test_locator_lists_every_json_in_todays_partition():
    s3 = FakeS3(pages=[
        {"Contents": [{"Key": RAW_KEY}, {"Key": "ingest_date=2026-10-09/notes.txt"}]},
        {"Contents": [{"Key": OTHER_KEY}]},
    ])
    locator = RawObjectLocator("raw-bucket", "", client=s3, today=lambda: date(2026, 10, 9))

    assert locator.locate({}) == ("raw-bucket", [OTHER_KEY, RAW_KEY])
    assert s3.paginator.calls == [{"Bucket": "raw-bucket", "Prefix": "ingest_date=2026-10-09/"}]


def test_locator_uses_extract_lambda_output_bucket_and_ingest_date():
    s3 = FakeS3(pages=[{"Contents": [{"Key": "hotel_room_rates/ingest_date=2026-10-01/hotels_X.json"}]}])
    locator = RawObjectLocator("raw-bucket", "hotel_room_rates/", client=s3)

    bucket, keys = locator.locate({"Payload": {"bucket": "other-raw", "ingest_date": "2026-10-01", "written": []}})

    assert bucket == "other-raw"
    assert keys == ["hotel_room_rates/ingest_date=2026-10-01/hotels_X.json"]
    assert s3.paginator.calls[0]["Prefix"] == "hotel_room_rates/ingest_date=2026-10-01/"


def test_locator_single_key_and_s3_notification():
    locator = RawObjectLocator("raw-bucket")

    assert locator.locate({"key": RAW_KEY}) == ("raw-bucket", [RAW_KEY])
    encoded = RAW_KEY.replace("=", "%3D")
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": encoded}}}]}
    assert locator.locate(event) == ("b", [RAW_KEY])


def test_locator_errors_on_empty_partition():
    locator = RawObjectLocator("raw-bucket", client=FakeS3(pages=[{}]), today=lambda: date(2026, 10, 9))

    with pytest.raises(ValueError, match="ingest_date=2026-10-09"):
        locator.locate({})


# ---------------------------------------------------------------------------
# TransformedKeyBuilder
# ---------------------------------------------------------------------------
def test_key_builder_mirrors_partition_and_swaps_extension():
    assert TransformedKeyBuilder("", "").build(RAW_KEY) == RAW_KEY[: -len(".json")] + ".parquet"
    assert (
        TransformedKeyBuilder("hotel_room_rates/", "hotels/").build("hotel_room_rates/ingest_date=2026-10-09/h.json")
        == "hotels/ingest_date=2026-10-09/h.parquet"
    )


# ---------------------------------------------------------------------------
# TransformService
# ---------------------------------------------------------------------------
def test_service_transforms_every_file_in_partition():
    s3 = FakeS3(
        objects={
            ("raw-bucket", RAW_KEY): json.dumps(make_payload()).encode(),
            ("raw-bucket", OTHER_KEY): json.dumps(make_payload([make_property(), make_property()])).encode(),
        },
        pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": OTHER_KEY}]}],
    )

    result = build_service(s3).run({})

    assert result["status"] == "SUCCEEDED"
    assert result["file_count"] == 2
    assert result["row_count"] == 3
    assert [p["Key"] for p in s3.puts] == [
        OTHER_KEY.replace(".json", ".parquet"),
        RAW_KEY.replace(".json", ".parquet"),
    ]
    written = pq.read_table(io.BytesIO(s3.puts[0]["Body"]))
    assert written.num_rows == 2
    assert written.to_pylist()[0]["amenities"][0] == "Free Wi-Fi"


def test_service_skips_bad_file_and_reports_partial():
    s3 = FakeS3(
        objects={
            ("raw-bucket", RAW_KEY): json.dumps(make_payload()).encode(),
            ("raw-bucket", OTHER_KEY): b"not json",
        },
        pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": OTHER_KEY}]}],
    )

    result = build_service(s3).run({})

    assert result["status"] == "PARTIAL"
    assert result["file_count"] == 1
    assert result["failed"][0]["source_key"] == OTHER_KEY


def test_service_raises_when_every_file_fails():
    s3 = FakeS3(objects={("raw-bucket", RAW_KEY): b"{}"}, pages=[{"Contents": [{"Key": RAW_KEY}]}])

    with pytest.raises(TransformError):
        build_service(s3).run({})


def test_config_requires_both_buckets(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_PRICES_RAW_DATA_S3_BUCKET", "raw")
    monkeypatch.delenv("AWS_HOTEL_ROOM_PRICES_TRANSFORMED_DATA_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_HOTEL_ROOM_PRICES_TRANSFORMED_DATA_S3_BUCKET"):
        TransformConfig.from_env()

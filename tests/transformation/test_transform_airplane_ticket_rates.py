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

from transformation.transform_airplane_ticket_rates.handler import (
    DealsTransformer,
    InvalidRawPayloadError,
    RawObjectLocator,
    S3ParquetWriter,
    S3RawReader,
    TransformConfig,
    TransformedKeyBuilder,
    TransformService,
)

RAW_KEY = "airplane_ticket_rates/ingest_date=2026-10-07/deals_PHX_2026-10-14_2026-10-28_004706_ab12cd34.json"

SEARCH_METADATA = {
    "id": "6ac5968a919dfa73ed4e0e7c",
    "status": "Success",
    "json_endpoint": "https://serpapi.com/searches/x/6ac5968a919dfa73ed4e0e7c.json",
    "markdown_endpoint": "https://serpapi.com/searches/x/6ac5968a919dfa73ed4e0e7c.md",
    "created_at": "2026-10-07 00:47:06 UTC",
    "processed_at": "2026-10-07 00:47:06 UTC",
    "google_flights_deals_url": "https://www.google.com/travel/flights/deals?hl=en",
    "raw_html_file": "https://serpapi.com/searches/x/6ac5968a919dfa73ed4e0e7c.html",
    "prettify_html_file": "https://serpapi.com/searches/x/6ac5968a919dfa73ed4e0e7c.prettify",
    "total_time_taken": 0.98,
}


def make_deal(**overrides):
    deal = {
        "destination_id": "/m/0dzt9",
        "name": "Richmond",
        "country": "United States",
        "price": 281,
        "average_price": 377,
        "flight_link": "https://www.google.com/travel/flights?tfs=abc",
        "outbound_date": "2026-10-14",
        "return_date": "2026-10-28",
        "arrival_airport_code": "RIC",
        "flight_duration": 833,
        "stops": 1,
        "description": "Virginia capital with famed Revolutionary & Civil War sites.",
        "highlights": "Virginia capital & Civil War sites",
    }
    deal.update(overrides)
    return deal


def make_payload(deals=None):
    return {
        "search_metadata": dict(SEARCH_METADATA),
        "search_parameters": {"engine": "google_flights_deals", "departure_id": "PHX"},
        "departure_informations": {"airport_code": "PHX"},
        "deals": deals if deals is not None else [make_deal()],
    }


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
class FakeS3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def build_service(s3):
    return TransformService(
        locator=RawObjectLocator("raw-bucket"),
        reader=S3RawReader(client=s3),
        transformer=DealsTransformer(),
        key_builder=TransformedKeyBuilder("airplane_ticket_rates/", "airplane_ticket_rates/"),
        writer=S3ParquetWriter("transformed-bucket", client=s3),
    )


# ---------------------------------------------------------------------------
# DealsTransformer
# ---------------------------------------------------------------------------
def test_keeps_all_metadata_and_only_requested_deal_fields():
    table = DealsTransformer().transform(make_payload())

    assert table.column_names == [
        "search_metadata_id",
        "search_metadata_status",
        "search_metadata_json_endpoint",
        "search_metadata_markdown_endpoint",
        "search_metadata_created_at",
        "search_metadata_processed_at",
        "search_metadata_google_flights_deals_url",
        "search_metadata_raw_html_file",
        "search_metadata_prettify_html_file",
        "search_metadata_total_time_taken",
        "name",
        "price",
        "outbound_date",
        "return_date",
        "flight_duration",
        "description",
    ]
    row = table.to_pylist()[0]
    assert row["search_metadata_id"] == "6ac5968a919dfa73ed4e0e7c"
    assert row["search_metadata_created_at"] == datetime(2026, 10, 7, 0, 47, 6, tzinfo=timezone.utc)
    assert row["search_metadata_total_time_taken"] == 0.98
    assert row["name"] == "Richmond"
    assert row["price"] == 281
    assert row["outbound_date"] == "2026-10-14"
    assert row["return_date"] == "2026-10-28"
    assert row["flight_duration"] == 833


def test_metadata_repeated_on_every_deal_row():
    deals = [make_deal(name="Richmond"), make_deal(name="Madison"), make_deal(name="Provo")]
    table = DealsTransformer().transform(make_payload(deals))

    assert table.num_rows == 3
    assert table.column("search_metadata_id").to_pylist() == ["6ac5968a919dfa73ed4e0e7c"] * 3
    assert table.column("name").to_pylist() == ["Richmond", "Madison", "Provo"]


@pytest.mark.parametrize("description", ["", "   ", "\n", None, "MISSING"])
def test_empty_or_missing_description_becomes_no_description(description):
    deal = make_deal(description=description)
    if description == "MISSING":
        del deal["description"]

    table = DealsTransformer().transform(make_payload([deal]))

    assert table.column("description").to_pylist() == ["NO DESCRIPTION"]


def test_description_whitespace_is_trimmed():
    deal = make_deal(description="Utah city known for Brigham Young University.\n")
    table = DealsTransformer().transform(make_payload([deal]))

    assert table.column("description").to_pylist() == ["Utah city known for Brigham Young University."]


def test_unexpected_metadata_field_is_kept_as_string():
    payload = make_payload()
    payload["search_metadata"]["new_field"] = {"a": 1}

    table = DealsTransformer().transform(payload)

    assert table.column("search_metadata_new_field").to_pylist() == ['{"a": 1}']


def test_no_deals_gives_empty_table_with_full_schema():
    table = DealsTransformer().transform(make_payload(deals=[]))

    assert table.num_rows == 0
    assert "search_metadata_id" in table.column_names
    assert "description" in table.column_names


@pytest.mark.parametrize(
    "payload",
    [
        {"deals": []},
        {"search_metadata": {}, "deals": []},
        {"search_metadata": SEARCH_METADATA},
        {"search_metadata": SEARCH_METADATA, "deals": {"not": "a list"}},
    ],
)
def test_invalid_payload_raises(payload):
    with pytest.raises(InvalidRawPayloadError):
        DealsTransformer().transform(payload)


# ---------------------------------------------------------------------------
# RawObjectLocator / TransformedKeyBuilder
# ---------------------------------------------------------------------------
def test_locator_reads_step_functions_event():
    locator = RawObjectLocator("raw-bucket")
    assert locator.locate({"bucket": "b", "key": "k.json"}) == ("b", "k.json")
    assert locator.locate({"key": "k.json"}) == ("raw-bucket", "k.json")


def test_locator_unwraps_step_functions_lambda_invoke_payload():
    event = {"StatusCode": 200, "Payload": {"status": "SUCCEEDED", "bucket": "b", "key": "k.json"}}
    assert RawObjectLocator("raw-bucket").locate(event) == ("b", "k.json")


def test_locator_reads_eventbridge_s3_event():
    event = {"detail-type": "Object Created", "detail": {"bucket": {"name": "b"}, "object": {"key": "k.json"}}}
    assert RawObjectLocator("raw-bucket").locate(event) == ("b", "k.json")


class FakeListingS3:
    def __init__(self, objects):
        self.objects = objects
        self.prefixes = []

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix):
        self.prefixes.append(Prefix)
        return [{"Contents": [o for o in self.objects if o["Key"].startswith(Prefix)]}]


def test_locator_without_key_uses_newest_json_in_todays_partition():
    s3 = FakeListingS3(
        [
            {"Key": RAW_KEY, "LastModified": datetime(2026, 10, 7, 0, 47)},
            {"Key": "airplane_ticket_rates/ingest_date=2026-10-07/deals_PHX_2026-10-14_2026-10-21_005019_52b30cfc.json",
             "LastModified": datetime(2026, 10, 7, 0, 50)},
            {"Key": "airplane_ticket_rates/ingest_date=2026-10-07/notes.txt", "LastModified": datetime(2026, 10, 7, 1, 0)},
            {"Key": "airplane_ticket_rates/ingest_date=2026-10-06/old.json", "LastModified": datetime(2026, 10, 8)},
        ]
    )
    locator = RawObjectLocator("raw-bucket", "airplane_ticket_rates/", client=s3, today=lambda: date(2026, 10, 7))

    assert locator.locate({}) == (
        "raw-bucket",
        "airplane_ticket_rates/ingest_date=2026-10-07/deals_PHX_2026-10-14_2026-10-21_005019_52b30cfc.json",
    )
    assert s3.prefixes == ["airplane_ticket_rates/ingest_date=2026-10-07/"]


def test_locator_without_key_honours_ingest_date():
    s3 = FakeListingS3([{"Key": "airplane_ticket_rates/ingest_date=2026-10-06/old.json", "LastModified": datetime(2026, 10, 6)}])
    locator = RawObjectLocator("raw-bucket", "airplane_ticket_rates/", client=s3, today=lambda: date(2026, 10, 7))

    assert locator.locate({"ingest_date": "2026-10-06"}) == ("raw-bucket", "airplane_ticket_rates/ingest_date=2026-10-06/old.json")


def test_locator_decodes_s3_notification_key():
    event = {
        "Records": [
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "airplane_ticket_rates/ingest_date%3D2026-10-07/a+b.json"}}}
        ]
    }
    assert RawObjectLocator("raw-bucket").locate(event) == ("b", "airplane_ticket_rates/ingest_date=2026-10-07/a b.json")


def test_locator_rejects_event_without_key_when_partition_is_empty():
    locator = RawObjectLocator("raw-bucket", client=FakeListingS3([]), today=lambda: date(2026, 10, 7))
    with pytest.raises(ValueError, match="ingest_date=2026-10-07"):
        locator.locate({})


def test_key_builder_keeps_partition_and_swaps_extension():
    builder = TransformedKeyBuilder("airplane_ticket_rates/", "flights_clean")
    assert builder.build(RAW_KEY) == (
        "flights_clean/ingest_date=2026-10-07/deals_PHX_2026-10-14_2026-10-28_004706_ab12cd34.parquet"
    )


# ---------------------------------------------------------------------------
# TransformService end to end
# ---------------------------------------------------------------------------
def test_service_reads_json_and_writes_parquet():
    s3 = FakeS3({("raw-bucket", RAW_KEY): json.dumps(make_payload()).encode()})

    result = build_service(s3).run({"bucket": "raw-bucket", "key": RAW_KEY})

    assert result["status"] == "SUCCEEDED"
    assert result["bucket"] == "transformed-bucket"
    assert result["key"].endswith(".parquet")
    assert result["row_count"] == 1
    assert result["search_id"] == "6ac5968a919dfa73ed4e0e7c"

    put = s3.puts[0]
    assert put["Bucket"] == "transformed-bucket"
    written = pq.read_table(io.BytesIO(put["Body"]))
    assert written.to_pylist()[0]["name"] == "Richmond"


def test_config_requires_both_buckets(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET", "raw")
    monkeypatch.delenv("AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET"):
        TransformConfig.from_env()

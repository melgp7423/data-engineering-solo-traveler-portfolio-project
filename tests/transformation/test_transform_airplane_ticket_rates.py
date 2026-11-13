import io
import json
import sys
import types
from datetime import date, datetime, timezone

import pyarrow.parquet as pq
import pytest

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

try:
    import botocore.config
except ImportError:
    _botocore = types.ModuleType("botocore")
    _botocore.config = types.ModuleType("botocore.config")
    _botocore.config.Config = lambda **kwargs: types.SimpleNamespace(**kwargs)
    sys.modules["botocore"] = _botocore
    sys.modules["botocore.config"] = _botocore.config

from transformation.transform_airplane_ticket_rates import handler
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

BUCKET = "travel-app-bucket"
RAW_PREFIX = "raw/"
TRANSFORMED_PREFIX = "transformed/"

RAW_KEY = (
    "raw/extract_date=2026-10-07/deals_PHX_2026-10-14_2026-10-28_004706_ab12cd34.json"
)
TRANSFORMED_KEY = "transformed/extract_date=2026-10-07/deals_PHX_2026-10-14_2026-10-28_004706_ab12cd34.parquet"

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
        locator=RawObjectLocator(BUCKET, RAW_PREFIX, client=s3),
        reader=S3RawReader(client=s3),
        transformer=DealsTransformer(),
        key_builder=TransformedKeyBuilder(RAW_PREFIX, TRANSFORMED_PREFIX),
        writer=S3ParquetWriter(BUCKET, client=s3),
    )


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
    assert row["search_metadata_created_at"] == datetime(
        2026, 10, 7, 0, 47, 6, tzinfo=timezone.utc
    )
    assert row["search_metadata_total_time_taken"] == 0.98
    assert row["name"] == "Richmond"
    assert row["price"] == 281
    assert row["outbound_date"] == "2026-10-14"
    assert row["return_date"] == "2026-10-28"
    assert row["flight_duration"] == 833


def test_metadata_repeated_on_every_deal_row():
    deals = [
        make_deal(name="Richmond"),
        make_deal(name="Madison"),
        make_deal(name="Provo"),
    ]
    table = DealsTransformer().transform(make_payload(deals))

    assert table.num_rows == 3
    assert (
        table.column("search_metadata_id").to_pylist()
        == ["6ac5968a919dfa73ed4e0e7c"] * 3
    )
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

    assert table.column("description").to_pylist() == [
        "Utah city known for Brigham Young University."
    ]


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


def make_locator(client=None, **kwargs):
    # Events that carry a key never touch S3, so those tests need no client.
    return RawObjectLocator(BUCKET, RAW_PREFIX, client=client, **kwargs)


def test_locator_reads_step_functions_event():
    locator = make_locator()
    assert locator.locate({"bucket": "b", "key": "k.json"}) == ("b", "k.json")
    assert locator.locate({"key": "k.json"}) == (BUCKET, "k.json")


def test_locator_unwraps_step_functions_lambda_invoke_payload():
    event = {
        "StatusCode": 200,
        "Payload": {"status": "SUCCEEDED", "bucket": "b", "key": "k.json"},
    }
    assert make_locator().locate(event) == ("b", "k.json")


def test_locator_reads_eventbridge_s3_event():
    event = {
        "detail-type": "Object Created",
        "detail": {"bucket": {"name": "b"}, "object": {"key": "k.json"}},
    }
    assert make_locator().locate(event) == ("b", "k.json")


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
            {
                "Key": "raw/extract_date=2026-10-07/deals_PHX_2026-10-14_2026-10-21_005019_52b30cfc.json",
                "LastModified": datetime(2026, 10, 7, 0, 50),
            },
            {
                "Key": "raw/extract_date=2026-10-07/notes.txt",
                "LastModified": datetime(2026, 10, 7, 1, 0),
            },
            {
                "Key": "raw/extract_date=2026-10-06/old.json",
                "LastModified": datetime(2026, 10, 8),
            },
            {
                "Key": "transformed/extract_date=2026-10-07/newer.json",
                "LastModified": datetime(2026, 10, 7, 2, 0),
            },
        ]
    )
    locator = RawObjectLocator(
        BUCKET,
        RAW_PREFIX,
        client=s3,
        today=lambda: date(2026, 10, 7),
    )

    assert locator.locate({}) == (
        BUCKET,
        "raw/extract_date=2026-10-07/deals_PHX_2026-10-14_2026-10-21_005019_52b30cfc.json",
    )
    assert s3.prefixes == ["raw/extract_date=2026-10-07/"]


def test_locator_without_key_honours_extract_date():
    s3 = FakeListingS3(
        [
            {
                "Key": "raw/extract_date=2026-10-06/old.json",
                "LastModified": datetime(2026, 10, 6),
            }
        ]
    )
    locator = RawObjectLocator(
        BUCKET,
        RAW_PREFIX,
        client=s3,
        today=lambda: date(2026, 10, 7),
    )

    assert locator.locate({"extract_date": "2026-10-06"}) == (
        BUCKET,
        "raw/extract_date=2026-10-06/old.json",
    )
    assert s3.prefixes == ["raw/extract_date=2026-10-06/"]


def test_locator_decodes_s3_notification_key():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "b"},
                    "object": {"key": "raw/extract_date%3D2026-10-07/a+b.json"},
                }
            }
        ]
    }
    assert make_locator().locate(event) == (
        "b",
        "raw/extract_date=2026-10-07/a b.json",
    )


def test_locator_rejects_non_dict_event():
    with pytest.raises(ValueError, match="JSON object"):
        make_locator().locate(["not", "a", "dict"])


def test_locator_rejects_event_without_key_when_partition_is_empty():
    locator = RawObjectLocator(
        BUCKET, RAW_PREFIX, client=FakeListingS3([]), today=lambda: date(2026, 10, 7)
    )
    with pytest.raises(ValueError, match="raw/extract_date=2026-10-07"):
        locator.locate({})


def test_key_builder_swaps_raw_prefix_for_transformed_and_keeps_partition():
    builder = TransformedKeyBuilder(RAW_PREFIX, TRANSFORMED_PREFIX)
    assert builder.build(RAW_KEY) == TRANSFORMED_KEY


def test_key_builder_accepts_prefixes_without_trailing_slash():
    builder = TransformedKeyBuilder("raw", "transformed")
    assert builder.build(RAW_KEY) == TRANSFORMED_KEY


def test_service_reads_json_and_writes_parquet():
    s3 = FakeS3({(BUCKET, RAW_KEY): json.dumps(make_payload()).encode()})

    result = build_service(s3).run({"bucket": BUCKET, "key": RAW_KEY})

    assert result["status"] == "SUCCEEDED"
    assert result["source_bucket"] == BUCKET
    assert result["source_key"] == RAW_KEY
    assert result["bucket"] == BUCKET
    assert result["key"] == TRANSFORMED_KEY
    assert result["row_count"] == 1
    assert result["search_id"] == "6ac5968a919dfa73ed4e0e7c"

    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Bucket"] == BUCKET
    assert put["Key"] == TRANSFORMED_KEY
    assert put["ContentType"] == "application/vnd.apache.parquet"
    # The writer uploads the buffer itself, rewound to the start.
    assert put["Body"].tell() == 0
    written = pq.read_table(put["Body"])
    assert written.to_pylist()[0]["name"] == "Richmond"


def test_service_skips_parquet_objects():
    s3 = FakeS3()

    result = build_service(s3).run({"bucket": BUCKET, "key": TRANSFORMED_KEY})

    assert result == {
        "status": "SKIPPED",
        "source_bucket": BUCKET,
        "source_key": TRANSFORMED_KEY,
    }
    assert s3.puts == []


def written_compression(s3):
    metadata = pq.ParquetFile(s3.puts[0]["Body"]).metadata
    return metadata.row_group(0).column(0).compression


def test_writer_compresses_with_zstd_by_default():
    s3 = FakeS3()
    table = DealsTransformer().transform(make_payload())

    assert S3ParquetWriter(BUCKET, client=s3).write(TRANSFORMED_KEY, table) == (
        TRANSFORMED_KEY
    )
    assert written_compression(s3) == "ZSTD"


def test_writer_uses_configured_compression():
    s3 = FakeS3()
    table = DealsTransformer().transform(make_payload())

    S3ParquetWriter(BUCKET, client=s3, compression="snappy").write(
        TRANSFORMED_KEY, table
    )

    assert written_compression(s3) == "SNAPPY"


def test_config_reads_single_bucket_and_prefixes(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", f"arn:aws:s3:::{BUCKET}")
    monkeypatch.setenv("RAW_PREFIX", " raw/ ")
    monkeypatch.setenv("TRANSFORMED_PREFIX", "transformed/")
    monkeypatch.delenv("PARQUET_COMPRESSION", raising=False)

    config = TransformConfig.from_env()

    assert config.aws_airplane_ticket_rates_s3_bucket == BUCKET
    assert config.raw_prefix == RAW_PREFIX
    assert config.transformed_prefix == TRANSFORMED_PREFIX
    assert config.parquet_compression == "zstd"


def test_config_reads_parquet_compression(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", BUCKET)
    monkeypatch.setenv("PARQUET_COMPRESSION", " Snappy ")

    assert TransformConfig.from_env().parquet_compression == "snappy"


def test_config_requires_bucket(monkeypatch):
    monkeypatch.delenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_AIRPLANE_TICKET_RATES_S3_BUCKET"):
        TransformConfig.from_env()


def test_build_service_wires_config_and_shares_one_s3_client(monkeypatch):
    created = []

    def fake_client(*args, **kwargs):
        created.append((args, kwargs))
        return object()

    monkeypatch.setattr(handler.boto3, "client", fake_client)
    service = handler.build_service(
        TransformConfig(
            aws_airplane_ticket_rates_s3_bucket="travel-bucket",
            raw_prefix="raw/",
            transformed_prefix="transformed/",
            parquet_compression="snappy",
        )
    )

    assert service._locator._default_bucket == "travel-bucket"
    assert service._writer.bucket == "travel-bucket"
    assert service._writer._compression == "snappy"
    assert created == [(("s3",), {"config": handler._S3_CLIENT_CONFIG})]
    assert (
        service._locator._client
        is service._reader._client
        is service._writer._client
    )


def test_build_service_uses_injected_client(monkeypatch):
    def fail(*_a, **_k):
        raise AssertionError("boto3.client should not be called")

    monkeypatch.setattr(handler.boto3, "client", fail)
    s3 = FakeS3()

    service = handler.build_service(
        TransformConfig(
            aws_airplane_ticket_rates_s3_bucket="travel-bucket",
            raw_prefix="raw/",
            transformed_prefix="transformed/",
        ),
        client=s3,
    )

    assert service._reader._client is s3
    assert service._writer._client is s3


def test_lambda_handler_builds_once_and_passes_event(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", "travel-bucket")
    built = []

    class StubService:
        def run(self, event):
            return {"event": event}

    def fake_build_service(cfg):
        built.append(cfg)
        return StubService()

    monkeypatch.setattr(handler, "_service", None)
    monkeypatch.setattr(handler, "build_service", fake_build_service)

    event = {"bucket": "travel-bucket", "key": "k.json"}
    first = handler.lambda_handler(event, None)
    handler.lambda_handler(event, None)

    assert first == {"event": event}
    assert len(built) == 1
    assert built[0].aws_airplane_ticket_rates_s3_bucket == "travel-bucket"


def test_lambda_handler_treats_missing_event_as_empty(monkeypatch):
    class StubService:
        def run(self, event):
            return event

    monkeypatch.setattr(handler, "_service", StubService())
    assert handler.lambda_handler(None, None) == {}

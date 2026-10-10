import io
import json
import sys
import types
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from transformation.transform_exchange_rates.handler import (
    ExchangeRatesTransformer,
    InvalidRawPayloadError,
    RawKeyParser,
    RawObjectLocator,
    S3ParquetWriter,
    S3RawReader,
    TransformConfig,
    TransformedKeyBuilder,
    TransformError,
    TransformService,
)

RAW_KEY = "ingest_date=2026-10-10/rates_USD_2026-10-10_223747_4dc31908.json"
OTHER_KEY = "ingest_date=2026-10-10/rates_USD_2026-10-10_101500_aa11bb22.json"

PAYLOAD = {"data": {"CAD": 1.4257001775, "MXN": 18.3700024185, "USD": 1}}


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


def build_service(s3, today=date(2026, 10, 10)):
    return TransformService(
        locator=RawObjectLocator("raw-bucket", "", client=s3, today=lambda: today),
        reader=S3RawReader(client=s3),
        transformer=ExchangeRatesTransformer(),
        key_builder=TransformedKeyBuilder("", ""),
        writer=S3ParquetWriter("transformed-bucket", client=s3),
    )


# ---------------------------------------------------------------------------
# RawKeyParser
# ---------------------------------------------------------------------------
def test_key_parser_extracts_base_and_date_from_file_name():
    assert RawKeyParser().parse(RAW_KEY) == ("USD", date(2026, 10, 10))
    assert RawKeyParser().parse("exchange_rates/ingest_date=2026-10-11/rates_EUR_2026-10-11_000000_x.json") == (
        "EUR",
        date(2026, 10, 11),
    )


@pytest.mark.parametrize("key", ["ingest_date=2026-10-10/hotels_ABQ_2026-10-10.json", "rates_USD_20261010_x.json"])
def test_key_parser_rejects_unexpected_file_names(key):
    with pytest.raises(InvalidRawPayloadError):
        RawKeyParser().parse(key)


# ---------------------------------------------------------------------------
# ExchangeRatesTransformer
# ---------------------------------------------------------------------------
def test_one_row_per_currency_with_rate_date_and_base():
    table = ExchangeRatesTransformer().transform(PAYLOAD, RAW_KEY)

    assert table.schema == pa.schema([
        pa.field("rate_date", pa.date32()),
        pa.field("base_currency", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("rate", pa.float64()),
    ])
    assert table.to_pylist() == [
        {"rate_date": date(2026, 10, 10), "base_currency": "USD", "currency": "CAD", "rate": 1.4257001775},
        {"rate_date": date(2026, 10, 10), "base_currency": "USD", "currency": "MXN", "rate": 18.3700024185},
        {"rate_date": date(2026, 10, 10), "base_currency": "USD", "currency": "USD", "rate": 1.0},
    ]


@pytest.mark.parametrize("payload", [{}, {"data": {}}, {"data": []}, {"data": {"CAD": "1.42"}}, {"data": {"CAD": None}}])
def test_rejects_payload_without_numeric_rates(payload):
    with pytest.raises(InvalidRawPayloadError):
        ExchangeRatesTransformer().transform(payload, RAW_KEY)


# ---------------------------------------------------------------------------
# RawObjectLocator
# ---------------------------------------------------------------------------
def test_locator_uses_extract_lambda_output_key():
    locator = RawObjectLocator("raw-bucket")

    assert locator.locate({"Payload": {"bucket": "other-raw", "key": RAW_KEY, "status": "SUCCEEDED"}}) == (
        "other-raw",
        [RAW_KEY],
    )


def test_locator_lists_every_json_in_todays_partition():
    s3 = FakeS3(pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": "ingest_date=2026-10-10/notes.txt"}, {"Key": OTHER_KEY}]}])
    locator = RawObjectLocator("raw-bucket", "", client=s3, today=lambda: date(2026, 10, 10))

    assert locator.locate({}) == ("raw-bucket", [OTHER_KEY, RAW_KEY])
    assert s3.paginator.calls == [{"Bucket": "raw-bucket", "Prefix": "ingest_date=2026-10-10/"}]


def test_locator_errors_on_empty_partition():
    locator = RawObjectLocator("raw-bucket", client=FakeS3(pages=[{}]), today=lambda: date(2026, 10, 10))

    with pytest.raises(ValueError, match="ingest_date=2026-10-10"):
        locator.locate({})


# ---------------------------------------------------------------------------
# TransformedKeyBuilder
# ---------------------------------------------------------------------------
def test_key_builder_mirrors_partition_and_swaps_extension():
    assert TransformedKeyBuilder("", "").build(RAW_KEY) == RAW_KEY[: -len(".json")] + ".parquet"
    assert (
        TransformedKeyBuilder("exchange_rates/", "rates/").build("exchange_rates/ingest_date=2026-10-10/r.json")
        == "rates/ingest_date=2026-10-10/r.parquet"
    )


# ---------------------------------------------------------------------------
# TransformService
# ---------------------------------------------------------------------------
def test_service_writes_parquet_for_every_file_in_partition():
    s3 = FakeS3(
        objects={
            ("raw-bucket", RAW_KEY): json.dumps(PAYLOAD).encode(),
            ("raw-bucket", OTHER_KEY): json.dumps({"data": {"CAD": 1.4}}).encode(),
        },
        pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": OTHER_KEY}]}],
    )

    result = build_service(s3).run({})

    assert result["status"] == "SUCCEEDED"
    assert result["file_count"] == 2
    assert result["row_count"] == 4
    assert result["written"][1]["rate_date"] == "2026-10-10"
    assert [p["Key"] for p in s3.puts] == [
        OTHER_KEY.replace(".json", ".parquet"),
        RAW_KEY.replace(".json", ".parquet"),
    ]
    written = pq.read_table(io.BytesIO(s3.puts[1]["Body"]))
    assert written.column("currency").to_pylist() == ["CAD", "MXN", "USD"]
    assert written.column("rate_date").to_pylist() == [date(2026, 10, 10)] * 3


def test_service_skips_bad_file_and_reports_partial():
    s3 = FakeS3(
        objects={
            ("raw-bucket", RAW_KEY): json.dumps(PAYLOAD).encode(),
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
    monkeypatch.setenv("AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET", "raw")
    monkeypatch.delenv("AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET"):
        TransformConfig.from_env()

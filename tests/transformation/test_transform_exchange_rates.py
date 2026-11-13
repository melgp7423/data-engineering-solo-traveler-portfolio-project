import io
import json
import sys
import threading
import types
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# CI installs only pytest + the lambda's requirements.txt (pyarrow), so stand in
# for the AWS SDK that the Lambda runtime normally provides.
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

from transformation.transform_exchange_rates import handler
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

RAW_KEY = "extract_date=2026-10-10/rates_USD_2026-10-10_223747_4dc31908.json"
OTHER_KEY = "extract_date=2026-10-10/rates_USD_2026-10-10_101500_aa11bb22.json"

PAYLOAD = {"data": {"CAD": 1.4257001775, "MXN": 18.3700024185, "USD": 1}}


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
        self.read_threads = set()
        self._lock = threading.Lock()

    def get_object(self, Bucket, Key):
        self.read_threads.add(threading.get_ident())
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, **kwargs):
        with self._lock:
            self.puts.append(kwargs)

    def put_keys(self):
        # Files are processed in parallel, so uploads can land in any order.
        return sorted(p["Key"] for p in self.puts)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


def build_service(s3, today=date(2026, 10, 10), raw_prefix="", transformed_prefix=""):
    return TransformService(
        locator=RawObjectLocator(
            "travel-bucket", raw_prefix, client=s3, today=lambda: today
        ),
        reader=S3RawReader(client=s3),
        transformer=ExchangeRatesTransformer(),
        key_builder=TransformedKeyBuilder(raw_prefix, transformed_prefix),
        writer=S3ParquetWriter("travel-bucket", client=s3),
    )


def make_locator(default_bucket="travel-bucket", raw_prefix="", client=None, **kwargs):
    # Events that carry a key never touch S3, so those tests need no client.
    return RawObjectLocator(default_bucket, raw_prefix, client=client, **kwargs)


def test_key_parser_extracts_base_and_date_from_file_name():
    assert RawKeyParser().parse(RAW_KEY) == ("USD", date(2026, 10, 10))
    assert RawKeyParser().parse(
        "exchange_rates/extract_date=2026-10-11/rates_EUR_2026-10-11_000000_x.json"
    ) == (
        "EUR",
        date(2026, 10, 11),
    )


@pytest.mark.parametrize(
    "key",
    ["extract_date=2026-10-10/hotels_ABQ_2026-10-10.json", "rates_USD_20261010_x.json"],
)
def test_key_parser_rejects_unexpected_file_names(key):
    with pytest.raises(InvalidRawPayloadError):
        RawKeyParser().parse(key)


def test_one_row_per_currency_with_rate_date_and_base():
    table = ExchangeRatesTransformer().transform(PAYLOAD, RAW_KEY)

    assert table.schema == pa.schema(
        [
            pa.field("rate_date", pa.date32()),
            pa.field("base_currency", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("rate", pa.float64()),
        ]
    )
    assert table.to_pylist() == [
        {
            "rate_date": date(2026, 10, 10),
            "base_currency": "USD",
            "currency": "CAD",
            "rate": 1.4257001775,
        },
        {
            "rate_date": date(2026, 10, 10),
            "base_currency": "USD",
            "currency": "MXN",
            "rate": 18.3700024185,
        },
        {
            "rate_date": date(2026, 10, 10),
            "base_currency": "USD",
            "currency": "USD",
            "rate": 1.0,
        },
    ]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"data": {}},
        {"data": []},
        {"data": {"CAD": "1.42"}},
        {"data": {"CAD": None}},
    ],
)
def test_rejects_payload_without_numeric_rates(payload):
    with pytest.raises(InvalidRawPayloadError):
        ExchangeRatesTransformer().transform(payload, RAW_KEY)


def test_locator_uses_extract_lambda_output_key():
    locator = make_locator("raw-bucket")

    assert locator.locate(
        {"Payload": {"bucket": "other-raw", "key": RAW_KEY, "status": "SUCCEEDED"}}
    ) == (
        "other-raw",
        [RAW_KEY],
    )


def test_locator_lists_every_json_in_todays_partition():
    s3 = FakeS3(
        pages=[
            {
                "Contents": [
                    {"Key": RAW_KEY},
                    {"Key": "extract_date=2026-10-10/notes.txt"},
                    {"Key": OTHER_KEY},
                ]
            }
        ]
    )
    locator = RawObjectLocator(
        "raw-bucket", "", client=s3, today=lambda: date(2026, 10, 10)
    )

    assert locator.locate({}) == ("raw-bucket", [OTHER_KEY, RAW_KEY])
    assert s3.paginator.calls == [
        {"Bucket": "raw-bucket", "Prefix": "extract_date=2026-10-10/"}
    ]


def test_locator_errors_on_empty_partition():
    locator = make_locator(
        "raw-bucket", client=FakeS3(pages=[{}]), today=lambda: date(2026, 10, 10)
    )

    with pytest.raises(ValueError, match="extract_date=2026-10-10"):
        locator.locate({})


def test_key_builder_mirrors_partition_and_swaps_extension():
    assert (
        TransformedKeyBuilder("", "").build(RAW_KEY)
        == RAW_KEY[: -len(".json")] + ".parquet"
    )
    assert (
        TransformedKeyBuilder("exchange_rates/", "rates/").build(
            "exchange_rates/extract_date=2026-10-10/r.json"
        )
        == "rates/extract_date=2026-10-10/r.parquet"
    )


def test_service_writes_parquet_for_every_file_in_partition():
    s3 = FakeS3(
        objects={
            ("travel-bucket", RAW_KEY): json.dumps(PAYLOAD).encode(),
            ("travel-bucket", OTHER_KEY): json.dumps({"data": {"CAD": 1.4}}).encode(),
        },
        pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": OTHER_KEY}]}],
    )

    result = build_service(s3).run({})

    assert result["status"] == "SUCCEEDED"
    assert result["file_count"] == 2
    assert result["row_count"] == 4
    # Results keep the sorted key order even though files run in parallel.
    assert [w["source_key"] for w in result["written"]] == [OTHER_KEY, RAW_KEY]
    assert result["written"][1]["rate_date"] == "2026-10-10"
    assert s3.put_keys() == [
        OTHER_KEY.replace(".json", ".parquet"),
        RAW_KEY.replace(".json", ".parquet"),
    ]
    put = next(
        p for p in s3.puts if p["Key"] == RAW_KEY.replace(".json", ".parquet")
    )
    assert put["ContentType"] == "application/vnd.apache.parquet"
    # The writer uploads the buffer itself, rewound to the start.
    assert put["Body"].tell() == 0
    written = pq.read_table(put["Body"])
    assert written.column("currency").to_pylist() == ["CAD", "MXN", "USD"]
    assert written.column("rate_date").to_pylist() == [date(2026, 10, 10)] * 3


def test_service_skips_bad_file_and_reports_partial():
    s3 = FakeS3(
        objects={
            ("travel-bucket", RAW_KEY): json.dumps(PAYLOAD).encode(),
            ("travel-bucket", OTHER_KEY): b"not json",
        },
        pages=[{"Contents": [{"Key": RAW_KEY}, {"Key": OTHER_KEY}]}],
    )

    result = build_service(s3).run({})

    assert result["status"] == "PARTIAL"
    assert result["file_count"] == 1
    assert result["failed"][0]["source_key"] == OTHER_KEY
    assert s3.put_keys() == [RAW_KEY.replace(".json", ".parquet")]


def test_service_processes_multiple_files_in_parallel_threads():
    keys = [f"extract_date=2026-10-10/rates_USD_2026-10-10_{i:06d}_x.json" for i in range(3)]
    barrier = threading.Barrier(len(keys), timeout=5)

    class BarrierS3(FakeS3):
        def get_object(self, Bucket, Key):
            # Every read waits for the others; this only completes if the
            # files are read concurrently rather than one after another.
            barrier.wait()
            return super().get_object(Bucket, Key)

    s3 = BarrierS3(
        objects={("travel-bucket", k): json.dumps(PAYLOAD).encode() for k in keys},
        pages=[{"Contents": [{"Key": k} for k in keys]}],
    )

    result = build_service(s3).run({})

    assert result["file_count"] == 3
    assert len(s3.read_threads) == 3


def test_service_single_file_runs_without_thread_pool(monkeypatch):
    def fail(*_a, **_k):
        raise AssertionError("ThreadPoolExecutor should not be used for one file")

    monkeypatch.setattr(handler, "ThreadPoolExecutor", fail)
    s3 = FakeS3(objects={("travel-bucket", RAW_KEY): json.dumps(PAYLOAD).encode()})

    result = build_service(s3).run({"key": RAW_KEY})

    assert result["status"] == "SUCCEEDED"
    assert result["written"][0]["key"] == RAW_KEY.replace(".json", ".parquet")


def test_service_caps_workers_at_max_workers(monkeypatch):
    seen = []
    real_pool = handler.ThreadPoolExecutor

    def recording_pool(max_workers):
        seen.append(max_workers)
        return real_pool(max_workers=max_workers)

    monkeypatch.setattr(handler, "ThreadPoolExecutor", recording_pool)
    keys = [
        f"extract_date=2026-10-10/rates_USD_2026-10-10_{i:06d}_x.json"
        for i in range(handler.MAX_WORKERS + 2)
    ]
    s3 = FakeS3(
        objects={("travel-bucket", k): json.dumps(PAYLOAD).encode() for k in keys},
        pages=[{"Contents": [{"Key": k} for k in keys]}],
    )

    result = build_service(s3).run({})

    assert seen == [handler.MAX_WORKERS]
    assert result["file_count"] == len(keys)


def test_service_skips_parquet_objects():
    s3 = FakeS3()
    parquet_key = RAW_KEY.replace(".json", ".parquet")

    result = build_service(s3).run({"key": parquet_key})

    assert result == {
        "status": "SKIPPED",
        "source_bucket": "travel-bucket",
        "skipped": [parquet_key],
    }
    assert s3.puts == []


def written_compression(s3):
    metadata = pq.ParquetFile(s3.puts[0]["Body"]).metadata
    return metadata.row_group(0).column(0).compression


def test_writer_compresses_with_zstd_by_default():
    s3 = FakeS3()
    table = ExchangeRatesTransformer().transform(PAYLOAD, RAW_KEY)

    assert S3ParquetWriter("travel-bucket", client=s3).write("k.parquet", table) == (
        "k.parquet"
    )
    assert written_compression(s3) == "ZSTD"


def test_writer_uses_configured_compression():
    s3 = FakeS3()
    table = ExchangeRatesTransformer().transform(PAYLOAD, RAW_KEY)

    S3ParquetWriter("travel-bucket", client=s3, compression="snappy").write(
        "k.parquet", table
    )

    assert written_compression(s3) == "SNAPPY"


def test_service_raises_when_every_file_fails():
    s3 = FakeS3(
        objects={("travel-bucket", RAW_KEY): b"{}"},
        pages=[{"Contents": [{"Key": RAW_KEY}]}],
    )

    with pytest.raises(TransformError):
        build_service(s3).run({})


def test_service_reads_and_writes_same_bucket_under_separate_prefixes():
    raw_key = "raw/exchange_rates/" + RAW_KEY
    s3 = FakeS3(
        objects={("travel-bucket", raw_key): json.dumps(PAYLOAD).encode()},
        pages=[{"Contents": [{"Key": raw_key}]}],
    )

    result = build_service(
        s3,
        raw_prefix="raw/exchange_rates/",
        transformed_prefix="transformed/exchange_rates/",
    ).run({})

    assert s3.paginator.calls == [
        {
            "Bucket": "travel-bucket",
            "Prefix": "raw/exchange_rates/extract_date=2026-10-10/",
        }
    ]
    assert result["source_bucket"] == result["bucket"] == "travel-bucket"
    assert [(p["Bucket"], p["Key"]) for p in s3.puts] == [
        (
            "travel-bucket",
            "transformed/exchange_rates/" + RAW_KEY.replace(".json", ".parquet"),
        )
    ]


def test_config_from_env_reads_renamed_bucket_variable(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", " travel-bucket ")

    cfg = TransformConfig.from_env()

    assert cfg.aws_exchange_rates_s3_bucket == "travel-bucket"


def test_config_from_env_strips_bucket_arn(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "arn:aws:s3:::travel-bucket")

    assert TransformConfig.from_env().aws_exchange_rates_s3_bucket == "travel-bucket"


def test_config_requires_bucket(monkeypatch):
    monkeypatch.delenv("AWS_EXCHANGE_RATES_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_EXCHANGE_RATES_S3_BUCKET"):
        TransformConfig.from_env()


def test_config_rejects_placeholder_bucket(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "<YOUR_RAW_BUCKET_NAME>")

    with pytest.raises(ValueError, match="AWS_EXCHANGE_RATES_S3_BUCKET"):
        TransformConfig.from_env()


def test_config_from_env_ignores_old_raw_and_transformed_bucket_variables(monkeypatch):
    monkeypatch.delenv("AWS_EXCHANGE_RATES_S3_BUCKET", raising=False)
    monkeypatch.setenv("AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET", "raw")
    monkeypatch.setenv("AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET", "transformed")

    with pytest.raises(ValueError, match="AWS_EXCHANGE_RATES_S3_BUCKET"):
        TransformConfig.from_env()


def test_config_from_env_reads_and_strips_prefixes(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.setenv("RAW_PREFIX", " raw/exchange_rates/ ")
    monkeypatch.setenv("TRANSFORMED_PREFIX", " transformed/exchange_rates/ ")

    cfg = TransformConfig.from_env()

    assert cfg.raw_prefix == "raw/exchange_rates/"
    assert cfg.transformed_prefix == "transformed/exchange_rates/"


def test_config_from_env_prefixes_default_to_empty(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.delenv("RAW_PREFIX", raising=False)
    monkeypatch.delenv("TRANSFORMED_PREFIX", raising=False)

    cfg = TransformConfig.from_env()

    assert cfg.raw_prefix == ""
    assert cfg.transformed_prefix == ""


def test_config_parquet_compression_defaults_to_zstd(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.delenv("PARQUET_COMPRESSION", raising=False)

    assert TransformConfig.from_env().parquet_compression == "zstd"


def test_config_reads_parquet_compression(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.setenv("PARQUET_COMPRESSION", " Snappy ")

    assert TransformConfig.from_env().parquet_compression == "snappy"


def test_build_service_wires_config_and_shares_one_s3_client(monkeypatch):
    created = []

    def fake_client(*args, **kwargs):
        created.append((args, kwargs))
        return object()

    monkeypatch.setattr(handler.boto3, "client", fake_client)
    service = handler.build_service(
        TransformConfig(
            aws_exchange_rates_s3_bucket="travel-bucket",
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


def test_s3_client_pool_fits_every_worker():
    # A pool smaller than the worker count would make threads queue for connections.
    assert handler._S3_CLIENT_CONFIG.max_pool_connections >= handler.MAX_WORKERS


def test_build_service_uses_injected_client(monkeypatch):
    def fail(*_a, **_k):
        raise AssertionError("boto3.client should not be called")

    monkeypatch.setattr(handler.boto3, "client", fail)
    s3 = FakeS3()

    service = handler.build_service(
        TransformConfig(aws_exchange_rates_s3_bucket="travel-bucket"), client=s3
    )

    assert service._reader._client is s3
    assert service._writer._client is s3


def test_lambda_handler_builds_once_and_passes_event(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", "travel-bucket")
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
    assert built[0].aws_exchange_rates_s3_bucket == "travel-bucket"


def test_lambda_handler_treats_missing_event_as_empty(monkeypatch):
    class StubService:
        def run(self, event):
            return event

    monkeypatch.setattr(handler, "_service", StubService())
    assert handler.lambda_handler(None, None) == {}


def test_locator_reads_every_s3_notification_record_and_decodes_keys():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "b"},
                    "object": {"key": "extract_date%3D2026-10-10/a+b.json"},
                }
            },
            {
                "s3": {
                    "bucket": {"name": "b"},
                    "object": {"key": "extract_date%3D2026-10-10/c.json"},
                }
            },
        ]
    }
    assert make_locator().locate(event) == (
        "b",
        ["extract_date=2026-10-10/a b.json", "extract_date=2026-10-10/c.json"],
    )


def test_locator_reads_eventbridge_object_created_event():
    event = {
        "detail-type": "Object Created",
        "detail": {"bucket": {"name": "b"}, "object": {"key": "k.json"}},
    }
    assert make_locator().locate(event) == ("b", ["k.json"])


def test_locator_uses_default_bucket_when_event_has_only_key():
    assert make_locator().locate({"key": "k.json"}) == (
        "travel-bucket",
        ["k.json"],
    )


@pytest.mark.parametrize("event", [None, "k.json", ["k.json"]])
def test_locator_rejects_non_object_events(event):
    with pytest.raises(ValueError, match="JSON object"):
        make_locator().locate(event)

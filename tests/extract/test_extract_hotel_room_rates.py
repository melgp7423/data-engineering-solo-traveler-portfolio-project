import json
import sys
import types
import urllib.error
from datetime import date, datetime, timezone

import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from extract.extract_hotel_room_rates import handler
from extract.extract_hotel_room_rates.handler import (
    ApiResponseError,
    Destination,
    DestinationExtractor,
    ExtractConfig,
    ExtractError,
    ExtractRun,
    ExtractService,
    FlightDealsLocator,
    FlightDealsNotFoundError,
    FlightDealsReader,
    HotelQueryBuilder,
    RawObjectKeyBuilder,
    ResponseValidator,
    S3Location,
    SecretProvider,
    StayDates,
    StayDatesCalculator,
    build_locator,
    build_service,
)

# Raw JSON lives in one bucket, split by prefix
BUCKET = "travel-bucket"
FLIGHT_DEALS_PREFIX = "raw/airplane_ticket_rates/"
RAW_PREFIX = "raw/hotel_room_rates/"
TODAY = date(2026, 10, 5)
STAY = StayDates(check_in=date(2026, 10, 12), check_out=date(2026, 10, 19))


# Fakes
class FakeS3:
    """In-memory S3: objects maps key -> (body, last_modified)."""

    def __init__(self, objects=None):
        self._objects = objects or {}
        self.prefixes = []

    def get_paginator(self, _name):
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                fake.prefixes.append(Prefix)
                yield {
                    "Contents": [
                        {"Key": k, "LastModified": modified}
                        for k, (_body, modified) in fake._objects.items()
                        if k.startswith(Prefix)
                    ]
                }

        return Paginator()

    def get_object(self, Bucket, Key):
        body = self._objects[Key][0]

        class Body:
            def read(self):
                return body

        return {"Body": Body()}


class FakeSecrets:
    def __init__(self, secret_string):
        self._secret_string = secret_string
        self.calls = 0

    def get_secret_value(self, SecretId):
        self.calls += 1
        return {"SecretString": self._secret_string}


class FakeReader:
    def __init__(self, payload):
        self._payload = payload
        self.locations = []

    def read(self, location):
        self.locations.append(location)
        return self._payload


class FakeHotelClient:
    """Replays a body (or raises) per `q`; records every params dict."""

    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    def fetch(self, params):
        self.calls.append(params)
        response = self._responses[params["q"]]
        if isinstance(response, Exception):
            raise response
        return response


class FakeWriter:
    bucket = BUCKET

    def __init__(self):
        self.objects = {}

    def write(self, key, body):
        self.objects[key] = body
        return key


def success(**extra):
    return json.dumps(
        {"search_metadata": {"status": "Success"}, "properties": [], **extra}
    ).encode()


def deals(*entries):
    return {"deals": list(entries)}


def deal(name, country="", code="", destination_id=""):
    return {
        "name": name,
        "country": country,
        "arrival_airport_code": code,
        "destination_id": destination_id,
    }


def config(**overrides):
    values = dict(
        api_url="https://serpapi/search.json",
        api_key_secret="secret",
        aws_airplane_ticket_rates_s3_bucket=BUCKET,
        flight_deals_prefix=FLIGHT_DEALS_PREFIX,
        aws_hotel_room_rates_s3_bucket=BUCKET,
        raw_prefix=RAW_PREFIX,
    )
    values.update(overrides)
    return ExtractConfig(**values)


def set_required_env(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", BUCKET)
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", BUCKET)


def make_service(payload, responses, writer=None, cfg=None):
    cfg = cfg or config()
    client = FakeHotelClient(responses)
    service = ExtractService(
        dates=StayDatesCalculator(7, 14, today=lambda: TODAY),
        reader=FakeReader(payload),
        extractor=DestinationExtractor(cfg.max_destinations),
        query_builder=HotelQueryBuilder(cfg),
        client=client,
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(cfg.raw_prefix),
        writer=writer or FakeWriter(),
    )
    return service, client


# ExtractConfig
def test_config_accepts_valid_settings():
    config().validate()


def test_config_rejects_placeholder_hotel_bucket():
    with pytest.raises(ValueError, match="AWS_HOTEL_ROOM_RATES_S3_BUCKET"):
        config(aws_hotel_room_rates_s3_bucket="<YOUR_RAW_BUCKET_NAME>").validate()


def test_config_rejects_empty_flight_deals_bucket():
    with pytest.raises(ValueError, match="AWS_AIRPLANE_TICKET_RATES_S3_BUCKET"):
        config(aws_airplane_ticket_rates_s3_bucket="").validate()


@pytest.mark.parametrize("hotel_class", ["1,2", "3,6", "3;4", "", "345"])
def test_config_rejects_bad_hotel_class(hotel_class):
    with pytest.raises(ValueError, match="HOTEL_CLASS"):
        config(hotel_class=hotel_class).validate()


def test_config_rejects_bad_numbers():
    with pytest.raises(ValueError, match="ADULTS"):
        config(adults=0).validate()
    with pytest.raises(ValueError, match="MAX_DESTINATIONS"):
        config(max_destinations=0).validate()
    with pytest.raises(ValueError, match="CHECK_IN_OFFSET_DAYS"):
        config(check_in_offset_days=14, check_out_offset_days=14).validate()
    with pytest.raises(ValueError, match="CHECK_IN_OFFSET_DAYS"):
        config(check_in_offset_days=-1).validate()


def test_config_from_env_reads_renamed_variables(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", f" {BUCKET} ")
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", f" {BUCKET} ")
    monkeypatch.setenv("FLIGHT_DEALS_PREFIX", f" {FLIGHT_DEALS_PREFIX} ")
    monkeypatch.setenv("RAW_PREFIX", f" {RAW_PREFIX} ")
    monkeypatch.setenv("HOTEL_CLASS", "4, 5")
    monkeypatch.setenv("CURRENCY", " eur ")

    cfg = ExtractConfig.from_env()

    assert cfg.aws_airplane_ticket_rates_s3_bucket == BUCKET
    assert cfg.aws_hotel_room_rates_s3_bucket == BUCKET
    assert cfg.flight_deals_prefix == FLIGHT_DEALS_PREFIX
    assert cfg.raw_prefix == RAW_PREFIX
    assert cfg.hotel_class == "4,5"
    assert cfg.currency == "EUR"


def test_config_from_env_prefixes_default_to_empty(monkeypatch):
    set_required_env(monkeypatch)
    monkeypatch.delenv("FLIGHT_DEALS_PREFIX", raising=False)
    monkeypatch.delenv("RAW_PREFIX", raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.flight_deals_prefix == ""
    assert cfg.raw_prefix == ""


def test_config_from_env_uses_defaults(monkeypatch):
    set_required_env(monkeypatch)
    for name in (
        "API_URL",
        "API_KEY_SECRET",
        "MAX_DESTINATIONS",
        "CHECK_IN_OFFSET_DAYS",
        "CHECK_OUT_OFFSET_DAYS",
        "ADULTS",
        "HOTEL_CLASS",
        "CURRENCY",
        "REQUEST_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.api_url == "https://serpapi.com/search.json"
    assert cfg.api_key_secret == "prod/travelProject/serpApi"
    assert cfg.max_destinations == 50
    assert (cfg.check_in_offset_days, cfg.check_out_offset_days) == (7, 14)
    assert cfg.adults == 1
    assert cfg.hotel_class == "3,4,5"
    assert cfg.currency == "USD"
    assert cfg.request_timeout == 60


def test_config_from_env_ignores_old_raw_data_bucket_variables(monkeypatch):
    monkeypatch.delenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", raising=False)
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET", "flights")
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "hotels")

    with pytest.raises(ValueError) as err:
        ExtractConfig.from_env()

    assert "AWS_AIRPLANE_TICKET_RATES_S3_BUCKET" in str(err.value)
    assert "AWS_HOTEL_ROOM_RATES_S3_BUCKET" in str(err.value)


# StayDatesCalculator
def test_stay_dates_are_offsets_from_today():
    stay = StayDatesCalculator(7, 14, today=lambda: TODAY).calculate()
    assert stay == STAY


# FlightDealsLocator
def test_locator_uses_event_bucket_and_key():
    locator = FlightDealsLocator(BUCKET, FLIGHT_DEALS_PREFIX, client=FakeS3())
    event = {"bucket": "other-bucket", "key": "raw/airplane_ticket_rates/x.json"}
    assert locator.locate(event) == S3Location(
        "other-bucket", "raw/airplane_ticket_rates/x.json"
    )


def test_locator_falls_back_to_configured_bucket_when_event_has_only_key():
    locator = FlightDealsLocator(BUCKET, FLIGHT_DEALS_PREFIX, client=FakeS3())
    assert locator.locate({"key": "k.json"}) == S3Location(BUCKET, "k.json")


def test_locator_picks_newest_json_in_todays_partition():
    partition = "raw/airplane_ticket_rates/extract_date=2026-10-05/"
    s3 = FakeS3(
        {
            f"{partition}old.json": (b"{}", datetime(2026, 10, 5, 1, tzinfo=timezone.utc)),
            f"{partition}new.json": (b"{}", datetime(2026, 10, 5, 9, tzinfo=timezone.utc)),
            f"{partition}newest.txt": (b"", datetime(2026, 10, 5, 12, tzinfo=timezone.utc)),
            "raw/airplane_ticket_rates/extract_date=2026-10-04/yesterday.json": (
                b"{}",
                datetime(2026, 10, 4, 23, tzinfo=timezone.utc),
            ),
        }
    )
    locator = FlightDealsLocator(
        BUCKET, FLIGHT_DEALS_PREFIX, client=s3, today=lambda: TODAY
    )

    assert locator.locate({}) == S3Location(BUCKET, f"{partition}new.json")
    assert s3.prefixes == [partition]


def test_locator_never_falls_back_to_an_older_day():
    s3 = FakeS3(
        {
            "raw/airplane_ticket_rates/extract_date=2026-10-04/yesterday.json": (
                b"{}",
                datetime(2026, 10, 4, tzinfo=timezone.utc),
            )
        }
    )
    locator = FlightDealsLocator(
        BUCKET, FLIGHT_DEALS_PREFIX, client=s3, today=lambda: TODAY
    )
    with pytest.raises(FlightDealsNotFoundError):
        locator.locate(None)


# FlightDealsReader
def test_reader_returns_payload_with_deals_list():
    payload = deals(deal("Cancun", "Mexico"))
    s3 = FakeS3({"k.json": (json.dumps(payload).encode(), None)})
    assert FlightDealsReader(client=s3).read(S3Location(BUCKET, "k.json")) == payload


@pytest.mark.parametrize("body", [b"not json", b"[]", b'{"deals": "nope"}'])
def test_reader_rejects_bad_files(body):
    s3 = FakeS3({"k.json": (body, None)})
    with pytest.raises(FlightDealsNotFoundError):
        FlightDealsReader(client=s3).read(S3Location(BUCKET, "k.json"))


# DestinationExtractor / Destination
def test_extractor_dedupes_skips_blanks_and_normalises():
    payload = deals(
        deal(" Cancun ", " Mexico ", "cun", "d1"),
        deal("Cancun", "Mexico", "CUN", "d1"),
        deal("Paris", "France"),
        deal("paris", "france"),
        {"name": "   "},
        "not a dict",
    )

    destinations = DestinationExtractor(10).extract(payload)

    assert destinations == [
        Destination("Cancun", "Mexico", "CUN", "d1"),
        Destination("Paris", "France"),
    ]


def test_extractor_respects_max_destinations():
    payload = deals(deal("A"), deal("B"), deal("C"))
    assert [d.name for d in DestinationExtractor(2).extract(payload)] == ["A", "B"]


def test_destination_search_query_and_label():
    assert Destination("Cancun", "Mexico", "CUN").search_query == "Cancun, Mexico"
    assert Destination("Cancun").search_query == "Cancun"
    assert Destination("Cancun", "Mexico", "CUN").label == "CUN"
    assert Destination("San José", "Costa Rica").label == "San-Jos"
    assert Destination("***").label == "UNKNOWN"


# HotelQueryBuilder
def test_query_builder_builds_google_hotels_params():
    params = HotelQueryBuilder(config(adults=2, currency="EUR")).build(
        Destination("Cancun", "Mexico"), STAY
    )
    assert params == {
        "engine": "google_hotels",
        "q": "Cancun, Mexico",
        "check_in_date": "2026-10-12",
        "check_out_date": "2026-10-19",
        "adults": "2",
        "hotel_class": "3,4,5",
        "currency": "EUR",
    }


# SecretProvider
@pytest.mark.parametrize(
    "secret_string",
    ["abc123", " abc123 ", '{"SERPAPI_KEY": "abc123"}', '{"api_key": "abc123", "x": 1}'],
)
def test_secret_provider_extracts_key(secret_string):
    assert SecretProvider("id", client=FakeSecrets(secret_string)).get() == "abc123"


def test_secret_provider_caches_value():
    client = FakeSecrets("abc123")
    provider = SecretProvider("id", client=client)
    provider.get()
    provider.get()
    assert client.calls == 1


# ResponseValidator
def test_validator_accepts_success():
    assert ResponseValidator().validate(success())["search_metadata"] == {
        "status": "Success"
    }


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[]",
        json.dumps({"error": "Invalid API key"}).encode(),
        json.dumps({"search_metadata": {"status": "Error"}}).encode(),
        json.dumps({"properties": []}).encode(),
    ],
)
def test_validator_rejects_errors(body):
    with pytest.raises(ApiResponseError):
        ResponseValidator().validate(body)


# RawObjectKeyBuilder
def test_key_builder_builds_partitioned_key_under_raw_prefix():
    run = ExtractRun(
        run_id="abcd1234",
        started_at=datetime(2026, 10, 5, 13, 4, 5, tzinfo=timezone.utc),
    )
    key = RawObjectKeyBuilder(RAW_PREFIX).build(
        run, Destination("Cancun", "Mexico", "CUN"), STAY
    )
    assert key == (
        "raw/hotel_room_rates/extract_date=2026-10-05/"
        "hotels_CUN_2026-10-12_2026-10-19_130405_abcd1234.json"
    )


# ExtractService
def test_service_writes_one_file_per_destination():
    payload = deals(deal("Cancun", "Mexico", "CUN"), deal("Paris", "France", "CDG"))
    writer = FakeWriter()
    service, _client = make_service(
        payload,
        {"Cancun, Mexico": success(), "Paris, France": success()},
        writer=writer,
    )

    result = service.run(S3Location(BUCKET, "deals.json"))

    assert result["status"] == "SUCCEEDED"
    assert result["bucket"] == BUCKET
    assert result["source_flight_deals"] == f"s3://{BUCKET}/deals.json"
    assert result["check_in_date"] == "2026-10-12"
    assert result["check_out_date"] == "2026-10-19"
    assert result["destination_count"] == 2
    assert [w["destination"] for w in result["written"]] == [
        "Cancun, Mexico",
        "Paris, France",
    ]
    assert result["failed"] == []
    assert set(writer.objects) == {w["key"] for w in result["written"]}
    assert all(k.startswith(RAW_PREFIX) for k in writer.objects)


def test_service_reports_partial_when_some_searches_fail():
    payload = deals(deal("Cancun", "Mexico"), deal("Paris", "France"))
    service, _client = make_service(
        payload,
        {
            "Cancun, Mexico": success(),
            "Paris, France": json.dumps({"error": "boom"}).encode(),
        },
    )

    result = service.run(S3Location(BUCKET, "deals.json"))

    assert result["status"] == "PARTIAL"
    assert [w["destination"] for w in result["written"]] == ["Cancun, Mexico"]
    assert result["failed"][0]["destination"] == "Paris, France"


def test_service_raises_when_every_search_fails():
    payload = deals(deal("Cancun", "Mexico"), deal("Paris", "France"))
    service, _client = make_service(
        payload,
        {
            "Cancun, Mexico": urllib.error.URLError("down"),
            "Paris, France": json.dumps({"error": "boom"}).encode(),
        },
    )
    with pytest.raises(ExtractError):
        service.run(S3Location(BUCKET, "deals.json"))


def test_service_succeeds_with_no_destinations():
    writer = FakeWriter()
    service, _client = make_service(deals(), {}, writer=writer)

    result = service.run(S3Location(BUCKET, "deals.json"))

    assert result["status"] == "SUCCEEDED"
    assert result["destination_count"] == 0
    assert writer.objects == {}


def test_service_applies_query_param_overrides():
    service, client = make_service(
        deals(deal("Cancun", "Mexico")), {"Cancun, Mexico": success()}
    )
    service.run(S3Location(BUCKET, "deals.json"), {"currency": "EUR", "adults": "3"})
    assert client.calls[0]["currency"] == "EUR"
    assert client.calls[0]["adults"] == "3"


# Composition root
def test_build_locator_reads_from_airplane_ticket_rates_bucket(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    locator = build_locator(config(aws_airplane_ticket_rates_s3_bucket="flights"))
    assert locator.locate({"key": "k.json"}) == S3Location("flights", "k.json")


def test_build_service_writes_to_hotel_room_rates_bucket(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    service = build_service(config(aws_hotel_room_rates_s3_bucket="hotels"))
    assert service._writer.bucket == "hotels"
    assert service._key_builder._prefix == "raw/hotel_room_rates"


# lambda_handler
def test_lambda_handler_builds_once_and_passes_event(monkeypatch):
    set_required_env(monkeypatch)
    built = []

    class StubLocator:
        def locate(self, event):
            return S3Location(BUCKET, event["key"])

    class StubService:
        def run(self, source, overrides):
            return {"source": str(source), "overrides": overrides}

    def fake_build_service(cfg):
        built.append(cfg)
        return StubService()

    monkeypatch.setattr(handler, "_service", None)
    monkeypatch.setattr(handler, "_locator", None)
    monkeypatch.setattr(handler, "build_locator", lambda cfg: StubLocator())
    monkeypatch.setattr(handler, "build_service", fake_build_service)

    event = {"key": "deals.json", "query_params": {"currency": "EUR"}}
    first = handler.lambda_handler(event, None)
    handler.lambda_handler(event, None)

    assert first == {
        "source": f"s3://{BUCKET}/deals.json",
        "overrides": {"currency": "EUR"},
    }
    assert len(built) == 1
    assert built[0].aws_hotel_room_rates_s3_bucket == BUCKET

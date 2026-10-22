import io
import json
import sys
import types
import urllib.error
import urllib.parse
from datetime import date, datetime, timezone

import pytest

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from extract.extract_airplane_ticket_rates import handler
from extract.extract_airplane_ticket_rates.handler import (
    ApiResponseError,
    DealsQueryBuilder,
    ExtractConfig,
    ExtractRun,
    ExtractService,
    FlightDealsClient,
    OutboundDateCalculator,
    QueryParamKeyAuthenticator,
    RawObjectKeyBuilder,
    ResponseValidator,
    S3RawWriter,
    SecretProvider,
    build_service,
)

BUCKET = "travel-bucket"
RAW_PREFIX = "raw/airplane_ticket_rates/"
API_URL = "https://serpapi.com/search.json"
TODAY = date(2026, 10, 5)


class FakeSecrets:
    def __init__(self, secret_string):
        self._secret_string = secret_string
        self.calls = 0

    def get_secret_value(self, SecretId):
        self.calls += 1
        return {"SecretString": self._secret_string}


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


class FakeResponse:
    def __init__(self, body, status=200):
        self._body = body
        self.status = status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class FakeUrlopen:

    def __init__(self, response):
        self._response = response
        self.requests = []
        self.timeouts = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        self.timeouts.append(timeout)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class FakeClient:
    def __init__(self, body):
        self._body = body
        self.calls = []

    def fetch(self, params):
        self.calls.append(params)
        return self._body


class FakeWriter:
    bucket = BUCKET

    def __init__(self):
        self.objects = {}

    def write(self, key, body):
        self.objects[key] = body
        return key


def success(**extra):
    return json.dumps(
        {
            "search_metadata": {"id": "search-1", "status": "Success"},
            "deals": [],
            **extra,
        }
    ).encode()


def config(**overrides):
    values = dict(
        api_url=API_URL,
        api_key_secret="secret",
        aws_airplane_ticket_rates_s3_bucket=BUCKET,
        raw_prefix=RAW_PREFIX,
    )
    values.update(overrides)
    return ExtractConfig(**values)


def authenticator(key="abc123"):
    return QueryParamKeyAuthenticator(SecretProvider("id", client=FakeSecrets(key)))


def query_builder(cfg=None):
    cfg = cfg or config()
    return DealsQueryBuilder(
        cfg, OutboundDateCalculator(cfg.outbound_offset_days, today=lambda: TODAY)
    )


def make_service(body, writer=None, cfg=None):
    cfg = cfg or config()
    client = FakeClient(body)
    service = ExtractService(
        query_builder=query_builder(cfg),
        client=client,
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(cfg.raw_prefix),
        writer=writer or FakeWriter(),
    )
    return service, client


def test_config_accepts_valid_settings():
    config().validate()


@pytest.mark.parametrize(
    "field, env_name",
    [
        ("api_url", "API_URL"),
        ("api_key_secret", "API_KEY_SECRET"),
        ("aws_airplane_ticket_rates_s3_bucket", "AWS_AIRPLANE_TICKET_RATES_S3_BUCKET"),
    ],
)
@pytest.mark.parametrize("value", ["", "<PLACEHOLDER>"])
def test_config_rejects_missing_or_placeholder_values(field, env_name, value):
    with pytest.raises(ValueError, match=env_name):
        config(**{field: value}).validate()


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"trip_length_days": 0}, "TRIP_LENGTH_DAYS"),
        ({"adults": 0}, "ADULTS"),
        ({"outbound_offset_days": -1}, "OUTBOUND_OFFSET_DAYS"),
    ],
)
def test_config_rejects_bad_numbers(overrides, message):
    with pytest.raises(ValueError, match=message):
        config(**overrides).validate()


def test_config_allows_departing_today():
    config(outbound_offset_days=0).validate()


def test_config_from_env_normalises_values(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", f" {BUCKET} ")
    monkeypatch.setenv("RAW_PREFIX", f" {RAW_PREFIX} ")
    monkeypatch.setenv("DEPARTURE_ID", " jfk ")
    monkeypatch.setenv("CURRENCY", " eur ")
    monkeypatch.setenv("OUTBOUND_OFFSET_DAYS", "3")
    monkeypatch.setenv("TRIP_LENGTH_DAYS", "10")
    monkeypatch.setenv("ADULTS", "2")
    monkeypatch.setenv("REQUEST_TIMEOUT", "15")

    cfg = ExtractConfig.from_env()

    assert cfg.aws_airplane_ticket_rates_s3_bucket == BUCKET
    assert cfg.raw_prefix == RAW_PREFIX
    assert cfg.departure_id == "JFK"
    assert cfg.currency == "EUR"
    assert cfg.outbound_offset_days == 3
    assert cfg.trip_length_days == 10
    assert cfg.adults == 2
    assert cfg.request_timeout == 15


def test_config_from_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", BUCKET)
    for name in (
        "API_URL",
        "API_KEY_SECRET",
        "RAW_PREFIX",
        "DEPARTURE_ID",
        "OUTBOUND_OFFSET_DAYS",
        "TRIP_LENGTH_DAYS",
        "ADULTS",
        "CURRENCY",
        "REQUEST_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.api_url == API_URL
    assert cfg.api_key_secret == "prod/travelProject/serpApi"
    assert cfg.raw_prefix == ""
    assert cfg.departure_id == "PHX"
    assert cfg.outbound_offset_days == 7
    assert cfg.trip_length_days == 7
    assert cfg.adults == 1
    assert cfg.currency == "USD"
    assert cfg.request_timeout == 60


def test_config_from_env_requires_bucket(monkeypatch):
    monkeypatch.delenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="AWS_AIRPLANE_TICKET_RATES_S3_BUCKET"):
        ExtractConfig.from_env()


def test_config_from_env_validates_numbers(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", BUCKET)
    monkeypatch.setenv("ADULTS", "0")
    with pytest.raises(ValueError, match="ADULTS"):
        ExtractConfig.from_env()


def test_outbound_date_is_offset_from_today():
    assert OutboundDateCalculator(7, today=lambda: TODAY).calculate() == date(
        2026, 10, 12
    )


def test_outbound_date_defaults_to_utc_today():
    expected = datetime.now(timezone.utc).date()
    assert OutboundDateCalculator(0).calculate() == expected


def test_query_builder_builds_round_trip_params():
    params = query_builder(
        config(departure_id="JFK", adults=2, currency="EUR", trip_length_days=5)
    ).build()
    assert params == {
        "engine": "google_flights_deals",
        "type": "1",
        "outbound_date": "2026-10-12",
        "return_date": "2026-10-17",
        "adults": "2",
        "currency": "EUR",
        "departure_id": "JFK",
    }


def test_query_builder_drops_empty_values():
    params = query_builder(config(departure_id="", currency="")).build()
    assert "departure_id" not in params
    assert "currency" not in params


def test_query_builder_recalculates_date_on_every_build():
    days = iter([date(2026, 10, 5), date(2026, 10, 6)])
    builder = DealsQueryBuilder(
        config(), OutboundDateCalculator(7, today=lambda: next(days))
    )
    assert builder.build()["outbound_date"] == "2026-10-12"
    assert builder.build()["outbound_date"] == "2026-10-13"


@pytest.mark.parametrize(
    "secret_string",
    [
        "abc123",
        " abc123 ",
        '{"SERPAPI_KEY": " abc123 "}',
        '{"api_key": "abc123", "x": 1}',
    ],
)
def test_secret_provider_extracts_key(secret_string):
    assert SecretProvider("id", client=FakeSecrets(secret_string)).get() == "abc123"


@pytest.mark.parametrize("secret_string", ["{not json", '{"a": "1", "b": "2"}'])
def test_secret_provider_returns_raw_string_when_no_key_found(secret_string):
    assert (
        SecretProvider("id", client=FakeSecrets(secret_string)).get() == secret_string
    )


def test_secret_provider_caches_value():
    client = FakeSecrets("abc123")
    provider = SecretProvider("id", client=client)
    provider.get()
    provider.get()
    assert client.calls == 1


def test_authenticator_adds_api_key_without_mutating_input():
    params = {"engine": "google_flights_deals"}
    assert authenticator().apply(params) == {
        "engine": "google_flights_deals",
        "api_key": "abc123",
    }
    assert params == {"engine": "google_flights_deals"}


def test_client_sends_params_and_api_key(monkeypatch):
    urlopen = FakeUrlopen(FakeResponse(success()))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    body = FlightDealsClient(API_URL, authenticator(), timeout=11).fetch(
        {"engine": "google_flights_deals", "departure_id": "PHX"}
    )

    request = urlopen.requests[0]
    url = urllib.parse.urlsplit(request.full_url)
    assert body == success()
    assert f"{url.scheme}://{url.netloc}{url.path}" == API_URL
    assert urllib.parse.parse_qs(url.query) == {
        "engine": ["google_flights_deals"],
        "departure_id": ["PHX"],
        "api_key": ["abc123"],
    }
    assert request.get_header("Accept") == "application/json"
    assert request.get_method() == "GET"
    assert urlopen.timeouts == [11]


def test_client_reraises_http_errors(monkeypatch):
    error = urllib.error.HTTPError(
        API_URL, 401, "Unauthorized", {}, io.BytesIO(b'{"error": "Invalid API key"}')
    )
    monkeypatch.setattr(handler.urllib.request, "urlopen", FakeUrlopen(error))
    with pytest.raises(urllib.error.HTTPError):
        FlightDealsClient(API_URL, authenticator(), timeout=11).fetch({})


def test_client_reraises_connection_errors(monkeypatch):
    monkeypatch.setattr(
        handler.urllib.request, "urlopen", FakeUrlopen(urllib.error.URLError("down"))
    )
    with pytest.raises(urllib.error.URLError):
        FlightDealsClient(API_URL, authenticator(), timeout=11).fetch({})


def test_validator_accepts_success():
    assert ResponseValidator().validate(success())["search_metadata"]["status"] == (
        "Success"
    )


@pytest.mark.parametrize(
    "body, message",
    [
        (b"not json", "not valid JSON"),
        (
            json.dumps({"error": "Invalid API key"}).encode(),
            "API error: Invalid API key",
        ),
        (json.dumps({"search_metadata": {"status": "Error"}}).encode(), "'Error'"),
        (json.dumps({"deals": []}).encode(), "None"),
    ],
)
def test_validator_rejects_errors(body, message):
    with pytest.raises(ApiResponseError, match=message):
        ResponseValidator().validate(body)


RUN = ExtractRun(
    run_id="abcd1234",
    started_at=datetime(2026, 10, 5, 13, 4, 5, tzinfo=timezone.utc),
)


def test_key_builder_builds_partitioned_key_under_raw_prefix():
    params = {
        "departure_id": "PHX",
        "outbound_date": "2026-10-12",
        "return_date": "2026-10-19",
    }
    assert RawObjectKeyBuilder(RAW_PREFIX).build(RUN, params) == (
        "raw/airplane_ticket_rates/extract_date=2026-10-05/"
        "deals_PHX_2026-10-12_2026-10-19_130405_abcd1234.json"
    )


def test_key_builder_falls_back_when_params_are_missing():
    assert RawObjectKeyBuilder(RAW_PREFIX).build(RUN, {}) == (
        "raw/airplane_ticket_rates/extract_date=2026-10-05/"
        "deals_ANY_2026-10-05_OPEN_130405_abcd1234.json"
    )


def test_extract_run_defaults_are_unique_and_utc():
    first, second = ExtractRun(), ExtractRun()
    assert len(first.run_id) == 8
    assert first.run_id != second.run_id
    assert first.started_at.tzinfo == timezone.utc


def test_writer_puts_json_object_and_returns_key():
    s3 = FakeS3()
    writer = S3RawWriter(BUCKET, client=s3)

    assert writer.write("k.json", b"{}") == "k.json"
    assert writer.bucket == BUCKET
    assert s3.puts == [
        {
            "Bucket": BUCKET,
            "Key": "k.json",
            "Body": b"{}",
            "ContentType": "application/json",
        }
    ]


def test_service_writes_raw_body_and_reports_result():
    writer = FakeWriter()
    service, client = make_service(success(), writer=writer)

    result = service.run()

    assert client.calls[0]["outbound_date"] == "2026-10-12"
    assert result["status"] == "SUCCEEDED"
    assert result["bucket"] == BUCKET
    assert result["search_id"] == "search-1"
    assert result["trip_type"] == "1"
    assert result["outbound_date"] == "2026-10-12"
    assert result["return_date"] == "2026-10-19"
    assert result["adults"] == "1"
    assert result["key"].startswith(
        f"{RAW_PREFIX}extract_date={result['extract_date']}/deals_PHX_2026-10-12_2026-10-19_"
    )
    assert result["run_id"] in result["key"]
    # The raw response is stored unchanged.
    assert writer.objects == {result["key"]: success()}


def test_service_applies_query_param_overrides():
    service, client = make_service(success())
    result = service.run({"departure_id": "JFK", "adults": "3"})
    assert client.calls[0]["departure_id"] == "JFK"
    assert client.calls[0]["adults"] == "3"
    assert "deals_JFK_" in result["key"]
    assert result["adults"] == "3"


def test_service_does_not_write_invalid_payloads():
    writer = FakeWriter()
    service, _client = make_service(
        json.dumps({"error": "Invalid API key"}).encode(), writer=writer
    )
    with pytest.raises(ApiResponseError):
        service.run()
    assert writer.objects == {}


def test_build_service_wires_config(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    service = build_service(
        config(aws_airplane_ticket_rates_s3_bucket="flights", request_timeout=9)
    )
    assert service._writer.bucket == "flights"
    assert service._key_builder._prefix == "raw/airplane_ticket_rates"
    assert service._client._api_url == API_URL
    assert service._client._timeout == 9


def test_lambda_handler_builds_once_and_passes_overrides(monkeypatch):
    monkeypatch.setenv("AWS_AIRPLANE_TICKET_RATES_S3_BUCKET", BUCKET)
    built = []

    class StubService:
        def run(self, overrides):
            return {"overrides": overrides}

    def fake_build_service(cfg):
        built.append(cfg)
        return StubService()

    monkeypatch.setattr(handler, "_service", None)
    monkeypatch.setattr(handler, "build_service", fake_build_service)

    event = {"query_params": {"departure_id": "JFK"}}
    first = handler.lambda_handler(event, None)
    handler.lambda_handler(event, None)

    assert first == {"overrides": {"departure_id": "JFK"}}
    assert len(built) == 1
    assert built[0].aws_airplane_ticket_rates_s3_bucket == BUCKET


@pytest.mark.parametrize("event", [None, "scheduled", {}, {"other": 1}])
def test_lambda_handler_defaults_overrides_to_empty(monkeypatch, event):
    class StubService:
        def run(self, overrides):
            return overrides

    monkeypatch.setattr(handler, "_service", StubService())
    assert handler.lambda_handler(event, None) == {}

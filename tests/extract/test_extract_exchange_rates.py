import io
import json
import sys
import types
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import pytest

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from extract.extract_exchange_rates import handler
from extract.extract_exchange_rates.handler import (
    ApiResponseError,
    ExchangeRatesClient,
    ExtractConfig,
    ExtractRun,
    ExtractService,
    HeaderKeyAuthenticator,
    RawObjectKeyBuilder,
    ResponseValidator,
    S3RawWriter,
    SecretProvider,
    build_service,
)

BUCKET = "travel-bucket"
RAW_PREFIX = "raw/exchange_rates/"
ENDPOINT = "https://api.freecurrencyapi.com/v1/latest"
RATES = {"CAD": 1.4257001775, "MXN": 18.3700024185, "USD": 1}


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


def rates_body(rates=None):
    return json.dumps({"data": RATES if rates is None else rates}).encode()


def http_error(code, body=b"", headers=None):
    return urllib.error.HTTPError(
        ENDPOINT, code, "error", headers or {}, io.BytesIO(body)
    )


def config(**overrides):
    values = dict(
        api_base_url="https://api.freecurrencyapi.com/v1",
        api_endpoint="latest",
        access_key_secret="secret",
        aws_exchange_rates_s3_bucket=BUCKET,
        raw_prefix=RAW_PREFIX,
    )
    values.update(overrides)
    return ExtractConfig(**values)


def authenticator(key="abc123"):
    return HeaderKeyAuthenticator(SecretProvider("id", client=FakeSecrets(key)))


def make_service(body, writer=None, default_params=None):
    client = FakeClient(body)
    service = ExtractService(
        client=client,
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(RAW_PREFIX, "USD"),
        writer=writer or FakeWriter(),
        default_params=default_params
        or {"base_currency": "USD", "currencies": "CAD,MXN,USD"},
    )
    return service, client


def test_config_accepts_valid_settings():
    config().validate()


@pytest.mark.parametrize(
    "field, env_name",
    [
        ("api_base_url", "API_BASE_URL"),
        ("api_endpoint", "API_ENDPOINT"),
        ("access_key_secret", "ACCESS_KEY_SECRET"),
        ("aws_exchange_rates_s3_bucket", "AWS_EXCHANGE_RATES_S3_BUCKET"),
    ],
)
@pytest.mark.parametrize("value", ["", "<PLACEHOLDER>"])
def test_config_rejects_missing_or_placeholder_values(field, env_name, value):
    with pytest.raises(ValueError, match=env_name):
        config(**{field: value}).validate()


def test_config_from_env_normalises_values(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", f" {BUCKET} ")
    monkeypatch.setenv("RAW_PREFIX", f" {RAW_PREFIX} ")
    monkeypatch.setenv("BASE_CURRENCY", " eur ")
    monkeypatch.setenv("CURRENCIES", "cad, mxn")
    monkeypatch.setenv("REQUEST_TIMEOUT", "5")

    cfg = ExtractConfig.from_env()

    assert cfg.aws_exchange_rates_s3_bucket == BUCKET
    assert cfg.raw_prefix == RAW_PREFIX
    assert cfg.base_currency == "EUR"
    assert cfg.currencies == "CAD,MXN"
    assert cfg.request_timeout == 5


def test_config_from_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", BUCKET)
    for name in (
        "API_BASE_URL",
        "API_ENDPOINT",
        "ACCESS_KEY_SECRET",
        "RAW_PREFIX",
        "BASE_CURRENCY",
        "CURRENCIES",
        "REQUEST_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.api_base_url == "https://api.freecurrencyapi.com/v1"
    assert cfg.api_endpoint == "latest"
    assert cfg.access_key_secret == "prod/travelProject/exchangeRatesApi"
    assert cfg.raw_prefix == ""
    assert cfg.base_currency == "USD"
    assert cfg.currencies == "CAD,MXN,USD"
    assert cfg.request_timeout == 30


def test_config_from_env_requires_bucket(monkeypatch):
    monkeypatch.delenv("AWS_EXCHANGE_RATES_S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="AWS_EXCHANGE_RATES_S3_BUCKET"):
        ExtractConfig.from_env()


@pytest.mark.parametrize(
    "base_url, endpoint",
    [
        ("https://api.freecurrencyapi.com/v1", "latest"),
        ("https://api.freecurrencyapi.com/v1/", "/latest/"),
    ],
)
def test_config_endpoint_url_joins_with_single_slash(base_url, endpoint):
    assert config(api_base_url=base_url, api_endpoint=endpoint).endpoint_url == ENDPOINT


def test_config_query_params_drop_empty_values():
    assert config().query_params == {
        "base_currency": "USD",
        "currencies": "CAD,MXN,USD",
    }
    assert config(currencies="").query_params == {"base_currency": "USD"}


@pytest.mark.parametrize(
    "secret_string",
    [
        "abc123",
        " abc123 ",
        '{"FREECURRENCY_KEY": " abc123 "}',
        '{"apikey": "abc123", "x": 1}',
        '{"api_key": "abc123", "x": 1}',
        '{"access_key": "abc123", "x": 1}',
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


def test_authenticator_adds_apikey_header_without_mutating_input():
    headers = {"Accept": "application/json"}
    assert authenticator().apply(headers) == {
        "Accept": "application/json",
        "apikey": "abc123",
    }
    assert headers == {"Accept": "application/json"}


def test_client_sends_key_in_header_not_url(monkeypatch):
    urlopen = FakeUrlopen(FakeResponse(rates_body()))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    body = ExchangeRatesClient(ENDPOINT, authenticator(), timeout=7).fetch(
        {"base_currency": "USD", "currencies": "CAD,MXN"}
    )

    request = urlopen.requests[0]
    url = urllib.parse.urlsplit(request.full_url)
    assert body == rates_body()
    assert f"{url.scheme}://{url.netloc}{url.path}" == ENDPOINT
    assert urllib.parse.parse_qs(url.query) == {
        "base_currency": ["USD"],
        "currencies": ["CAD,MXN"],
    }
    assert "abc123" not in request.full_url
    assert request.get_header("Apikey") == "abc123"
    assert request.get_method() == "GET"
    assert urlopen.timeouts == [7]


def test_client_sends_custom_user_agent(monkeypatch):
    urlopen = FakeUrlopen(FakeResponse(rates_body()))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    ExchangeRatesClient(ENDPOINT, authenticator(), timeout=7).fetch({})

    user_agent = urlopen.requests[0].get_header("User-agent")
    assert user_agent == ExchangeRatesClient.HEADERS["User-Agent"]
    assert not user_agent.startswith("Python-urllib")


@pytest.mark.parametrize(
    "body", [b'{"message": "Invalid authentication credentials"}', b"cloudflare_error"]
)
def test_client_reraises_http_errors(monkeypatch, body):
    monkeypatch.setattr(
        handler.urllib.request, "urlopen", FakeUrlopen(http_error(403, body))
    )
    with pytest.raises(urllib.error.HTTPError):
        ExchangeRatesClient(ENDPOINT, authenticator(), timeout=7).fetch({})


def test_client_reraises_connection_errors(monkeypatch):
    monkeypatch.setattr(
        handler.urllib.request, "urlopen", FakeUrlopen(urllib.error.URLError("down"))
    )
    with pytest.raises(urllib.error.URLError):
        ExchangeRatesClient(ENDPOINT, authenticator(), timeout=7).fetch({})


def test_validator_accepts_rates():
    assert ResponseValidator().validate(rates_body()) == {"data": RATES}


@pytest.mark.parametrize(
    "body, message",
    [
        (b"not json", "not valid JSON"),
        (b"[]", "Unexpected response shape"),
        (
            json.dumps({"message": "Validation error", "errors": {"x": 1}}).encode(),
            "API error: Validation error",
        ),
        (json.dumps({"data": {}}).encode(), "no 'data' rates"),
        (json.dumps({"data": [1, 2]}).encode(), "no 'data' rates"),
        (json.dumps({}).encode(), "no 'data' rates"),
    ],
)
def test_validator_rejects_errors(body, message):
    with pytest.raises(ApiResponseError, match=message):
        ResponseValidator().validate(body)


def test_validator_accepts_message_alongside_data():
    body = json.dumps({"message": "ok", "data": RATES}).encode()
    assert ResponseValidator().validate(body)["data"] == RATES


def test_key_builder_builds_partitioned_key_under_raw_prefix():
    run = ExtractRun(
        run_id="abcd1234",
        started_at=datetime(2026, 10, 5, 13, 4, 5, tzinfo=timezone.utc),
    )
    assert RawObjectKeyBuilder(RAW_PREFIX, "USD").build(run) == (
        "raw/exchange_rates/extract_date=2026-10-05/"
        "rates_USD_2026-10-05_130405_abcd1234.json"
    )


def test_key_builder_uses_unknown_when_base_currency_is_blank():
    run = ExtractRun(
        run_id="abcd1234",
        started_at=datetime(2026, 10, 5, 13, 4, 5, tzinfo=timezone.utc),
    )
    assert "rates_UNKNOWN_" in RawObjectKeyBuilder(RAW_PREFIX, "").build(run)


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
    service, client = make_service(rates_body(), writer=writer)

    result = service.run()

    assert client.calls == [{"base_currency": "USD", "currencies": "CAD,MXN,USD"}]
    assert result["status"] == "SUCCEEDED"
    assert result["bucket"] == BUCKET
    assert result["base"] == "USD"
    assert result["rate_count"] == 3
    assert result["rate_date"] == result["extract_date"]
    assert result["key"].startswith(
        f"{RAW_PREFIX}extract_date={result['extract_date']}/"
    )
    assert result["run_id"] in result["key"]
    assert writer.objects == {result["key"]: rates_body()}


def test_service_applies_query_param_overrides():
    service, client = make_service(rates_body())
    result = service.run({"currencies": "CAD", "base_currency": "EUR"})
    assert client.calls == [{"base_currency": "EUR", "currencies": "CAD"}]
    assert result["base"] == "EUR"


def test_service_does_not_write_invalid_payloads():
    writer = FakeWriter()
    service, _client = make_service(
        json.dumps({"message": "Invalid key"}).encode(), writer=writer
    )
    with pytest.raises(ApiResponseError):
        service.run()
    assert writer.objects == {}


def test_build_service_wires_config(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    service = build_service(
        config(
            aws_exchange_rates_s3_bucket="rates", currencies="CAD", request_timeout=9
        )
    )
    assert service._writer.bucket == "rates"
    assert service._key_builder._prefix == "raw/exchange_rates"
    assert service._default_params == {"base_currency": "USD", "currencies": "CAD"}
    assert service._client._endpoint_url == ENDPOINT
    assert service._client._timeout == 9


def test_lambda_handler_builds_once_and_passes_overrides(monkeypatch):
    monkeypatch.setenv("AWS_EXCHANGE_RATES_S3_BUCKET", BUCKET)
    built = []

    class StubService:
        def run(self, overrides):
            return {"overrides": overrides}

    def fake_build_service(cfg):
        built.append(cfg)
        return StubService()

    monkeypatch.setattr(handler, "_service", None)
    monkeypatch.setattr(handler, "build_service", fake_build_service)

    event = {"query_params": {"currencies": "CAD,MXN"}}
    first = handler.lambda_handler(event, None)
    handler.lambda_handler(event, None)

    assert first == {"overrides": {"currencies": "CAD,MXN"}}
    assert len(built) == 1
    assert built[0].aws_exchange_rates_s3_bucket == BUCKET


@pytest.mark.parametrize("event", [None, "scheduled", {}, {"other": 1}])
def test_lambda_handler_defaults_overrides_to_empty(monkeypatch, event):
    class StubService:
        def run(self, overrides):
            return overrides

    monkeypatch.setattr(handler, "_service", StubService())
    assert handler.lambda_handler(event, None) == {}

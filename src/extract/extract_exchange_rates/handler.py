"""
Lambda: ingest exchange rates from freecurrencyapi.com and land the raw JSON
response in S3 — OOP / Single Responsibility Principle.

Classes and their one job:
    IngestionConfig          -> read & validate settings
    SecretProvider           -> fetch the API key from Secrets Manager
    HeaderKeyAuthenticator   -> add the "apikey" header to the request
    ExchangeRatesClient      -> make the HTTP call
    ResponseValidator        -> reject payloads without a "data" rates map
    RawObjectKeyBuilder      -> name the S3 object
    S3RawWriter              -> write bytes to S3
    IngestionService         -> orchestrate fetch -> validate -> store
    lambda_handler           -> Lambda entry point

API docs: https://freecurrencyapi.com/docs/
    GET https://api.freecurrencyapi.com/v1/latest?base_currency=USD&currencies=CAD,MXN,USD
    -> {"data": {"CAD": 1.4257001775, "MXN": 18.3700024185, "USD": 1}}

The key can be sent as ?apikey=... or an "apikey" header; the header is used
(recommended by the docs) so the key never appears in URLs or access logs.
Errors come back as non-2xx statuses (401 bad key, 422 validation, 429 quota)
with {"message": ..., "errors"?: {...}}.

Environment variables (placeholders shown):
    API_BASE_URL      = https://api.freecurrencyapi.com/v1
    API_ENDPOINT      = latest
    ACCESS_KEY_SECRET = <SECRETS_MANAGER_SECRET_ID>   secret value = your freecurrencyapi key
    BASE_CURRENCY     = USD
    CURRENCIES        = CAD,MXN,USD
    RAW_BUCKET        = <YOUR_RAW_BUCKET_NAME>
    RAW_PREFIX        = exchange_rates/
    REQUEST_TIMEOUT   = 30

Only uses libraries built into the Lambda Python runtime.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class IngestionConfig:
    """Holds and validates all runtime settings."""

    api_base_url: str
    api_endpoint: str
    access_key_secret: str
    raw_bucket: str
    raw_prefix: str
    base_currency: str = "USD"
    currencies: str = "CAD,MXN,USD"
    request_timeout: int = 30

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        config = cls(
            api_base_url=os.environ.get("API_BASE_URL", "https://api.freecurrencyapi.com/v1"),
            api_endpoint=os.environ.get("API_ENDPOINT", "latest"),
            access_key_secret=os.environ.get("ACCESS_KEY_SECRET", "prod/travelProject/exchangeRatesApi"),
            raw_bucket=os.environ.get("RAW_BUCKET", "").strip(),
            raw_prefix=os.environ.get("RAW_PREFIX", "").strip(),
            base_currency=os.environ.get("BASE_CURRENCY", "USD").strip().upper(),
            currencies=os.environ.get("CURRENCIES", "CAD,MXN,USD").replace(" ", "").upper(),
            request_timeout=int(os.environ.get("REQUEST_TIMEOUT", "30")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "API_BASE_URL": self.api_base_url,
            "API_ENDPOINT": self.api_endpoint,
            "ACCESS_KEY_SECRET": self.access_key_secret,
            "RAW_BUCKET": self.raw_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    @property
    def endpoint_url(self) -> str:
        return f"{self.api_base_url.rstrip('/')}/{self.api_endpoint.strip('/')}"

    @property
    def query_params(self) -> Dict[str, str]:
        """Optional filters; empty values are left out."""
        params = {"base_currency": self.base_currency, "currencies": self.currencies}
        return {k: v for k, v in params.items() if v}


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
class SecretProvider:
    """Fetches (and caches) the access key from Secrets Manager."""

    def __init__(self, secret_id: str, client=None):
        self._secret_id = secret_id
        self._client = client or boto3.client("secretsmanager")
        self._cached: Optional[str] = None

    def get(self) -> str:
        if self._cached is None:
            raw = self._client.get_secret_value(SecretId=self._secret_id)["SecretString"].strip()
            self._cached = self._extract_key(raw)
        return self._cached

    @staticmethod
    def _extract_key(raw: str) -> str:
        """Secrets Manager can store either a plain string or a JSON
        key/value pair (e.g. {"apikey": "..."}) depending on how the
        secret was created in the console. Support both."""
        if raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except ValueError:
                return raw
            if isinstance(parsed, dict) and len(parsed) == 1:
                return next(iter(parsed.values())).strip()
            if isinstance(parsed, dict):
                for name in ("apikey", "api_key", "access_key"):
                    if name in parsed:
                        return parsed[name].strip()
        return raw


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
class HeaderKeyAuthenticator:
    """freecurrencyapi authenticates with an "apikey" header on every request."""

    def __init__(self, secrets: SecretProvider, header_name: str = "apikey"):
        self._secrets = secrets
        self._header_name = header_name

    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        return {**headers, self._header_name: self._secrets.get()}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class ExchangeRatesClient:
    """Performs the GET request and returns the raw response bytes."""

    # Cloudflare in front of freecurrencyapi rejects urllib's default
    # "Python-urllib/3.x" User-Agent with a 403 (error 1010), so send our own.
    HEADERS = {
        "Accept": "application/json",
        "User-Agent": "travel-app-data-engineering/1.0 (+aws-lambda)",
    }

    def __init__(self, endpoint_url: str, authenticator: HeaderKeyAuthenticator, timeout: int):
        self._endpoint_url = endpoint_url
        self._authenticator = authenticator
        self._timeout = timeout

    def fetch(self, params: Dict[str, str]) -> bytes:
        query = urllib.parse.urlencode(params)
        headers = self._authenticator.apply(self.HEADERS)
        request = urllib.request.Request(
            f"{self._endpoint_url}?{query}",
            headers=headers,
            method="GET",
        )
        # DEBUG: log what is actually sent, with the API key redacted.
        logger.debug(
            "GET %s?%s headers=%s",
            self._endpoint_url,
            query,
            {k: (f"<redacted len={len(v)}>" if k.lower() == "apikey" else v) for k, v in headers.items()},
        )
        # The key travels in a header, never the URL, so it never hits CloudWatch.
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                logger.info("GET %s -> %s (%d bytes)", self._endpoint_url, response.status, len(body))
                return body
        except urllib.error.HTTPError as err:
            body = err.read()[:1000]
            logger.error("HTTP %s from %s: %s", err.code, self._endpoint_url, body)
            # DEBUG: tell a Cloudflare block apart from a real API auth/quota error.
            logger.debug("Response headers: server=%s cf-ray=%s",
                         err.headers.get("server"), err.headers.get("cf-ray"))
            if b"cloudflare_error" in body:
                logger.error("Blocked by Cloudflare before reaching the API (check User-Agent / source IP), "
                             "not an API key problem")
            raise
        except urllib.error.URLError as err:
            logger.error("Could not reach %s: %s", self._endpoint_url, err.reason)
            raise


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class ApiResponseError(Exception):
    """Raised when the API returns an error payload."""


class ResponseValidator:
    """
    freecurrencyapi signals errors with non-2xx statuses (raised by the client),
    but still check the 200 body has a non-empty {"data": {CODE: rate}} map so
    bad data never lands in the raw bucket and the Lambda/Step Function fails loudly.
    """

    def validate(self, body: bytes) -> Dict:
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(f"Response is not valid JSON: {body[:200]!r}") from err

        if not isinstance(payload, dict):
            raise ApiResponseError(f"Unexpected response shape: {body[:200]!r}")
        if "message" in payload and "data" not in payload:
            raise ApiResponseError(f"API error: {payload.get('message')} {payload.get('errors', '')}".strip())
        rates = payload.get("data")
        if not isinstance(rates, dict) or not rates:
            raise ApiResponseError("Response has no 'data' rates")
        return payload


# ---------------------------------------------------------------------------
# S3 storage
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class IngestionRun:
    """Identifies one execution."""

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class RawObjectKeyBuilder:
    """Builds Hive-style partitioned keys for Glue/Athena."""

    def __init__(self, prefix: str, base_currency: str):
        self._prefix = prefix.rstrip("/")
        self._base_currency = base_currency or "UNKNOWN"

    def build(self, run: IngestionRun) -> str:
        # The API response carries no date or base, so use the run time and config.
        rate_date = f"{run.started_at:%Y-%m-%d}"
        base = self._base_currency
        return (
            f"{self._prefix}/ingest_date={run.started_at:%Y-%m-%d}/"
            f"rates_{base}_{rate_date}_{run.started_at:%H%M%S}_{run.run_id}.json"
        )


class S3RawWriter:
    """Writes raw bytes to S3 unchanged."""

    def __init__(self, bucket: str, client=None):
        self._bucket = bucket
        self._client = client or boto3.client("s3")

    @property
    def bucket(self) -> str:
        return self._bucket

    def write(self, key: str, body: bytes) -> str:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            # ServerSideEncryption="aws:kms", SSEKMSKeyId="<YOUR_KMS_KEY_ARN>",  # optional
        )
        return key


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
class IngestionService:
    """Coordinates fetch -> validate -> name -> store."""

    def __init__(
        self,
        client: ExchangeRatesClient,
        validator: ResponseValidator,
        key_builder: RawObjectKeyBuilder,
        writer: S3RawWriter,
        default_params: Dict[str, str],
    ):
        self._client = client
        self._validator = validator
        self._key_builder = key_builder
        self._writer = writer
        self._default_params = default_params

    def run(self, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        run = IngestionRun()
        params = {**self._default_params, **(overrides or {})}
        body = self._client.fetch(params)
        payload = self._validator.validate(body)
        key = self._writer.write(self._key_builder.build(run), body)

        logger.info("Wrote %d rates to s3://%s/%s", len(payload["data"]), self._writer.bucket, key)
        return {
            "status": "SUCCEEDED",
            "bucket": self._writer.bucket,
            "key": key,
            "base": params.get("base_currency"),
            "rate_date": f"{run.started_at:%Y-%m-%d}",
            "rate_count": len(payload["data"]),
            "run_id": run.run_id,
            "ingest_date": f"{run.started_at:%Y-%m-%d}",
        }


def build_service(config: IngestionConfig) -> IngestionService:
    """Composition root: the only place that wires the pieces together."""
    return IngestionService(
        client=ExchangeRatesClient(
            config.endpoint_url,
            HeaderKeyAuthenticator(SecretProvider(config.access_key_secret)),
            config.request_timeout,
        ),
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(config.raw_prefix, config.base_currency),
        writer=S3RawWriter(config.raw_bucket),
        default_params=config.query_params,
    )


_service: Optional[IngestionService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(IngestionConfig.from_env())

    # Optional per-run overrides from Step Functions, e.g. {"query_params": {"currencies": "CAD,MXN"}}
    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(overrides)
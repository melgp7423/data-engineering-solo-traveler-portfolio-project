"""
Lambda: ingest exchange rates from exchangeratesapi.io (apilayer) and land the
raw JSON response in S3 — OOP / Single Responsibility Principle.

Classes and their one job:
    IngestionConfig          -> read & validate settings
    SecretProvider           -> fetch the access key from Secrets Manager
    QueryParamKeyAuthenticator -> add ?access_key=... to the request
    ExchangeRatesClient      -> make the HTTP call
    ResponseValidator        -> reject API error payloads (the API returns
                                errors as {"success": false, ...})
    RawObjectKeyBuilder      -> name the S3 object
    S3RawWriter              -> write bytes to S3
    IngestionService         -> orchestrate fetch -> validate -> store
    lambda_handler           -> Lambda entry point

NOTE: the docs sample calls docs.apilayer.com/.../proxy/... — that's the
documentation "try it" proxy. Production code calls the API directly:
    https://api.exchangeratesapi.io/v1/<endpoint>

Environment variables (placeholders shown):
    API_BASE_URL      = https://api.exchangeratesapi.io/v1   (try http:// if your plan lacks HTTPS)
    API_ENDPOINT      = latest            latest | YYYY-MM-DD (historical)
    ACCESS_KEY_SECRET = <SECRETS_MANAGER_SECRET_ID>   secret value = your access key
    BASE_CURRENCY     = <OPTIONAL e.g. EUR>   free plan supports EUR only
    SYMBOLS           = <OPTIONAL e.g. USD,GBP,JPY>   blank = all currencies
    RAW_BUCKET        = <YOUR_RAW_BUCKET_NAME>
    RAW_PREFIX        = exchange_rates/
    REQUEST_TIMEOUT   = 30
    test

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
logger.setLevel(logging.INFO)


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
    base_currency: str = ""
    symbols: str = ""
    request_timeout: int = 30

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        config = cls(
            api_base_url=os.environ.get("API_BASE_URL", "https://api.exchangeratesapi.io/v1"),
            api_endpoint=os.environ.get("API_ENDPOINT", "latest"),
            access_key_secret=os.environ.get("ACCESS_KEY_SECRET", "prod/travelProject/exchangeRates"),
            raw_bucket=os.environ.get("RAW_BUCKET", "exchange-rates-bucket-raw-data-075996947402-us-east-2-an"),
            raw_prefix=os.environ.get("RAW_PREFIX", "exchange_rates/"),
            base_currency=os.environ.get("BASE_CURRENCY", "").strip().upper(),
            symbols=os.environ.get("SYMBOLS", "").replace(" ", "").upper(),
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
        params = {"base": self.base_currency, "symbols": self.symbols}
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
        key/value pair (e.g. {"access_key": "..."}) depending on how the
        secret was created in the console. Support both."""
        if raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except ValueError:
                return raw
            if isinstance(parsed, dict) and len(parsed) == 1:
                return next(iter(parsed.values())).strip()
            if isinstance(parsed, dict) and "access_key" in parsed:
                return parsed["access_key"].strip()
        return raw


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
class QueryParamKeyAuthenticator:
    """apilayer APIs authenticate with ?access_key=<key> on every request."""

    def __init__(self, secrets: SecretProvider, param_name: str = "access_key"):
        self._secrets = secrets
        self._param_name = param_name

    def apply(self, params: Dict[str, str]) -> Dict[str, str]:
        return {**params, self._param_name: self._secrets.get()}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class ExchangeRatesClient:
    """Performs the GET request and returns the raw response bytes."""

    HEADERS = {"Accept": "application/json"}

    def __init__(self, endpoint_url: str, authenticator: QueryParamKeyAuthenticator, timeout: int):
        self._endpoint_url = endpoint_url
        self._authenticator = authenticator
        self._timeout = timeout

    def fetch(self, params: Dict[str, str]) -> bytes:
        query = urllib.parse.urlencode(self._authenticator.apply(params))
        request = urllib.request.Request(
            f"{self._endpoint_url}?{query}", headers=self.HEADERS, method="GET"
        )
        # Log the URL WITHOUT the query string so the access key never hits CloudWatch.
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                logger.info("GET %s -> %s (%d bytes)", self._endpoint_url, response.status, len(body))
                return body
        except urllib.error.HTTPError as err:
            logger.error("HTTP %s from %s: %s", err.code, self._endpoint_url, err.read()[:500])
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
    exchangeratesapi.io can return HTTP 200 with {"success": false, "error": {...}}
    (e.g. bad key, quota exceeded, feature not on plan). Catch that so bad
    data never lands in the raw bucket and the Lambda/Step Function fails loudly.
    """

    def validate(self, body: bytes) -> Dict:
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(f"Response is not valid JSON: {body[:200]!r}") from err

        if not payload.get("success", False):
            error = payload.get("error", {})
            raise ApiResponseError(
                f"API error {error.get('code')} ({error.get('type')}): {error.get('info')}"
            )
        if "rates" not in payload:
            raise ApiResponseError("Response has no 'rates' field")
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

    def __init__(self, prefix: str):
        self._prefix = prefix.rstrip("/")

    def build(self, run: IngestionRun, payload: Dict) -> str:
        rate_date = payload.get("date", f"{run.started_at:%Y-%m-%d}")
        base = payload.get("base", "UNKNOWN")
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
        body = self._client.fetch({**self._default_params, **(overrides or {})})
        payload = self._validator.validate(body)
        key = self._writer.write(self._key_builder.build(run, payload), body)

        logger.info("Wrote %d rates to s3://%s/%s", len(payload["rates"]), self._writer.bucket, key)
        return {
            "status": "SUCCEEDED",
            "bucket": self._writer.bucket,
            "key": key,
            "base": payload.get("base"),
            "rate_date": payload.get("date"),
            "rate_count": len(payload["rates"]),
            "run_id": run.run_id,
            "ingest_date": f"{run.started_at:%Y-%m-%d}",
        }


def build_service(config: IngestionConfig) -> IngestionService:
    """Composition root: the only place that wires the pieces together."""
    return IngestionService(
        client=ExchangeRatesClient(
            config.endpoint_url,
            QueryParamKeyAuthenticator(SecretProvider(config.access_key_secret)),
            config.request_timeout,
        ),
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(config.raw_prefix),
        writer=S3RawWriter(config.raw_bucket),
        default_params=config.query_params,
    )


_service: Optional[IngestionService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(IngestionConfig.from_env())

    # Optional per-run overrides from Step Functions, e.g. {"query_params": {"symbols": "USD,GBP"}}
    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(overrides)
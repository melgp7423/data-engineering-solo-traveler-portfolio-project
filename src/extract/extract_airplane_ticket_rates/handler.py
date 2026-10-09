"""
Lambda: ingest Google Flights trip deals from SerpApi and land the raw JSON
response in S3 — OOP / Single Responsibility Principle.

Docs: https://serpapi.com/google-flights-deals-api

Classes and their one job:
    IngestionConfig          -> read & validate settings
    OutboundDateCalculator   -> compute outbound_date (today + N days)
    DealsQueryBuilder        -> build the round-trip query params for one run
    SecretProvider           -> fetch the API key from Secrets Manager
    QueryParamKeyAuthenticator -> add ?api_key=... to the request
    FlightDealsClient        -> make the HTTP call
    ResponseValidator        -> reject API error payloads (SerpApi returns
                                errors as {"error": "..."})
    RawObjectKeyBuilder      -> name the S3 object
    S3RawWriter              -> write bytes to S3
    IngestionService         -> orchestrate build query -> fetch -> validate -> store
    lambda_handler           -> Lambda entry point
    test

Environment variables (placeholders shown):
    API_URL                                         = https://serpapi.com/search.json
    API_KEY_SECRET                                  = <SECRETS_MANAGER_SECRET_ID>   secret value = your SerpApi key
    DEPARTURE_ID                                    = PHX   airport code / kgmid of the origin
    OUTBOUND_OFFSET_DAYS                            = 7     outbound_date = today (UTC) + this many days
    TRIP_LENGTH_DAYS                                = 14    return_date = outbound_date + this many days
    ADULTS                                          = 1
    CURRENCY                                        = USD
    AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET          = <YOUR_RAW_BUCKET_NAME>
    RAW_PREFIX                                      = airplane_ticket_rates/
    REQUEST_TIMEOUT                                 = 60

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
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class IngestionConfig:
    """Holds and validates all runtime settings."""

    api_url: str
    api_key_secret: str
    aws_airplane_ticket_raw_data_s3_bucket: str
    raw_prefix: str
    departure_id: str = "PHX"
    outbound_offset_days: int = 7
    trip_length_days: int = 7
    adults: int = 1
    currency: str = "USD"
    request_timeout: int = 60

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        config = cls(
            api_url=os.environ.get("API_URL", "https://serpapi.com/search.json"),
            api_key_secret=os.environ.get("API_KEY_SECRET", "prod/travelProject/serpApi"),
            aws_airplane_ticket_raw_data_s3_bucket=os.environ.get("AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET", "").strip(),
            raw_prefix=os.environ.get("RAW_PREFIX", "airplane_ticket_rates/"),
            departure_id=os.environ.get("DEPARTURE_ID", "PHX").strip().upper(),
            outbound_offset_days=int(os.environ.get("OUTBOUND_OFFSET_DAYS", "7")),
            trip_length_days=int(os.environ.get("TRIP_LENGTH_DAYS", "7")),
            adults=int(os.environ.get("ADULTS", "1")),
            currency=os.environ.get("CURRENCY", "USD").strip().upper(),
            request_timeout=int(os.environ.get("REQUEST_TIMEOUT", "60")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "API_URL": self.api_url,
            "API_KEY_SECRET": self.api_key_secret,
            "AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET": self.aws_airplane_ticket_raw_data_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        if self.trip_length_days < 1:
            raise ValueError("TRIP_LENGTH_DAYS must be at least 1")
        if self.adults < 1:
            raise ValueError("ADULTS must be at least 1")
        if self.outbound_offset_days < 0:
            raise ValueError("OUTBOUND_OFFSET_DAYS cannot be negative")


# ---------------------------------------------------------------------------
# Query parameters
# ---------------------------------------------------------------------------
class OutboundDateCalculator:
    """Computes the outbound date as today + offset_days. The clock is
    injectable so tests can pin 'today'."""

    def __init__(self, offset_days: int, today: Callable[[], date] = None):
        self._offset_days = offset_days
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def calculate(self) -> date:
        return self._today() + timedelta(days=self._offset_days)


class DealsQueryBuilder:
    """
    Builds the round-trip SerpApi query for one run. The outbound date is
    recalculated on every call — warm Lambda containers are reused across
    days, so it must never be computed once at cold start.

    type=1 requests round trips explicitly, and an exact return_date pins
    the return leg (SerpApi rejects return_date on one-way searches and
    doesn't allow it alongside travel_duration).
    """

    ENGINE = "google_flights_deals"
    ROUND_TRIP = "1"

    def __init__(self, config: IngestionConfig, date_calculator: OutboundDateCalculator):
        self._config = config
        self._date_calculator = date_calculator

    def build(self) -> Dict[str, str]:
        outbound_date = self._date_calculator.calculate()
        return_date = outbound_date + timedelta(days=self._config.trip_length_days)
        params = {
            "engine": self.ENGINE,
            "type": self.ROUND_TRIP,
            "outbound_date": outbound_date.isoformat(),
            "return_date": return_date.isoformat(),
            "adults": str(self._config.adults),
            "currency": self._config.currency,
            "departure_id": self._config.departure_id,
        }
        return {k: v for k, v in params.items() if v}


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
class SecretProvider:
    """Fetches (and caches) the API key from Secrets Manager."""

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
        key/value pair (e.g. {"api_key": "..."}) depending on how the
        secret was created in the console. Support both."""
        if raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except ValueError:
                return raw
            if isinstance(parsed, dict) and len(parsed) == 1:
                return next(iter(parsed.values())).strip()
            if isinstance(parsed, dict) and "api_key" in parsed:
                return parsed["api_key"].strip()
        return raw


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
class QueryParamKeyAuthenticator:
    """SerpApi authenticates with ?api_key=<key> on every request."""

    def __init__(self, secrets: SecretProvider, param_name: str = "api_key"):
        self._secrets = secrets
        self._param_name = param_name

    def apply(self, params: Dict[str, str]) -> Dict[str, str]:
        return {**params, self._param_name: self._secrets.get()}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class FlightDealsClient:
    """Performs the GET request and returns the raw response bytes."""

    HEADERS = {"Accept": "application/json"}

    def __init__(self, api_url: str, authenticator: QueryParamKeyAuthenticator, timeout: int):
        self._api_url = api_url
        self._authenticator = authenticator
        self._timeout = timeout

    def fetch(self, params: Dict[str, str]) -> bytes:
        query = urllib.parse.urlencode(self._authenticator.apply(params))
        request = urllib.request.Request(
            f"{self._api_url}?{query}", headers=self.HEADERS, method="GET"
        )
        # Log the URL WITHOUT the query string so the API key never hits CloudWatch.
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                logger.info("GET %s -> %s (%d bytes)", self._api_url, response.status, len(body))
                return body
        except urllib.error.HTTPError as err:
            logger.error("HTTP %s from %s: %s", err.code, self._api_url, err.read()[:500])
            raise
        except urllib.error.URLError as err:
            logger.error("Could not reach %s: %s", self._api_url, err.reason)
            raise


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class ApiResponseError(Exception):
    """Raised when the API returns an error payload."""


class ResponseValidator:
    """
    SerpApi reports failures as {"error": "..."} and sets
    search_metadata.status to "Error". Catch that so bad data never lands
    in the raw bucket and the Lambda/Step Function fails loudly.
    """

    def validate(self, body: bytes) -> Dict:
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(f"Response is not valid JSON: {body[:200]!r}") from err

        if "error" in payload:
            raise ApiResponseError(f"API error: {payload['error']}")
        status = payload.get("search_metadata", {}).get("status")
        if status != "Success":
            raise ApiResponseError(f"Search did not succeed (status={status!r})")
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

    def build(self, run: IngestionRun, params: Dict[str, str]) -> str:
        departure = params.get("departure_id", "ANY")
        outbound_date = params.get("outbound_date", f"{run.started_at:%Y-%m-%d}")
        return_date = params.get("return_date", "OPEN")
        return (
            f"{self._prefix}/ingest_date={run.started_at:%Y-%m-%d}/"
            f"deals_{departure}_{outbound_date}_{return_date}_{run.started_at:%H%M%S}_{run.run_id}.json"
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
    """Coordinates build query -> fetch -> validate -> name -> store."""

    def __init__(
        self,
        query_builder: DealsQueryBuilder,
        client: FlightDealsClient,
        validator: ResponseValidator,
        key_builder: RawObjectKeyBuilder,
        writer: S3RawWriter,
    ):
        self._query_builder = query_builder
        self._client = client
        self._validator = validator
        self._key_builder = key_builder
        self._writer = writer

    def run(self, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        run = IngestionRun()
        params = {**self._query_builder.build(), **(overrides or {})}
        body = self._client.fetch(params)
        payload = self._validator.validate(body)
        key = self._writer.write(self._key_builder.build(run, params), body)

        logger.info("Wrote flight deals to s3://%s/%s", self._writer.bucket, key)
        return {
            "status": "SUCCEEDED",
            "bucket": self._writer.bucket,
            "key": key,
            "search_id": payload.get("search_metadata", {}).get("id"),
            "trip_type": params.get("type"),
            "outbound_date": params.get("outbound_date"),
            "return_date": params.get("return_date"),
            "adults": params.get("adults"),
            "run_id": run.run_id,
            "ingest_date": f"{run.started_at:%Y-%m-%d}",
        }


def build_service(config: IngestionConfig) -> IngestionService:
    """Composition root: the only place that wires the pieces together."""
    return IngestionService(
        query_builder=DealsQueryBuilder(
            config, OutboundDateCalculator(config.outbound_offset_days)
        ),
        client=FlightDealsClient(
            config.api_url,
            QueryParamKeyAuthenticator(SecretProvider(config.api_key_secret)),
            config.request_timeout,
        ),
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(config.raw_prefix),
        writer=S3RawWriter(config.aws_airplane_ticket_raw_data_s3_bucket),
    )


_service: Optional[IngestionService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(IngestionConfig.from_env())

    # Optional per-run overrides from Step Functions, e.g. {"query_params": {"departure_id": "JFK"}}
    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(overrides)

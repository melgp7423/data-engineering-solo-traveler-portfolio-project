"""
Lambda: ingest Google Hotels results from SerpApi for the destinations found
by ingest_airplane_ticket_rates, and land each raw JSON response in S3 —
OOP / Single Responsibility Principle.

Docs: https://serpapi.com/google-hotels-api

How it correlates with the flights Lambda:
    1. ingest_airplane_ticket_rates writes today's PHX flight deals to
       s3://<FLIGHT_DEALS_BUCKET>/airplane_ticket_rates/ingest_date=YYYY-MM-DD/...
    2. This Lambda reads that file. It uses the S3 key passed in the event
       (the flights Lambda's output, when chained in Step Functions), or else
       the newest file in TODAY's partition. It never falls back to an older
       day, so hotel dates can't drift from the flight dates.
    3. Each unique deal destination (e.g. "Cancun, Mexico") becomes the `q`
       of one hotel search. MAX_DESTINATIONS caps the number of searches.

Classes and their one job:
    IngestionConfig            -> read & validate settings
    StayDatesCalculator        -> compute check-in / check-out (today + N days)
    FlightDealsLocator         -> find which flight-deals S3 object to use
    FlightDealsReader          -> load and parse that object
    DestinationExtractor       -> turn flight deals into unique destinations
    HotelQueryBuilder          -> build the query params for one destination
    SecretProvider             -> fetch the API key from Secrets Manager
    QueryParamKeyAuthenticator -> add ?api_key=... to the request
    HotelSearchClient          -> make the HTTP call
    ResponseValidator          -> reject API error payloads
    RawObjectKeyBuilder        -> name the S3 object
    S3RawWriter                -> write bytes to S3
    IngestionService           -> orchestrate read deals -> search each destination -> store
    lambda_handler             -> Lambda entry point
    test

Environment variables (placeholders shown):
    API_URL                                             = https://serpapi.com/search.json
    API_KEY_SECRET                                      = prod/travelProject/serpApi   secret value = your SerpApi key
    AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET              = bucket the flights Lambda writes to
    FLIGHT_DEALS_PREFIX                                 = airplane_ticket_rates/
    MAX_DESTINATIONS                                    = 50    max hotel searches per run (each costs 1 SerpApi credit)
    CHECK_IN_OFFSET_DAYS                                = 7     check_in_date  = today (UTC) + this many days (matches flight outbound_date)
    CHECK_OUT_OFFSET_DAYS                               = 14    check_out_date = today (UTC) + this many days
    ADULTS                                              = 1
    HOTEL_CLASS                                         = 3,4,5
    CURRENCY                                            = USD
    AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET            = <YOUR_RAW_BUCKET_NAME>
    RAW_PREFIX                                          = hotel_room_rates/
    REQUEST_TIMEOUT                                     = 60

The Lambda timeout must cover MAX_DESTINATIONS sequential API calls (e.g. 5 min).

Only uses libraries built into the Lambda Python runtime.
"""

import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

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
    flight_deals_prefix: str
    aws_hotel_room_rates_raw_data_s3_bucket: str
    raw_prefix: str
    max_destinations: int = 50
    check_in_offset_days: int = 7
    check_out_offset_days: int = 14
    adults: int = 1
    hotel_class: str = "3,4,5"
    currency: str = "USD"
    request_timeout: int = 60

    @classmethod 
    def from_env(cls) -> "IngestionConfig":
        config = cls(
            api_url=os.environ.get("API_URL", "https://serpapi.com/search.json"),
            api_key_secret=os.environ.get("API_KEY_SECRET", "prod/travelProject/serpApi"),
            aws_airplane_ticket_raw_data_s3_bucket=os.environ.get(
                "AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET", ""
            ).strip(),
            flight_deals_prefix=os.environ.get("FLIGHT_DEALS_PREFIX", "airplane_ticket_rates/"),
            aws_hotel_room_rates_raw_data_s3_bucket=os.environ.get("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "").strip(),
            raw_prefix=os.environ.get("RAW_PREFIX", "hotel_room_rates/"),
            max_destinations=int(os.environ.get("MAX_DESTINATIONS", "50")),
            check_in_offset_days=int(os.environ.get("CHECK_IN_OFFSET_DAYS", "7")),
            check_out_offset_days=int(os.environ.get("CHECK_OUT_OFFSET_DAYS", "14")),
            adults=int(os.environ.get("ADULTS", "1")),
            hotel_class=os.environ.get("HOTEL_CLASS", "3,4,5").replace(" ", ""),
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
            "AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET": self.aws_hotel_room_rates_raw_data_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        if not re.fullmatch(r"[2-5](,[2-5])*", self.hotel_class):
            raise ValueError("HOTEL_CLASS must be comma-separated values from 2-5, e.g. 3,4,5")
        if self.adults < 1:
            raise ValueError("ADULTS must be at least 1")
        if self.max_destinations < 1:
            raise ValueError("MAX_DESTINATIONS must be at least 1")
        if not 0 <= self.check_in_offset_days < self.check_out_offset_days:
            raise ValueError("Need 0 <= CHECK_IN_OFFSET_DAYS < CHECK_OUT_OFFSET_DAYS")


# ---------------------------------------------------------------------------
# Stay dates
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StayDates:
    check_in: date
    check_out: date


class StayDatesCalculator:
    """Computes check-in / check-out relative to today. The clock is
    injectable so tests can pin 'today'."""

    def __init__(self, check_in_offset: int, check_out_offset: int, today: Callable[[], date] = None):
        self._check_in_offset = check_in_offset
        self._check_out_offset = check_out_offset
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def calculate(self) -> StayDates:
        today = self._today()
        return StayDates(
            check_in=today + timedelta(days=self._check_in_offset),
            check_out=today + timedelta(days=self._check_out_offset),
        )


# ---------------------------------------------------------------------------
# Flight deals (input from ingest_airplane_ticket_rates)
# ---------------------------------------------------------------------------
class FlightDealsNotFoundError(Exception):
    """Raised when there is no flight-deals file to correlate with."""


@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class FlightDealsLocator:
    """
    Decides which flight-deals object to read:
      * the bucket/key in the event (the flights Lambda's return value), or
      * the newest .json in today's ingest_date partition.
    Older partitions are deliberately ignored so stale destinations are never used.
    """

    def __init__(self, bucket: str, prefix: str, client=None, today: Callable[[], date] = None):
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._client = client or boto3.client("s3")
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def locate(self, event: Optional[Dict] = None) -> S3Location:
        if isinstance(event, dict) and event.get("key"):
            return S3Location(event.get("bucket") or self._bucket, event["key"])
        return self._latest_from_today()

    def _latest_from_today(self) -> S3Location:
        partition = f"{self._prefix}/ingest_date={self._today():%Y-%m-%d}/"
        latest = None
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=partition):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and (
                    latest is None or obj["LastModified"] > latest["LastModified"]
                ):
                    latest = obj
        if latest is None:
            raise FlightDealsNotFoundError(
                f"No flight deals in s3://{self._bucket}/{partition} — "
                "run ingest_airplane_ticket_rates first"
            )
        return S3Location(self._bucket, latest["Key"])


class FlightDealsReader:
    """Loads a flight-deals object from S3 and parses it."""

    def __init__(self, client=None):
        self._client = client or boto3.client("s3")

    def read(self, location: S3Location) -> Dict:
        body = self._client.get_object(Bucket=location.bucket, Key=location.key)["Body"].read()
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise FlightDealsNotFoundError(f"{location} is not valid JSON") from err
        if not isinstance(payload, dict) or not isinstance(payload.get("deals"), list):
            raise FlightDealsNotFoundError(f"{location} has no 'deals' list")
        return payload


@dataclass(frozen=True)
class Destination:
    name: str
    country: str = ""
    airport_code: str = ""
    destination_id: str = ""

    @property
    def search_query(self) -> str:
        return f"{self.name}, {self.country}" if self.country else self.name

    @property
    def label(self) -> str:
        """Short, S3-safe identifier used in object keys."""
        raw = self.airport_code or self.name
        return re.sub(r"[^A-Za-z0-9]+", "-", raw).strip("-") or "UNKNOWN"


class DestinationExtractor:
    """Turns the flight deals into unique destinations, in the API's order,
    capped at max_destinations."""

    def __init__(self, max_destinations: int):
        self._max = max_destinations

    def extract(self, flight_deals: Dict) -> List[Destination]:
        destinations: List[Destination] = []
        seen = set()
        for deal in flight_deals.get("deals", []):
            if not isinstance(deal, dict) or not str(deal.get("name") or "").strip():
                continue
            destination = Destination(
                name=str(deal["name"]).strip(),
                country=str(deal.get("country") or "").strip(),
                airport_code=str(deal.get("arrival_airport_code") or "").strip().upper(),
                destination_id=str(deal.get("destination_id") or "").strip(),
            )
            identity = destination.destination_id or destination.search_query.lower()
            if identity in seen:
                continue
            seen.add(identity)
            destinations.append(destination)
            if len(destinations) == self._max:
                break
        return destinations


# ---------------------------------------------------------------------------
# Query parameters
# ---------------------------------------------------------------------------
class HotelQueryBuilder:
    """Builds the SerpApi Google Hotels query for one destination."""

    ENGINE = "google_hotels"

    def __init__(self, config: IngestionConfig):
        self._config = config

    def build(self, destination: Destination, stay: StayDates) -> Dict[str, str]:
        return {
            "engine": self.ENGINE,
            "q": destination.search_query,
            "check_in_date": stay.check_in.isoformat(),
            "check_out_date": stay.check_out.isoformat(),
            "adults": str(self._config.adults),
            "hotel_class": self._config.hotel_class,
            "currency": self._config.currency,
        }


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
class HotelSearchClient:
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
    in the raw bucket.
    """

    def validate(self, body: bytes) -> Dict:
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(f"Response is not valid JSON: {body[:200]!r}") from err

        if not isinstance(payload, dict):
            raise ApiResponseError(f"Expected a JSON object, got {type(payload).__name__}")
        if "error" in payload:
            raise ApiResponseError(f"API error: {payload['error']}")
        metadata = payload.get("search_metadata")
        status = metadata.get("status") if isinstance(metadata, dict) else None
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

    def build(self, run: IngestionRun, destination: Destination, stay: StayDates) -> str:
        return (
            f"{self._prefix}/ingest_date={run.started_at:%Y-%m-%d}/"
            f"hotels_{destination.label}_{stay.check_in:%Y-%m-%d}_{stay.check_out:%Y-%m-%d}_"
            f"{run.started_at:%H%M%S}_{run.run_id}.json"
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
class IngestionError(Exception):
    """Raised when every hotel search in a run failed."""


class IngestionService:
    """Coordinates read flight deals -> extract destinations -> search each -> store."""

    def __init__(
        self,
        dates: StayDatesCalculator,
        reader: FlightDealsReader,
        extractor: DestinationExtractor,
        query_builder: HotelQueryBuilder,
        client: HotelSearchClient,
        validator: ResponseValidator,
        key_builder: RawObjectKeyBuilder,
        writer: S3RawWriter,
    ):
        self._dates = dates
        self._reader = reader
        self._extractor = extractor
        self._query_builder = query_builder
        self._client = client
        self._validator = validator
        self._key_builder = key_builder
        self._writer = writer

    def run(self, source: S3Location, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        run = IngestionRun()
        stay = self._dates.calculate()
        destinations = self._extractor.extract(self._reader.read(source))
        logger.info("Found %d destinations in %s", len(destinations), source)

        written, failed = [], []
        for destination in destinations:
            params = {**self._query_builder.build(destination, stay), **(overrides or {})}
            try:
                body = self._client.fetch(params)
                self._validator.validate(body)
            except (ApiResponseError, urllib.error.URLError) as err:
                # One bad destination shouldn't throw away the others.
                logger.error("Hotel search failed for %r: %s", destination.search_query, err)
                failed.append({"destination": destination.search_query, "error": str(err)})
                continue
            key = self._writer.write(self._key_builder.build(run, destination, stay), body)
            written.append({"destination": destination.search_query, "key": key})

        if destinations and not written:
            raise IngestionError(f"All {len(failed)} hotel searches failed: {failed}")

        logger.info("Wrote %d hotel result files to s3://%s", len(written), self._writer.bucket)
        return {
            "status": "PARTIAL" if failed else "SUCCEEDED",
            "bucket": self._writer.bucket,
            "source_flight_deals": str(source),
            "check_in_date": stay.check_in.isoformat(),
            "check_out_date": stay.check_out.isoformat(),
            "destination_count": len(destinations),
            "written": written,
            "failed": failed,
            "run_id": run.run_id,
            "ingest_date": f"{run.started_at:%Y-%m-%d}",
        }


def build_locator(config: IngestionConfig) -> FlightDealsLocator:
    return FlightDealsLocator(config.aws_airplane_ticket_raw_data_s3_bucket, config.flight_deals_prefix)


def build_service(config: IngestionConfig) -> IngestionService:
    """Composition root: the only place that wires the pieces together."""
    return IngestionService(
        dates=StayDatesCalculator(config.check_in_offset_days, config.check_out_offset_days),
        reader=FlightDealsReader(),
        extractor=DestinationExtractor(config.max_destinations),
        query_builder=HotelQueryBuilder(config),
        client=HotelSearchClient(
            config.api_url,
            QueryParamKeyAuthenticator(SecretProvider(config.api_key_secret)),
            config.request_timeout,
        ),
        validator=ResponseValidator(),
        key_builder=RawObjectKeyBuilder(config.raw_prefix),
        writer=S3RawWriter(config.aws_hotel_room_rates_raw_data_s3_bucket),
    )


_locator: Optional[FlightDealsLocator] = None
_service: Optional[IngestionService] = None


def lambda_handler(event, context):
    global _locator, _service
    if _service is None:
        config = IngestionConfig.from_env()
        _locator = build_locator(config)
        _service = build_service(config)

    # Chained after the flights Lambda in Step Functions, `event` is its output
    # ({"bucket": ..., "key": ...}); on a schedule it's empty and today's newest
    # flight-deals file is used. Optional {"query_params": {...}} overrides the search.
    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(_locator.locate(event), overrides)

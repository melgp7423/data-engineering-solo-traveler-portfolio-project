"""
Lambda: ingest tourist activities from the Geoapify Places API for the
destinations found by ingest_hotel_room_rates, and land the raw JSON
responses in S3 — OOP / Single Responsibility Principle.

Docs: https://apidocs.geoapify.com/docs/places/
      https://apidocs.geoapify.com/docs/geocoding/batch/

How it correlates with the hotels Lambda:
    1. ingest_hotel_room_rates searches hotels for each flight-deal destination
       and returns {"written": [{"destination": "Cancun, Mexico", ...}, ...]}.
    2. This Lambda takes those destination names from the event (when chained
       in Step Functions), or else from the hotel files in TODAY's partition
       (each file's search_parameters.q).
    3. All names are forward-geocoded to decimal-degree lat/lon in ONE batch
       geocoding request (async job: submit, then poll until it finishes).
    4. A square of AREA_RADIUS_METERS around each city is built, and all squares
       are sent as a single inline GeoJSON MultiPolygon filter in ONE Places
       POST request (paged with offset only if results exceed PLACES_LIMIT).

Classes and their one job:
    IngestionConfig            -> read & validate settings
    DestinationLocator         -> find the destination names to search
    SecretProvider             -> fetch the API key from Secrets Manager
    QueryParamKeyAuthenticator -> add ?apiKey=... to the request
    GeoapifyClient             -> make the HTTP calls
    BatchGeocoder              -> city names -> decimal-degree coordinates
    AreaPolygonBuilder         -> coordinates -> one GeoJSON MultiPolygon
    PlacesQueryBuilder         -> build the Places POST body
    ResponseValidator          -> reject API error payloads
    RawObjectKeyBuilder        -> name the S3 objects
    S3RawWriter                -> write bytes to S3
    IngestionService           -> orchestrate destinations -> geocode -> places -> store
    lambda_handler             -> Lambda entry point
    test
    
Environment variables (placeholders shown):
    GEOCODE_URL                                             = https://api.geoapify.com/v1/batch/geocode/search
    PLACES_URL                                              = https://api.geoapify.com/v2/places
    API_KEY_SECRET                                          = prod/travelProject/geoapify   secret value = your Geoapify key
    AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET                 = bucket the hotels Lambda writes to
    HOTEL_PREFIX                                            = hotel_room_rates/
    CATEGORIES                                              = tourism,entertainment,leisure,catering.restaurant,beach,natural
    AREA_RADIUS_METERS                                      = 10000  half the side of the square searched around each city
    PLACES_LIMIT                                            = 500    places per page (API max 500)
    MAX_PAGES                                               = 10     safety cap on Places pages per run
    GEOCODE_POLL_SECONDS                                    = 3      wait between batch geocoding status checks
    GEOCODE_MAX_WAIT                                        = 120    give up on the geocoding job after this many seconds
    PLACES_LANG                                             = en     2-letter language code (not LANG, which Lambda sets itself)
    AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET           = <YOUR_RAW_BUCKET_NAME>
    RAW_PREFIX                                              = destination_activities/
    REQUEST_TIMEOUT                                         = 60

The Lambda timeout must cover GEOCODE_MAX_WAIT plus MAX_PAGES Places calls (e.g. 5 min).

Only uses libraries built into the Lambda Python runtime.
"""

import json
import logging
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class IngestionConfig:
    """Holds and validates all runtime settings."""

    geocode_url: str
    places_url: str
    api_key_secret: str
    aws_hotel_room_rates_raw_data_s3_bucket: str
    hotel_prefix: str
    aws_destination_activities_raw_data_s3_bucket: str
    raw_prefix: str
    categories: Tuple[str, ...] = (
     "tourism.attraction.artwork", "tourism.attraction.artwork.mural", "tourism.attraction.viewpoint", "entertainment.culture", "heritage", "natural", "beach", "national_park", "ski"
    )
    
    area_radius_meters: int = 10000
    places_limit: int = 500
    max_pages: int = 10
    geocode_poll_seconds: float = 10-15
    geocode_max_wait: float = 600
    lang: str = "en"
    request_timeout: int = 60

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        config = cls(
            geocode_url=os.environ.get(
                "GEOCODE_URL", "https://api.geoapify.com/v1/batch/geocode/search"
            ),
            places_url=os.environ.get("PLACES_URL", "https://api.geoapify.com/v2/places"),
            api_key_secret=os.environ.get("API_KEY_SECRET", "prod/travelProject/geoapify"),
            aws_hotel_room_rates_raw_data_s3_bucket=os.environ.get("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "").strip(),
            hotel_prefix=os.environ.get("HOTEL_PREFIX", "hotel_room_rates/"),
            aws_destination_activities_raw_data_s3_bucket=os.environ.get("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "").strip(),
            raw_prefix=os.environ.get("RAW_PREFIX", "destination_activities/"),
            categories=tuple(
                c.strip()
                for c in os.environ.get(
                    "CATEGORIES", "tourism.attraction.artwork,tourism.attraction.artwork.mural,tourism.attraction.viewpoint,entertainment.culture,heritage,natural,beach,national_park,ski"
                ).split(",")
                if c.strip()
            ),
            area_radius_meters=int(os.environ.get("AREA_RADIUS_METERS", "10000")),
            places_limit=int(os.environ.get("PLACES_LIMIT", "500")),
            max_pages=int(os.environ.get("MAX_PAGES", "10")),
            geocode_poll_seconds=float(os.environ.get("GEOCODE_POLL_SECONDS", "3")),
            geocode_max_wait=float(os.environ.get("GEOCODE_MAX_WAIT", "120")),
            lang=os.environ.get("PLACES_LANG", "en").strip().lower(),
            request_timeout=int(os.environ.get("REQUEST_TIMEOUT", "60")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "GEOCODE_URL": self.geocode_url,
            "PLACES_URL": self.places_url,
            "API_KEY_SECRET": self.api_key_secret,
            "AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET": self.aws_hotel_room_rates_raw_data_s3_bucket,
            "AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET": self.aws_destination_activities_raw_data_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        if not 1 <= len(self.categories) <= 100:
            raise ValueError("CATEGORIES must list 1-100 Geoapify category keys")
        if not 0 < self.area_radius_meters <= 100000:
            raise ValueError("AREA_RADIUS_METERS must be between 1 and 100000")
        if not 1 <= self.places_limit <= 500:
            raise ValueError("PLACES_LIMIT must be between 1 and 500")
        if self.max_pages < 1:
            raise ValueError("MAX_PAGES must be at least 1")
        if len(self.lang) != 2:
            raise ValueError("PLACES_LANG must be a 2-letter ISO 639-1 code, e.g. en")
        if self.geocode_poll_seconds <= 0 or self.geocode_max_wait <= 0:
            raise ValueError("GEOCODE_POLL_SECONDS and GEOCODE_MAX_WAIT must be positive")


# ---------------------------------------------------------------------------
# Destinations (input from ingest_hotel_room_rates)
# ---------------------------------------------------------------------------
class DestinationsNotFoundError(Exception):
    """Raised when there are no hotel destinations to search around."""


class DestinationLocator:
    """
    Decides which destination names to search:
      * the "written" list in the event (the hotels Lambda's return value), or
      * search_parameters.q of every hotel file in today's ingest_date partition.
    Only destinations whose hotel search succeeded are used, and older
    partitions are ignored so the activities always match today's hotels.
    """

    def __init__(self, bucket: str, prefix: str, client=None, today: Callable[[], date] = None):
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._client = client or boto3.client("s3")
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def locate(self, event: Optional[Dict] = None) -> List[str]:
        if isinstance(event, dict) and isinstance(event.get("written"), list):
            names = [item.get("destination") for item in event["written"] if isinstance(item, dict)]
        else:
            names = self._from_today()
        destinations = self._unique(names)
        if not destinations:
            raise DestinationsNotFoundError(
                "No hotel destinations found — run ingest_hotel_room_rates first"
            )
        return destinations

    def _from_today(self) -> List[str]:
        partition = f"{self._prefix}/ingest_date={self._today():%Y-%m-%d}/"
        names = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=partition):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".json"):
                    continue
                body = self._client.get_object(Bucket=self._bucket, Key=obj["Key"])["Body"].read()
                try:
                    params = json.loads(body).get("search_parameters") or {}
                except (ValueError, AttributeError):
                    logger.warning("Skipping unreadable hotel file s3://%s/%s", self._bucket, obj["Key"])
                    continue
                names.append(params.get("q"))
        return names

    @staticmethod
    def _unique(names: List[Optional[str]]) -> List[str]:
        seen, unique = set(), []
        for name in names:
            name = str(name or "").strip()
            if name and name.lower() not in seen:
                seen.add(name.lower())
                unique.append(name)
        return unique


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
    """Geoapify authenticates with ?apiKey=<key> on every request."""

    def __init__(self, secrets: SecretProvider, param_name: str = "apiKey"):
        self._secrets = secrets
        self._param_name = param_name

    def apply(self, params: Dict[str, str]) -> Dict[str, str]:
        return {**params, self._param_name: self._secrets.get()}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes


class GeoapifyClient:
    """Performs GET / JSON POST requests and returns the status and raw bytes."""

    HEADERS = {"Accept": "application/json"}

    def __init__(self, authenticator: QueryParamKeyAuthenticator, timeout: int):
        self._authenticator = authenticator
        self._timeout = timeout

    def get(self, url: str, params: Dict[str, str]) -> HttpResponse:
        return self._send("GET", url, params)

    def post_json(self, url: str, payload, params: Optional[Dict[str, str]] = None) -> HttpResponse:
        return self._send("POST", url, params or {}, json.dumps(payload).encode("utf-8"))

    def _send(self, method: str, url: str, params: Dict[str, str], data: bytes = None) -> HttpResponse:
        query = urllib.parse.urlencode(self._authenticator.apply(params))
        headers = {**self.HEADERS, **({"Content-Type": "application/json"} if data else {})}
        request = urllib.request.Request(f"{url}?{query}", data=data, headers=headers, method=method)
        # Log the URL WITHOUT the query string so the API key never hits CloudWatch.
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                logger.info("%s %s -> %s (%d bytes)", method, url, response.status, len(body))
                return HttpResponse(response.status, body)
        except urllib.error.HTTPError as err:
            logger.error("HTTP %s from %s %s: %s", err.code, method, url, err.read()[:500])
            raise
        except urllib.error.URLError as err:
            logger.error("Could not reach %s: %s", url, err.reason)
            raise


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class ApiResponseError(Exception):
    """Raised when the API returns an error payload."""


class ResponseValidator:
    """Geoapify reports failures as {"statusCode": ..., "error": ..., "message": ...}.
    Catch that (and anything that isn't the expected shape) so bad data never
    lands in the raw bucket."""

    def parse(self, body: bytes):
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(f"Response is not valid JSON: {body[:200]!r}") from err
        if isinstance(payload, dict) and "error" in payload:
            raise ApiResponseError(f"API error: {payload.get('message') or payload['error']}")
        return payload

    def validate_places(self, body: bytes) -> Dict:
        payload = self.parse(body)
        if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
            raise ApiResponseError("Places response has no 'features' list")
        return payload


# ---------------------------------------------------------------------------
# Forward geocoding (city name -> decimal-degree coordinates)
# ---------------------------------------------------------------------------
class GeocodingError(Exception):
    """Raised when the batch geocoding job fails or never finishes."""


@dataclass(frozen=True)
class GeocodedDestination:
    name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class GeocodeResult:
    destinations: List[GeocodedDestination]
    not_found: List[str]
    raw: bytes


class BatchGeocoder:
    """
    Geocodes every destination in ONE batch job instead of one request per city.
    The batch API is asynchronous: the POST returns 202 + a job id, and the
    same URL is polled with ?id=... until it returns 200 with the results.
    The clock and sleep are injectable so tests don't wait.
    """

    def __init__(
        self,
        client: GeoapifyClient,
        validator: ResponseValidator,
        url: str,
        lang: str,
        poll_seconds: float,
        max_wait: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._client = client
        self._validator = validator
        self._url = url
        self._lang = lang
        self._poll_seconds = poll_seconds
        self._max_wait = max_wait
        self._sleep = sleep
        self._clock = clock

    def geocode(self, names: List[str]) -> GeocodeResult:
        response = self._client.post_json(self._url, names, {"type": "city", "lang": self._lang})
        response = self._wait_for_results(response)
        results = self._validator.parse(response.body)
        if not isinstance(results, list):
            raise GeocodingError(f"Expected a list of geocoding results, got {type(results).__name__}")
        return self._match(names, results, response.body)

    def _wait_for_results(self, response: HttpResponse) -> HttpResponse:
        deadline = self._clock() + self._max_wait
        while response.status == 202:
            job = self._validator.parse(response.body)
            job_id = job.get("id") if isinstance(job, dict) else None
            if not job_id:
                raise GeocodingError(f"Batch geocoding job has no id: {response.body[:200]!r}")
            if self._clock() >= deadline:
                raise GeocodingError(f"Batch geocoding job {job_id} not done after {self._max_wait}s")
            self._sleep(self._poll_seconds)
            response = self._client.get(self._url, {"id": job_id, "format": "json"})
        return response

    @staticmethod
    def _match(names: List[str], results: List, raw: bytes) -> GeocodeResult:
        """Results come back in request order; match on query.text first and
        fall back to position in case the API normalizes the text."""
        by_text = {}
        for result in results:
            if isinstance(result, dict):
                text = str((result.get("query") or {}).get("text") or "").strip().lower()
                by_text.setdefault(text, result)

        found, not_found = [], []
        for index, name in enumerate(names):
            result = by_text.get(name.lower())
            if result is None and index < len(results) and isinstance(results[index], dict):
                result = results[index]
            try:
                found.append(GeocodedDestination(name, float(result["lat"]), float(result["lon"])))
            except (TypeError, KeyError, ValueError):
                not_found.append(name)
        return GeocodeResult(found, not_found, raw)


# ---------------------------------------------------------------------------
# Search area (all destinations -> one inline MultiPolygon)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class BoundingBox:
    west: float
    south: float
    east: float
    north: float

    def overlaps(self, other: "BoundingBox") -> bool:
        return not (
            self.east < other.west or other.east < self.west
            or self.north < other.south or other.north < self.south
        )

    def union(self, other: "BoundingBox") -> "BoundingBox":
        return BoundingBox(
            min(self.west, other.west), min(self.south, other.south),
            max(self.east, other.east), max(self.north, other.north),
        )

    def ring(self) -> List[List[float]]:
        """Closed, counter-clockwise GeoJSON ring of [lon, lat] positions."""
        return [
            [self.west, self.south], [self.east, self.south],
            [self.east, self.north], [self.west, self.north],
            [self.west, self.south],
        ]


class AreaPolygonBuilder:
    """
    Turns each destination's center point into a square of radius_meters
    (half the side) and combines them into ONE GeoJSON MultiPolygon, so a
    single Places request covers every destination. Overlapping squares
    (nearby destinations) are merged, since a valid MultiPolygon can't
    contain overlapping polygons.
    """

    METERS_PER_DEGREE = 111_320

    def __init__(self, radius_meters: int):
        self._radius = radius_meters

    def build(self, destinations: List[GeocodedDestination]) -> Dict:
        boxes = self._merge([self._box(d) for d in destinations])
        return {
            "type": "MultiPolygon",
            "coordinates": [[[[round(v, 6) for v in pos] for pos in box.ring()]] for box in boxes],
        }

    def _box(self, destination: GeocodedDestination) -> BoundingBox:
        d_lat = self._radius / self.METERS_PER_DEGREE
        # Degrees of longitude shrink toward the poles; floor cos() to avoid blow-up.
        d_lon = self._radius / (self.METERS_PER_DEGREE * max(math.cos(math.radians(destination.lat)), 0.01))
        return BoundingBox(
            max(destination.lon - d_lon, -180.0), max(destination.lat - d_lat, -90.0),
            min(destination.lon + d_lon, 180.0), min(destination.lat + d_lat, 90.0),
        )

    @staticmethod
    def _merge(boxes: List[BoundingBox]) -> List[BoundingBox]:
        merged: List[BoundingBox] = []
        for box in boxes:
            # Absorb every existing box this one touches; repeat because the
            # grown box may now touch boxes it didn't before.
            changed = True
            while changed:
                changed = False
                for other in merged:
                    if box.overlaps(other):
                        merged.remove(other)
                        box = box.union(other)
                        changed = True
                        break
            merged.append(box)
        return merged


# ---------------------------------------------------------------------------
# Places query
# ---------------------------------------------------------------------------
class PlacesQueryBuilder:
    """Builds the Places POST body for one page."""

    def __init__(self, config: IngestionConfig):
        self._config = config

    def build(self, area: Dict, offset: int) -> Dict:
        return {
            "categories": list(self._config.categories),
            "filter": {"type": "polygon", "geometry": area},
            "limit": self._config.places_limit,
            "offset": offset,
            "lang": self._config.lang,
        }


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

    def _base(self, run: IngestionRun) -> str:
        return f"{self._prefix}/ingest_date={run.started_at:%Y-%m-%d}"

    def geocode(self, run: IngestionRun) -> str:
        return f"{self._base(run)}/geocode_{run.started_at:%H%M%S}_{run.run_id}.json"

    def places_page(self, run: IngestionRun, page: int) -> str:
        return f"{self._base(run)}/places_{run.started_at:%H%M%S}_{run.run_id}_page{page:03d}.json"


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
    """Raised when no destination could be geocoded."""


class IngestionService:
    """Coordinates geocode destinations -> build area -> page through places -> store."""

    def __init__(
        self,
        geocoder: BatchGeocoder,
        area_builder: AreaPolygonBuilder,
        query_builder: PlacesQueryBuilder,
        client: GeoapifyClient,
        places_url: str,
        validator: ResponseValidator,
        key_builder: RawObjectKeyBuilder,
        writer: S3RawWriter,
        max_pages: int,
    ):
        self._geocoder = geocoder
        self._area_builder = area_builder
        self._query_builder = query_builder
        self._client = client
        self._places_url = places_url
        self._validator = validator
        self._key_builder = key_builder
        self._writer = writer
        self._max_pages = max_pages

    def run(self, names: List[str], overrides: Optional[Dict] = None) -> Dict[str, object]:
        run = IngestionRun()
        geocoded = self._geocoder.geocode(names)
        if geocoded.not_found:
            logger.warning("Could not geocode: %s", geocoded.not_found)
        if not geocoded.destinations:
            raise IngestionError(f"None of the {len(names)} destinations could be geocoded: {names}")
        # Land the raw geocoding result too: the transform step needs it to map
        # each place back to the destination it belongs to.
        geocode_key = self._writer.write(self._key_builder.geocode(run), geocoded.raw)

        area = self._area_builder.build(geocoded.destinations)
        page_keys, place_count = [], 0
        for page in range(self._max_pages):
            body = {**self._query_builder.build(area, offset=place_count), **(overrides or {})}
            raw = self._client.post_json(self._places_url, body).body
            features = self._validator.validate_places(raw)["features"]
            if not features:
                break
            page_keys.append(self._writer.write(self._key_builder.places_page(run, page), raw))
            place_count += len(features)
            if len(features) < body["limit"]:
                break
        else:
            logger.warning("Stopped after MAX_PAGES=%d; more places may exist", self._max_pages)

        logger.info(
            "Wrote %d places for %d destinations to s3://%s",
            place_count, len(geocoded.destinations), self._writer.bucket,
        )
        return {
            "status": "PARTIAL" if geocoded.not_found else "SUCCEEDED",
            "bucket": self._writer.bucket,
            "destinations": [
                {"destination": d.name, "lat": d.lat, "lon": d.lon} for d in geocoded.destinations
            ],
            "not_geocoded": geocoded.not_found,
            "geocode_key": geocode_key,
            "places_keys": page_keys,
            "place_count": place_count,
            "run_id": run.run_id,
            "ingest_date": f"{run.started_at:%Y-%m-%d}",
        }


def build_locator(config: IngestionConfig) -> DestinationLocator:
    return DestinationLocator(config.aws_hotel_room_rates_raw_data_s3_bucket, config.hotel_prefix)


def build_service(config: IngestionConfig) -> IngestionService:
    """Composition root: the only place that wires the pieces together."""
    client = GeoapifyClient(
        QueryParamKeyAuthenticator(SecretProvider(config.api_key_secret)),
        config.request_timeout,
    )
    validator = ResponseValidator()
    return IngestionService(
        geocoder=BatchGeocoder(
            client, validator, config.geocode_url, config.lang,
            config.geocode_poll_seconds, config.geocode_max_wait,
        ),
        area_builder=AreaPolygonBuilder(config.area_radius_meters),
        query_builder=PlacesQueryBuilder(config),
        client=client,
        places_url=config.places_url,
        validator=validator,
        key_builder=RawObjectKeyBuilder(config.raw_prefix),
        writer=S3RawWriter(config.aws_destination_activities_raw_data_s3_bucket),
        max_pages=config.max_pages,
    )


_locator: Optional[DestinationLocator] = None
_service: Optional[IngestionService] = None


def lambda_handler(event, context):
    global _locator, _service
    if _service is None:
        config = IngestionConfig.from_env()
        _locator = build_locator(config)
        _service = build_service(config)

    # Chained after the hotels Lambda in Step Functions, `event` is its output
    # ({"written": [{"destination": ...}, ...]}); on a schedule it's empty and
    # today's hotel files are used. Optional {"query_params": {...}} overrides
    # fields of the Places POST body, e.g. {"categories": ["beach"]}.
    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(_locator.locate(event), overrides)

import json
import logging
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Configuration


@dataclass(frozen=True)
class ExtractConfig:

    geocode_url: str
    places_url: str
    api_key_secret: str
    aws_hotel_room_rates_s3_bucket: str
    hotel_prefix: str
    aws_destination_activities_s3_bucket: str
    raw_prefix: str
    categories: Tuple[str, ...] = (
        "tourism.attraction.artwork",
        "tourism.attraction.artwork.mural",
        "tourism.sights.archaeological_site",
        "entertainment.culture",
        "entertainment.museum",
        "entertainment.planetarium",
        "entertainment.museum",
        "heritage",
        "natural",
        "beach",
        "national_park",
        "ski",
    )
    area_radius_meters: int = 25000
    places_limit: int = 500
    max_pages: int = 10
    geocode_concurrency: int = 4
    lang: str = "en"
    request_timeout: int = 30

    @classmethod
    def from_env(cls) -> "ExtractConfig":
        config = cls(
            geocode_url=os.environ.get(
                "GEOCODE_SEARCH_URL", "https://api.geoapify.com/v1/geocode/search"
            ),
            places_url=os.environ.get(
                "PLACES_URL", "https://api.geoapify.com/v2/places"
            ),
            api_key_secret=os.environ.get(
                "API_KEY_SECRET", "prod/travelProject/geoapify"
            ),
            aws_hotel_room_rates_s3_bucket=os.environ.get(
                "AWS_HOTEL_ROOM_RATES_S3_BUCKET", ""
            ).strip(),
            hotel_prefix=os.environ.get("HOTEL_PREFIX", "").strip(),
            aws_destination_activities_s3_bucket=os.environ.get(
                "AWS_DESTINATION_ACTIVITIES_S3_BUCKET", ""
            ).strip(),
            raw_prefix=os.environ.get("RAW_PREFIX", "").strip(),
            categories=tuple(
                c.strip()
                for c in os.environ.get(
                    "CATEGORIES",
                    "tourism.attraction.artwork,tourism.attraction.artwork.mural,tourism.sights.archaeological_site,entertainment.culture,entertainment.museum,entertainment.planetarium,entertainment.museum,heritage,natural,beach,national_park,ski",
                ).split(",")
                if c.strip()
            ),
            area_radius_meters=int(os.environ.get("AREA_RADIUS_METERS", "25000")),
            places_limit=int(os.environ.get("PLACES_LIMIT", "500")),
            max_pages=int(os.environ.get("MAX_PAGES", "10")),
            geocode_concurrency=int(os.environ.get("GEOCODE_CONCURRENCY", "4")),
            lang=os.environ.get("PLACES_LANG", "en").strip().lower(),
            request_timeout=int(os.environ.get("REQUEST_TIMEOUT", "30")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "GEOCODE_SEARCH_URL": self.geocode_url,
            "PLACES_URL": self.places_url,
            "API_KEY_SECRET": self.api_key_secret,
            "AWS_HOTEL_ROOM_RATES_S3_BUCKET": self.aws_hotel_room_rates_s3_bucket,
            "AWS_DESTINATION_ACTIVITIES_S3_BUCKET": self.aws_destination_activities_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
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
        if not 1 <= self.geocode_concurrency <= 5:
            raise ValueError("GEOCODE_CONCURRENCY must be between 1 and 5")


# Destinations (input from extract_hotel_room_rates)


class DestinationsNotFoundError(Exception):
    """Raised when there are no hotel destinations to search around."""


class DestinationLocator:

    def __init__(
        self, bucket: str, prefix: str, client=None, today: Callable[[], date] = None
    ):
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._client = client or boto3.client("s3")
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def locate(self, event: Optional[Dict] = None) -> List[str]:
        if isinstance(event, dict) and isinstance(event.get("written"), list):
            names = [
                item.get("destination")
                for item in event["written"]
                if isinstance(item, dict)
            ]
        else:
            names = self._from_today()
        destinations = self._unique(names)
        if not destinations:
            raise DestinationsNotFoundError(
                "No hotel destinations found — run extract_hotel_room_rates first"
            )
        return destinations

    def _from_today(self) -> List[str]:
        partition = f"{self._prefix}/extract_date={self._today():%Y-%m-%d}/"
        names = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=partition):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".json"):
                    continue
                body = self._client.get_object(Bucket=self._bucket, Key=obj["Key"])[
                    "Body"
                ].read()
                try:
                    params = json.loads(body).get("search_parameters") or {}
                except (ValueError, AttributeError):
                    logger.warning(
                        "Skipping unreadable hotel file s3://%s/%s",
                        self._bucket,
                        obj["Key"],
                    )
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


# Secrets


class SecretProvider:

    def __init__(self, secret_id: str, client=None):
        self._secret_id = secret_id
        self._client = client or boto3.client("secretsmanager")
        self._cached: Optional[str] = None

    def get(self) -> str:
        if self._cached is None:
            raw = self._client.get_secret_value(SecretId=self._secret_id)[
                "SecretString"
            ].strip()
            self._cached = self._extract_key(raw)
        return self._cached

    @staticmethod
    def _extract_key(raw: str) -> str:

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


# Authentication


class QueryParamKeyAuthenticator:

    def __init__(self, secrets: SecretProvider, param_name: str = "apiKey"):
        self._secrets = secrets
        self._param_name = param_name

    def apply(self, params: Dict[str, str]) -> Dict[str, str]:
        return {**params, self._param_name: self._secrets.get()}


# HTTP


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes


class GeoapifyClient:

    HEADERS = {"Accept": "application/json"}

    def __init__(self, authenticator: QueryParamKeyAuthenticator, timeout: int):
        self._authenticator = authenticator
        self._timeout = timeout

    def get(self, url: str, params: Dict[str, str]) -> HttpResponse:
        return self._send("GET", url, params)

    def post_json(
        self, url: str, payload, params: Optional[Dict[str, str]] = None
    ) -> HttpResponse:
        return self._send(
            "POST", url, params or {}, json.dumps(payload).encode("utf-8")
        )

    def _send(
        self, method: str, url: str, params: Dict[str, str], data: bytes = None
    ) -> HttpResponse:
        query = urllib.parse.urlencode(self._authenticator.apply(params))
        headers = {
            **self.HEADERS,
            **({"Content-Type": "application/json"} if data else {}),
        }
        request = urllib.request.Request(
            f"{url}?{query}", data=data, headers=headers, method=method
        )
        # Log the URL WITHOUT the query string so the API key never hits CloudWatch.
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                logger.info(
                    "%s %s -> %s (%d bytes)", method, url, response.status, len(body)
                )
                return HttpResponse(response.status, body)
        except urllib.error.HTTPError as err:
            logger.error(
                "HTTP %s from %s %s: %s", err.code, method, url, err.read()[:500]
            )
            raise
        except urllib.error.URLError as err:
            logger.error("Could not reach %s: %s", url, err.reason)
            raise


# Validation


class ApiResponseError(Exception):
    """Raised when the API returns an error payload."""


class ResponseValidator:

    def parse(self, body: bytes):
        try:
            payload = json.loads(body)
        except ValueError as err:
            raise ApiResponseError(
                f"Response is not valid JSON: {body[:200]!r}"
            ) from err
        if isinstance(payload, dict) and "error" in payload:
            raise ApiResponseError(
                f"API error: {payload.get('message') or payload['error']}"
            )
        return payload

    def validate_places(self, body: bytes) -> Dict:
        payload = self.parse(body)
        if not isinstance(payload, dict) or not isinstance(
            payload.get("features"), list
        ):
            raise ApiResponseError("Places response has no 'features' list")
        return payload


# Forward geocoding (city name -> decimal-degree coordinates)


class GeocodingError(Exception):
    """Raised when a city can't be geocoded after retrying."""


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


class CityGeocoder:

    RETRY_STATUSES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        client: GeoapifyClient,
        validator: ResponseValidator,
        url: str,
        lang: str,
        concurrency: int = 4,
        retries: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._client = client
        self._validator = validator
        self._url = url
        self._lang = lang
        self._concurrency = concurrency
        self._retries = retries
        self._sleep = sleep

    def geocode(self, names: List[str]) -> GeocodeResult:
        with ThreadPoolExecutor(
            max_workers=min(self._concurrency, len(names)) or 1
        ) as pool:
            results = list(pool.map(self._geocode_one, names))

        found, not_found, raw = [], [], []
        for name, (destination, payload) in zip(names, results):
            raw.append({"query": {"text": name}, "response": payload})
            if destination is None:
                not_found.append(name)
            else:
                found.append(destination)
        return GeocodeResult(found, not_found, json.dumps(raw).encode("utf-8"))

    def _geocode_one(self, name: str) -> Tuple[Optional[GeocodedDestination], object]:
        try:
            payload = self._validator.parse(self._fetch(name))
        except (GeocodingError, ApiResponseError, urllib.error.URLError) as err:
            logger.warning("Geocoding %r failed: %s", name, err)
            return None, {"error": str(err)}

        results = payload.get("results") if isinstance(payload, dict) else None
        try:
            best = results[0]
            return (
                GeocodedDestination(name, float(best["lat"]), float(best["lon"])),
                payload,
            )
        except (TypeError, IndexError, KeyError, ValueError):
            return None, payload

    def _fetch(self, name: str) -> bytes:
        params = {
            "text": name,
            "type": "city",
            "lang": self._lang,
            "limit": "1",
            "format": "json",
        }
        for attempt in range(self._retries + 1):
            try:
                return self._client.get(self._url, params).body
            except urllib.error.HTTPError as err:
                if err.code not in self.RETRY_STATUSES or attempt == self._retries:
                    raise GeocodingError(f"HTTP {err.code}") from err
                self._sleep(2**attempt)
        raise GeocodingError("unreachable")


# Search area (all destinations -> one inline MultiPolygon)


@dataclass(frozen=True)
class BoundingBox:
    west: float
    south: float
    east: float
    north: float

    def overlaps(self, other: "BoundingBox") -> bool:
        return not (
            self.east < other.west
            or other.east < self.west
            or self.north < other.south
            or other.north < self.south
        )

    def union(self, other: "BoundingBox") -> "BoundingBox":
        return BoundingBox(
            min(self.west, other.west),
            min(self.south, other.south),
            max(self.east, other.east),
            max(self.north, other.north),
        )

    def ring(self) -> List[List[float]]:
        """Closed, counter-clockwise GeoJSON ring of [lon, lat] positions."""
        return [
            [self.west, self.south],
            [self.east, self.south],
            [self.east, self.north],
            [self.west, self.north],
            [self.west, self.south],
        ]


class AreaPolygonBuilder:

    METERS_PER_DEGREE = 111_320

    def __init__(self, radius_meters: int):
        self._radius = radius_meters

    def build(self, destinations: List[GeocodedDestination]) -> Dict:
        boxes = self._merge([self._box(d) for d in destinations])
        return {
            "type": "MultiPolygon",
            "coordinates": [
                [[[round(v, 6) for v in pos] for pos in box.ring()]] for box in boxes
            ],
        }

    def _box(self, destination: GeocodedDestination) -> BoundingBox:
        d_lat = self._radius / self.METERS_PER_DEGREE
        # Degrees of longitude shrink toward the poles; floor cos() to avoid blow-up.
        d_lon = self._radius / (
            self.METERS_PER_DEGREE * max(math.cos(math.radians(destination.lat)), 0.01)
        )
        return BoundingBox(
            max(destination.lon - d_lon, -180.0),
            max(destination.lat - d_lat, -90.0),
            min(destination.lon + d_lon, 180.0),
            min(destination.lat + d_lat, 90.0),
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


# Places query


class PlacesQueryBuilder:
    """Builds the Places POST body for one page."""

    def __init__(self, config: ExtractConfig):
        self._config = config

    def build(self, area: Dict, offset: int) -> Dict:
        return {
            "categories": list(self._config.categories),
            "filter": {"type": "polygon", "geometry": area},
            "limit": self._config.places_limit,
            "offset": offset,
            "lang": self._config.lang,
        }


# S3 storage


@dataclass(frozen=True)
class ExtractRun:

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class RawObjectKeyBuilder:

    def __init__(self, prefix: str):
        self._prefix = prefix.rstrip("/")

    def _base(self, run: ExtractRun) -> str:
        return f"{self._prefix}/extract_date={run.started_at:%Y-%m-%d}"

    def geocode(self, run: ExtractRun) -> str:
        return f"{self._base(run)}/geocode_{run.started_at:%H%M%S}_{run.run_id}.json"

    def places_page(self, run: ExtractRun, destination: str, page: int) -> str:
        return (
            f"{self._base(run)}/destination={self._slug(destination)}/"
            f"places_{run.started_at:%H%M%S}_{run.run_id}_page{page:03d}.json"
        )

    @staticmethod
    def _slug(name: str) -> str:
        # "Detroit, MI" -> "detroit_mi", "San José, Costa Rica" -> "san_josé_costa_rica"
        slug = re.sub(r"[^\w]+", "_", name.lower()).strip("_")
        return slug or "unknown"


class S3RawWriter:

    def __init__(self, bucket: str, client=None):
        self._bucket = bucket
        self._client = client or boto3.client("s3")

    @property
    def bucket(self) -> str:
        return self._bucket

    def write(self, key: str, body: bytes) -> str:
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=body, ContentType="application/json"
        )
        return key


# Orchestration


class ExtractError(Exception):
    """Raised when no destination could be geocoded."""


class ExtractService:

    def __init__(
        self,
        geocoder: CityGeocoder,
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

    def run(
        self, names: List[str], overrides: Optional[Dict] = None
    ) -> Dict[str, object]:
        run = ExtractRun()
        geocoded = self._geocoder.geocode(names)
        if geocoded.not_found:
            logger.warning("Could not geocode: %s", geocoded.not_found)
        if not geocoded.destinations:
            raise ExtractError(
                f"None of the {len(names)} destinations could be geocoded: {names}"
            )

        geocode_key = self._writer.write(self._key_builder.geocode(run), geocoded.raw)

        page_keys, place_count = [], 0
        for destination in geocoded.destinations:
            keys, count = self._extract_destination(run, destination, overrides)
            page_keys.extend(keys)
            place_count += count

        logger.info(
            "Wrote %d places for %d destinations to s3://%s",
            place_count,
            len(geocoded.destinations),
            self._writer.bucket,
        )
        return {
            "status": "PARTIAL" if geocoded.not_found else "SUCCEEDED",
            "bucket": self._writer.bucket,
            "destinations": [
                {"destination": d.name, "lat": d.lat, "lon": d.lon}
                for d in geocoded.destinations
            ],
            "not_geocoded": geocoded.not_found,
            "geocode_key": geocode_key,
            "places_keys": page_keys,
            "place_count": place_count,
            "run_id": run.run_id,
            "extract_date": f"{run.started_at:%Y-%m-%d}",
        }

    def _extract_destination(
        self,
        run: ExtractRun,
        destination: GeocodedDestination,
        overrides: Optional[Dict],
    ) -> Tuple[List[str], int]:
        area = self._area_builder.build([destination])
        page_keys, place_count = [], 0
        for page in range(self._max_pages):
            body = {
                **self._query_builder.build(area, offset=place_count),
                **(overrides or {}),
            }
            payload = self._validator.validate_places(
                self._client.post_json(self._places_url, body).body
            )
            features = payload["features"]
            if not features:
                break
            self._tag_search_city(features, destination.name)
            page_keys.append(
                self._writer.write(
                    self._key_builder.places_page(run, destination.name, page),
                    json.dumps(payload).encode("utf-8"),
                )
            )
            place_count += len(features)
            if len(features) < body["limit"]:
                break
        else:
            logger.warning(
                "Stopped %r after MAX_PAGES=%d; more places may exist",
                destination.name,
                self._max_pages,
            )
        logger.info("Found %d places for %r", place_count, destination.name)
        return page_keys, place_count

    @staticmethod
    def _tag_search_city(features: List, search_city: str) -> None:
        """Stamp the destination name from DestinationLocator onto every place."""
        for feature in features:
            if isinstance(feature, dict) and isinstance(
                feature.get("properties"), dict
            ):
                feature["properties"]["search_city"] = search_city


def build_locator(config: ExtractConfig) -> DestinationLocator:
    return DestinationLocator(
        config.aws_hotel_room_rates_s3_bucket, config.hotel_prefix
    )


def build_service(config: ExtractConfig) -> ExtractService:
    client = GeoapifyClient(
        QueryParamKeyAuthenticator(SecretProvider(config.api_key_secret)),
        config.request_timeout,
    )
    validator = ResponseValidator()
    return ExtractService(
        geocoder=CityGeocoder(
            client,
            validator,
            config.geocode_url,
            config.lang,
            config.geocode_concurrency,
        ),
        area_builder=AreaPolygonBuilder(config.area_radius_meters),
        query_builder=PlacesQueryBuilder(config),
        client=client,
        places_url=config.places_url,
        validator=validator,
        key_builder=RawObjectKeyBuilder(config.raw_prefix),
        writer=S3RawWriter(config.aws_destination_activities_s3_bucket),
        max_pages=config.max_pages,
    )


_locator: Optional[DestinationLocator] = None
_service: Optional[ExtractService] = None


def lambda_handler(event, context):
    global _locator, _service
    if _service is None:
        config = ExtractConfig.from_env()
        _locator = build_locator(config)
        _service = build_service(config)

    overrides = event.get("query_params", {}) if isinstance(event, dict) else {}
    return _service.run(_locator.locate(event), overrides)

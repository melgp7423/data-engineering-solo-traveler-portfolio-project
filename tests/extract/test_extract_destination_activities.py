import json
import sys
import types
import urllib.error
from datetime import date

import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from extract.extract_destination_activities.handler import (
    AreaPolygonBuilder,
    CityGeocoder,
    DestinationLocator,
    DestinationsNotFoundError,
    GeocodedDestination,
    HttpResponse,
    IngestionError,
    IngestionService,
    PlacesQueryBuilder,
    RawObjectKeyBuilder,
    ResponseValidator,
    IngestionConfig,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
class FakeGeoapify:
    """Records calls and replays queued responses per (method, url).
    Geocoding GETs are answered per city from `cities` ({name: (lat, lon)});
    a city missing from it gets an empty result. A queued Exception is raised."""

    def __init__(self, responses=None, cities=None):
        self._responses = {k: list(v) for k, v in (responses or {}).items()}
        self._cities = cities or {}
        self.calls = []

    def get(self, url, params):
        self.calls.append(("GET", url, params, None))
        if ("GET", url) in self._responses:
            response = self._responses[("GET", url)].pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        if params["text"] in self._cities:
            lat, lon = self._cities[params["text"]]
            return ok({"results": [{"lat": lat, "lon": lon}], "query": {"text": params["text"]}})
        return ok({"results": [], "query": {"text": params["text"]}})

    def post_json(self, url, payload, params=None):
        self.calls.append(("POST", url, params, payload))
        return self._responses[("POST", url)].pop(0)


class FakeWriter:
    bucket = "raw-bucket"

    def __init__(self):
        self.objects = {}

    def write(self, key, body):
        self.objects[key] = body
        return key


class FakeS3:
    def __init__(self, objects):
        self._objects = objects

    def get_paginator(self, _name):
        objects = self._objects

        class Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in objects if k.startswith(Prefix)]}

        return Paginator()

    def get_object(self, Bucket, Key):
        body = self._objects[Key]

        class Body:
            def read(self):
                return body

        return {"Body": Body()}


def ok(payload, status=200):
    return HttpResponse(status, json.dumps(payload).encode())


def config(**overrides):
    values = dict(
        geocode_url="https://geo/search",
        places_url="https://geo/places",
        api_key_secret="secret",
        aws_hotel_room_rates_raw_data_s3_bucket="hotels",
        hotel_prefix="hotel_room_rates/",
        aws_destination_activities_raw_data_s3_bucket="raw-bucket",
        raw_prefix="destination_activities/",
    )
    values.update(overrides)
    return IngestionConfig(**values)


# ---------------------------------------------------------------------------
# DestinationLocator
# ---------------------------------------------------------------------------
def test_locator_uses_hotel_lambda_output_and_dedupes():
    event = {
        "written": [
            {"destination": "Cancun, Mexico", "key": "a"},
            {"destination": "cancun, mexico", "key": "b"},
            {"destination": "Paris, France", "key": "c"},
        ],
        "failed": [{"destination": "Nowhere", "error": "x"}],
    }
    locator = DestinationLocator("hotels", "hotel_room_rates/", client=FakeS3({}))
    assert locator.locate(event) == ["Cancun, Mexico", "Paris, France"]


def test_locator_falls_back_to_todays_hotel_files():
    s3 = FakeS3({
        "hotel_room_rates/ingest_date=2026-10-05/a.json": json.dumps(
            {"search_parameters": {"q": "Tokyo, Japan"}}
        ).encode(),
        "hotel_room_rates/ingest_date=2026-10-04/old.json": json.dumps(
            {"search_parameters": {"q": "Stale, Place"}}
        ).encode(),
    })
    locator = DestinationLocator("hotels", "hotel_room_rates/", client=s3, today=lambda: date(2026, 10, 5))
    assert locator.locate({}) == ["Tokyo, Japan"]


def test_locator_raises_when_nothing_found():
    locator = DestinationLocator("hotels", "hotel_room_rates/", client=FakeS3({}))
    with pytest.raises(DestinationsNotFoundError):
        locator.locate({"written": []})


# ---------------------------------------------------------------------------
# CityGeocoder
# ---------------------------------------------------------------------------
def make_geocoder(fake, sleeps=None):
    sleep = sleeps.append if sleeps is not None else (lambda _s: None)
    return CityGeocoder(fake, ResponseValidator(), "https://geo/search", "en", sleep=sleep)


def test_geocoder_sends_one_request_per_city_and_keeps_input_order():
    fake = FakeGeoapify(cities={"Cancun, Mexico": (21.16, -86.85), "Paris, France": (48.85, 2.35)})
    result = make_geocoder(fake).geocode(["Cancun, Mexico", "Atlantis", "Paris, France"])

    assert result.destinations == [
        GeocodedDestination("Cancun, Mexico", 21.16, -86.85),
        GeocodedDestination("Paris, France", 48.85, 2.35),
    ]
    assert result.not_found == ["Atlantis"]
    assert sorted(c[2]["text"] for c in fake.calls) == ["Atlantis", "Cancun, Mexico", "Paris, France"]
    assert all(c[2]["type"] == "city" and c[2]["limit"] == "1" for c in fake.calls)
    raw = json.loads(result.raw)
    assert [r["query"]["text"] for r in raw] == ["Cancun, Mexico", "Atlantis", "Paris, France"]


def rate_limited():
    return urllib.error.HTTPError("https://geo/search", 429, "Too Many Requests", {}, None)


def http_error(code):
    return urllib.error.HTTPError("https://geo/search", code, "error", {}, None)


def test_geocoder_retries_when_rate_limited():
    fake = FakeGeoapify({("GET", "https://geo/search"): [
        rate_limited(),
        http_error(503),
        ok({"results": [{"lat": 1.0, "lon": 2.0}]}),
    ]})
    sleeps = []
    result = make_geocoder(fake, sleeps).geocode(["X"])

    assert result.destinations == [GeocodedDestination("X", 1.0, 2.0)]
    assert len(fake.calls) == 3
    assert sleeps == [1, 2]  # exponential backoff


def test_geocoder_does_not_retry_client_errors():
    fake = FakeGeoapify({("GET", "https://geo/search"): [http_error(401)]})
    result = make_geocoder(fake).geocode(["X"])

    assert result.not_found == ["X"]
    assert len(fake.calls) == 1


def test_geocoder_treats_network_and_api_errors_as_not_found():
    fake = FakeGeoapify({("GET", "https://geo/search"): [
        urllib.error.URLError("timed out"),
        ok({"statusCode": 400, "error": "Bad Request", "message": "bad text"}),
    ]})
    geocoder = CityGeocoder(fake, ResponseValidator(), "https://geo/search", "en", concurrency=1)
    result = geocoder.geocode(["A", "B"])

    assert result.destinations == []
    assert result.not_found == ["A", "B"]
    assert all("error" in r["response"] for r in json.loads(result.raw))


def test_geocoder_reports_city_as_not_found_after_retries_run_out():
    fake = FakeGeoapify({("GET", "https://geo/search"): [rate_limited()] * 4})
    result = make_geocoder(fake).geocode(["X"])

    assert result.destinations == []
    assert result.not_found == ["X"]


# ---------------------------------------------------------------------------
# AreaPolygonBuilder
# ---------------------------------------------------------------------------
def test_area_is_one_multipolygon_with_a_closed_square_per_city():
    area = AreaPolygonBuilder(10000).build([
        GeocodedDestination("Cancun", 21.16, -86.85),
        GeocodedDestination("Paris", 48.85, 2.35),
    ])
    assert area["type"] == "MultiPolygon"
    assert len(area["coordinates"]) == 2
    for polygon in area["coordinates"]:
        ring = polygon[0]
        assert len(ring) == 5 and ring[0] == ring[-1]

    cancun = area["coordinates"][0][0]
    lons = [p[0] for p in cancun]
    lats = [p[1] for p in cancun]
    assert min(lons) < -86.85 < max(lons)
    assert min(lats) < 21.16 < max(lats)
    assert max(lats) - min(lats) == pytest.approx(20000 / 111320, abs=1e-5)


def test_overlapping_cities_are_merged_into_one_polygon():
    area = AreaPolygonBuilder(10000).build([
        GeocodedDestination("A", 40.0, -3.70),
        GeocodedDestination("B", 40.05, -3.65),
        GeocodedDestination("Far", 10.0, 10.0),
    ])
    assert len(area["coordinates"]) == 2


# ---------------------------------------------------------------------------
# IngestionService
# ---------------------------------------------------------------------------
def make_service(fake, writer, cfg):
    validator = ResponseValidator()
    return IngestionService(
        geocoder=make_geocoder(fake),
        area_builder=AreaPolygonBuilder(cfg.area_radius_meters),
        query_builder=PlacesQueryBuilder(cfg),
        client=fake,
        places_url=cfg.places_url,
        validator=validator,
        key_builder=RawObjectKeyBuilder(cfg.raw_prefix),
        writer=writer,
        max_pages=cfg.max_pages,
    )


def feature(i):
    return {"type": "Feature", "properties": {"name": f"p{i}"}, "geometry": None}


def test_service_sends_single_places_request_for_all_destinations():
    cfg = config()
    fake = FakeGeoapify(
        {("POST", "https://geo/places"): [ok({"type": "FeatureCollection", "features": [feature(1)]})]},
        cities={"Cancun, Mexico": (21.16, -86.85), "Paris, France": (48.85, 2.35)},
    )
    writer = FakeWriter()
    result = make_service(fake, writer, cfg).run(["Cancun, Mexico", "Paris, France"])

    places_calls = [c for c in fake.calls if c[1] == "https://geo/places"]
    assert len(places_calls) == 1
    body = places_calls[0][3]
    assert body["filter"]["type"] == "polygon"
    assert body["filter"]["geometry"]["type"] == "MultiPolygon"
    assert len(body["filter"]["geometry"]["coordinates"]) == 2
    assert body["offset"] == 0

    assert result["status"] == "SUCCEEDED"
    assert result["place_count"] == 1
    assert len(result["places_keys"]) == 1
    assert result["geocode_key"] in writer.objects


def test_service_pages_with_offset_when_a_page_is_full():
    cfg = config(places_limit=2)
    fake = FakeGeoapify(
        {("POST", "https://geo/places"): [
            ok({"features": [feature(1), feature(2)]}),
            ok({"features": [feature(3)]}),
        ]},
        cities={"X": (1.0, 1.0)},
    )
    result = make_service(fake, FakeWriter(), cfg).run(["X"])

    offsets = [c[3]["offset"] for c in fake.calls if c[1] == "https://geo/places"]
    assert offsets == [0, 2]
    assert result["place_count"] == 3


def test_service_is_partial_when_some_cities_do_not_geocode():
    fake = FakeGeoapify(
        {("POST", "https://geo/places"): [ok({"features": [feature(1)]})]},
        cities={"X": (1.0, 1.0)},
    )
    writer = FakeWriter()
    result = make_service(fake, writer, config()).run(["X", "Atlantis"])

    assert result["status"] == "PARTIAL"
    assert result["not_geocoded"] == ["Atlantis"]
    assert result["destinations"] == [{"destination": "X", "lat": 1.0, "lon": 1.0}]
    raw = json.loads(writer.objects[result["geocode_key"]])
    assert [r["query"]["text"] for r in raw] == ["X", "Atlantis"]


def test_service_fails_when_nothing_geocodes():
    fake = FakeGeoapify()
    with pytest.raises(IngestionError):
        make_service(fake, FakeWriter(), config()).run(["X"])


def test_config_rejects_placeholder_bucket():
    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET"):
        config(aws_destination_activities_raw_data_s3_bucket="<YOUR_RAW_BUCKET_NAME>").validate()


def test_config_rejects_empty_hotel_bucket():
    with pytest.raises(ValueError, match="AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET"):
        config(aws_hotel_room_rates_raw_data_s3_bucket="").validate()


def test_config_rejects_geocode_concurrency_out_of_range():
    for value in (0, 6):
        with pytest.raises(ValueError, match="GEOCODE_CONCURRENCY"):
            config(geocode_concurrency=value).validate()


def test_config_from_env_uses_synchronous_geocoding_defaults(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "raw-bucket")
    # The old batch URL variable must not be picked up any more.
    monkeypatch.setenv("GEOCODE_URL", "https://api.geoapify.com/v1/batch/geocode/search")
    for name in ("GEOCODE_SEARCH_URL", "GEOCODE_CONCURRENCY", "REQUEST_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)

    cfg = IngestionConfig.from_env()

    assert cfg.geocode_url == "https://api.geoapify.com/v1/geocode/search"
    assert cfg.geocode_concurrency == 4
    assert cfg.request_timeout == 30


def test_config_from_env_reads_geocode_overrides(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "raw-bucket")
    monkeypatch.setenv("GEOCODE_SEARCH_URL", "https://geo/custom")
    monkeypatch.setenv("GEOCODE_CONCURRENCY", "2")

    cfg = IngestionConfig.from_env()

    assert cfg.geocode_url == "https://geo/custom"
    assert cfg.geocode_concurrency == 2

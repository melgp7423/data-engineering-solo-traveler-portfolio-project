import json
import sys
import types
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
    BatchGeocoder,
    DestinationLocator,
    DestinationsNotFoundError,
    GeocodedDestination,
    GeocodingError,
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
    """Records calls and replays queued responses per (method, url)."""

    def __init__(self, responses):
        self._responses = {k: list(v) for k, v in responses.items()}
        self.calls = []

    def get(self, url, params):
        self.calls.append(("GET", url, params, None))
        return self._responses[("GET", url)].pop(0)

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
        geocode_url="https://geo/batch",
        places_url="https://geo/places",
        api_key_secret="secret",
        aws_hotel_room_prices_raw_data_s3_bucket="hotels",
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
# BatchGeocoder
# ---------------------------------------------------------------------------
def make_geocoder(fake, clock_values=None):
    clock = iter(clock_values or [0, 1, 2, 3, 4, 5])
    return BatchGeocoder(
        fake, ResponseValidator(), "https://geo/batch", "en",
        poll_seconds=1, max_wait=10, sleep=lambda _s: None, clock=lambda: next(clock),
    )


def test_geocoder_submits_one_batch_and_polls_until_done():
    fake = FakeGeoapify({
        ("POST", "https://geo/batch"): [ok({"id": "job1", "status": "pending"}, status=202)],
        ("GET", "https://geo/batch"): [
            ok({"id": "job1", "status": "pending"}, status=202),
            ok([
                {"query": {"text": "Cancun, Mexico"}, "lat": 21.16, "lon": -86.85},
                {"query": {"text": "Atlantis"}},
            ]),
        ],
    })
    result = make_geocoder(fake).geocode(["Cancun, Mexico", "Atlantis"])

    assert result.destinations == [GeocodedDestination("Cancun, Mexico", 21.16, -86.85)]
    assert result.not_found == ["Atlantis"]
    post = fake.calls[0]
    assert post[3] == ["Cancun, Mexico", "Atlantis"]
    assert post[2]["type"] == "city"
    assert fake.calls[1][2] == {"id": "job1", "format": "json"}


def test_geocoder_times_out():
    fake = FakeGeoapify({
        ("POST", "https://geo/batch"): [ok({"id": "job1"}, status=202)],
        ("GET", "https://geo/batch"): [ok({"id": "job1"}, status=202)] * 5,
    })
    with pytest.raises(GeocodingError):
        make_geocoder(fake, clock_values=[0, 5, 11]).geocode(["Cancun, Mexico"])


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
    fake = FakeGeoapify({
        ("POST", "https://geo/batch"): [ok([
            {"query": {"text": "Cancun, Mexico"}, "lat": 21.16, "lon": -86.85},
            {"query": {"text": "Paris, France"}, "lat": 48.85, "lon": 2.35},
        ])],
        ("POST", "https://geo/places"): [ok({"type": "FeatureCollection", "features": [feature(1)]})],
    })
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
    fake = FakeGeoapify({
        ("POST", "https://geo/batch"): [ok([{"query": {"text": "X"}, "lat": 1.0, "lon": 1.0}])],
        ("POST", "https://geo/places"): [
            ok({"features": [feature(1), feature(2)]}),
            ok({"features": [feature(3)]}),
        ],
    })
    result = make_service(fake, FakeWriter(), cfg).run(["X"])

    offsets = [c[3]["offset"] for c in fake.calls if c[1] == "https://geo/places"]
    assert offsets == [0, 2]
    assert result["place_count"] == 3


def test_service_fails_when_nothing_geocodes():
    fake = FakeGeoapify({("POST", "https://geo/batch"): [ok([{"query": {"text": "X"}}])]})
    with pytest.raises(IngestionError):
        make_service(fake, FakeWriter(), config()).run(["X"])


def test_config_rejects_placeholder_bucket():
    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET"):
        config(aws_destination_activities_raw_data_s3_bucket="<YOUR_RAW_BUCKET_NAME>").validate()


def test_config_rejects_empty_hotel_bucket():
    with pytest.raises(ValueError, match="AWS_HOTEL_ROOM_PRICES_RAW_DATA_S3_BUCKET"):
        config(aws_hotel_room_prices_raw_data_s3_bucket="").validate()

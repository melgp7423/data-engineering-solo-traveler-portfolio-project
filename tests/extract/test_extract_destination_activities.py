import json
import sys
import types
import io
import urllib.error
import urllib.parse
from datetime import date

import pytest

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from extract.extract_destination_activities import handler
from extract.extract_destination_activities.handler import (
    AreaPolygonBuilder,
    BoundingBox,
    CityGeocoder,
    GeoapifyClient,
    DestinationLocator,
    DestinationsNotFoundError,
    GeocodedDestination,
    HttpResponse,
    ExtractError,
    ExtractService,
    PlacesQueryBuilder,
    QueryParamKeyAuthenticator,
    RawObjectKeyBuilder,
    ResponseValidator,
    SecretProvider,
    ExtractConfig,
    S3RawWriter,
    build_locator,
    build_service,
)


class FakeGeoapify:

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
            return ok(
                {
                    "results": [{"lat": lat, "lon": lon}],
                    "query": {"text": params["text"]},
                }
            )
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
                yield {
                    "Contents": [{"Key": k} for k in objects if k.startswith(Prefix)]
                }

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
        aws_hotel_room_rates_s3_bucket="hotels",
        hotel_prefix="hotel_room_rates/",
        aws_destination_activities_s3_bucket="raw-bucket",
        raw_prefix="destination_activities/",
    )
    values.update(overrides)
    return ExtractConfig(**values)


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
    s3 = FakeS3(
        {
            "hotel_room_rates/extract_date=2026-10-05/a.json": json.dumps(
                {"search_parameters": {"q": "Tokyo, Japan"}}
            ).encode(),
            "hotel_room_rates/extract_date=2026-10-04/old.json": json.dumps(
                {"search_parameters": {"q": "Stale, Place"}}
            ).encode(),
        }
    )
    locator = DestinationLocator(
        "hotels", "hotel_room_rates/", client=s3, today=lambda: date(2026, 10, 5)
    )
    assert locator.locate({}) == ["Tokyo, Japan"]


def test_locator_raises_when_nothing_found():
    locator = DestinationLocator("hotels", "hotel_room_rates/", client=FakeS3({}))
    with pytest.raises(DestinationsNotFoundError):
        locator.locate({"written": []})


def make_geocoder(fake, sleeps=None):
    sleep = sleeps.append if sleeps is not None else (lambda _s: None)
    return CityGeocoder(
        fake, ResponseValidator(), "https://geo/search", "en", sleep=sleep
    )


def test_geocoder_sends_one_request_per_city_and_keeps_input_order():
    fake = FakeGeoapify(
        cities={"Cancun, Mexico": (21.16, -86.85), "Paris, France": (48.85, 2.35)}
    )
    result = make_geocoder(fake).geocode(
        ["Cancun, Mexico", "Atlantis", "Paris, France"]
    )

    assert result.destinations == [
        GeocodedDestination("Cancun, Mexico", 21.16, -86.85),
        GeocodedDestination("Paris, France", 48.85, 2.35),
    ]
    assert result.not_found == ["Atlantis"]
    assert sorted(c[2]["text"] for c in fake.calls) == [
        "Atlantis",
        "Cancun, Mexico",
        "Paris, France",
    ]
    assert all(c[2]["type"] == "city" and c[2]["limit"] == "1" for c in fake.calls)
    raw = json.loads(result.raw)
    assert [r["query"]["text"] for r in raw] == [
        "Cancun, Mexico",
        "Atlantis",
        "Paris, France",
    ]


def rate_limited():
    return urllib.error.HTTPError(
        "https://geo/search", 429, "Too Many Requests", {}, None
    )


def http_error(code):
    return urllib.error.HTTPError("https://geo/search", code, "error", {}, None)


def test_geocoder_retries_when_rate_limited():
    fake = FakeGeoapify(
        {
            ("GET", "https://geo/search"): [
                rate_limited(),
                http_error(503),
                ok({"results": [{"lat": 1.0, "lon": 2.0}]}),
            ]
        }
    )
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
    fake = FakeGeoapify(
        {
            ("GET", "https://geo/search"): [
                urllib.error.URLError("timed out"),
                ok({"statusCode": 400, "error": "Bad Request", "message": "bad text"}),
            ]
        }
    )
    geocoder = CityGeocoder(
        fake, ResponseValidator(), "https://geo/search", "en", concurrency=1
    )
    result = geocoder.geocode(["A", "B"])

    assert result.destinations == []
    assert result.not_found == ["A", "B"]
    assert all("error" in r["response"] for r in json.loads(result.raw))


def test_geocoder_reports_city_as_not_found_after_retries_run_out():
    fake = FakeGeoapify({("GET", "https://geo/search"): [rate_limited()] * 4})
    result = make_geocoder(fake).geocode(["X"])

    assert result.destinations == []
    assert result.not_found == ["X"]


def test_area_is_one_multipolygon_with_a_closed_square_per_city():
    area = AreaPolygonBuilder(10000).build(
        [
            GeocodedDestination("Cancun", 21.16, -86.85),
            GeocodedDestination("Paris", 48.85, 2.35),
        ]
    )
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
    area = AreaPolygonBuilder(10000).build(
        [
            GeocodedDestination("A", 40.0, -3.70),
            GeocodedDestination("B", 40.05, -3.65),
            GeocodedDestination("Far", 10.0, 10.0),
        ]
    )
    assert len(area["coordinates"]) == 2


def make_service(fake, writer, cfg):
    validator = ResponseValidator()
    return ExtractService(
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


def test_service_sends_one_places_search_per_destination():
    cfg = config()
    fake = FakeGeoapify(
        {
            ("POST", "https://geo/places"): [
                ok({"type": "FeatureCollection", "features": [feature(1)]}),
                ok({"type": "FeatureCollection", "features": [feature(2)]}),
            ]
        },
        cities={"Cancun, Mexico": (21.16, -86.85), "Paris, France": (48.85, 2.35)},
    )
    writer = FakeWriter()
    result = make_service(fake, writer, cfg).run(["Cancun, Mexico", "Paris, France"])

    places_calls = [c for c in fake.calls if c[1] == "https://geo/places"]
    assert len(places_calls) == 2
    for call in places_calls:
        body = call[3]
        assert body["filter"]["type"] == "polygon"
        assert body["filter"]["geometry"]["type"] == "MultiPolygon"
        assert len(body["filter"]["geometry"]["coordinates"]) == 1
        assert body["offset"] == 0

    assert result["status"] == "SUCCEEDED"
    assert result["place_count"] == 2
    assert len(result["places_keys"]) == 2
    assert result["geocode_key"] in writer.objects


def test_service_tags_every_place_with_its_search_city_and_partitions_by_it():
    fake = FakeGeoapify(
        {
            ("POST", "https://geo/places"): [
                ok({"type": "FeatureCollection", "features": [feature(1), feature(2)]}),
                ok({"type": "FeatureCollection", "features": [feature(3)]}),
            ]
        },
        cities={"Detroit, MI": (42.33, -83.05), "San José, Costa Rica": (9.93, -84.08)},
    )
    writer = FakeWriter()
    result = make_service(fake, writer, config()).run(
        ["Detroit, MI", "San José, Costa Rica"]
    )

    detroit_key, san_jose_key = result["places_keys"]
    assert "/destination=detroit_mi/places_" in detroit_key
    assert "/destination=san_josé_costa_rica/places_" in san_jose_key
    detroit = json.loads(writer.objects[detroit_key])
    san_jose = json.loads(writer.objects[san_jose_key])
    assert detroit["type"] == "FeatureCollection"
    assert [f["properties"]["search_city"] for f in detroit["features"]] == [
        "Detroit, MI",
        "Detroit, MI",
    ]
    assert [f["properties"]["search_city"] for f in san_jose["features"]] == [
        "San José, Costa Rica"
    ]
    # The API's own fields are left as they were.
    assert detroit["features"][0]["properties"]["name"] == "p1"


def test_service_pages_with_offset_when_a_page_is_full():
    cfg = config(places_limit=2)
    fake = FakeGeoapify(
        {
            ("POST", "https://geo/places"): [
                ok({"features": [feature(1), feature(2)]}),
                ok({"features": [feature(3)]}),
            ]
        },
        cities={"X": (1.0, 1.0)},
    )
    result = make_service(fake, FakeWriter(), cfg).run(["X"])

    offsets = [c[3]["offset"] for c in fake.calls if c[1] == "https://geo/places"]
    assert offsets == [0, 2]
    assert result["place_count"] == 3
    assert [k.rsplit("_", 1)[-1] for k in result["places_keys"]] == [
        "page000.json",
        "page001.json",
    ]


def test_service_pages_each_destination_from_offset_zero():
    cfg = config(places_limit=1)
    fake = FakeGeoapify(
        {
            ("POST", "https://geo/places"): [
                ok({"features": [feature(1)]}),
                ok({"features": []}),
                ok({"features": []}),
            ]
        },
        cities={"X": (1.0, 1.0), "Y": (50.0, 50.0)},
    )
    result = make_service(fake, FakeWriter(), cfg).run(["X", "Y"])

    offsets = [c[3]["offset"] for c in fake.calls if c[1] == "https://geo/places"]
    assert offsets == [0, 1, 0]
    assert result["place_count"] == 1


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
    with pytest.raises(ExtractError):
        make_service(fake, FakeWriter(), config()).run(["X"])


def test_config_rejects_placeholder_bucket():
    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_S3_BUCKET"):
        config(aws_destination_activities_s3_bucket="<YOUR_RAW_BUCKET_NAME>").validate()


def test_config_rejects_empty_hotel_bucket():
    with pytest.raises(ValueError, match="AWS_HOTEL_ROOM_RATES_S3_BUCKET"):
        config(aws_hotel_room_rates_s3_bucket="").validate()


def test_config_rejects_geocode_concurrency_out_of_range():
    for value in (0, 6):
        with pytest.raises(ValueError, match="GEOCODE_CONCURRENCY"):
            config(geocode_concurrency=value).validate()


def test_config_from_env_uses_synchronous_geocoding_defaults(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "raw-bucket")
    monkeypatch.setenv(
        "GEOCODE_URL", "https://api.geoapify.com/v1/batch/geocode/search"
    )
    for name in ("GEOCODE_SEARCH_URL", "GEOCODE_CONCURRENCY", "REQUEST_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.geocode_url == "https://api.geoapify.com/v1/geocode/search"
    assert cfg.geocode_concurrency == 4
    assert cfg.request_timeout == 30


def test_config_from_env_reads_geocode_overrides(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "raw-bucket")
    monkeypatch.setenv("GEOCODE_SEARCH_URL", "https://geo/custom")
    monkeypatch.setenv("GEOCODE_CONCURRENCY", "2")

    cfg = ExtractConfig.from_env()

    assert cfg.geocode_url == "https://geo/custom"
    assert cfg.geocode_concurrency == 2


def test_config_from_env_reads_renamed_bucket_variables(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", " travel-bucket ")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", " travel-bucket ")

    cfg = ExtractConfig.from_env()

    assert cfg.aws_hotel_room_rates_s3_bucket == "travel-bucket"
    assert cfg.aws_destination_activities_s3_bucket == "travel-bucket"


def test_config_from_env_ignores_old_raw_data_bucket_variables(monkeypatch):
    monkeypatch.delenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", raising=False)
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "raw-bucket")

    with pytest.raises(ValueError) as err:
        ExtractConfig.from_env()

    assert "AWS_HOTEL_ROOM_RATES_S3_BUCKET" in str(err.value)
    assert "AWS_DESTINATION_ACTIVITIES_S3_BUCKET" in str(err.value)


def test_config_from_env_reads_raw_prefix(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "travel-bucket")
    monkeypatch.setenv("RAW_PREFIX", " raw/destination_activities/ ")

    cfg = ExtractConfig.from_env()

    assert cfg.raw_prefix == "raw/destination_activities/"


def test_config_from_env_raw_prefix_defaults_to_empty(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", "travel-bucket")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "travel-bucket")
    monkeypatch.delenv("RAW_PREFIX", raising=False)

    cfg = ExtractConfig.from_env()

    assert cfg.raw_prefix == ""


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


def split_url(request):
    url = urllib.parse.urlsplit(request.full_url)
    return f"{url.scheme}://{url.netloc}{url.path}", urllib.parse.parse_qs(url.query)


class FakeSecrets:
    def __init__(self, secret_string):
        self._secret_string = secret_string
        self.calls = 0

    def get_secret_value(self, SecretId):
        self.calls += 1
        return {"SecretString": self._secret_string}


def geoapify_client(timeout=5):
    secrets = SecretProvider("id", client=FakeSecrets("abc123"))
    return GeoapifyClient(QueryParamKeyAuthenticator(secrets), timeout)


@pytest.mark.parametrize(
    "secret_string",
    [
        "abc123",
        " abc123 ",
        '{"GEOAPIFY_KEY": " abc123 "}',
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


def test_authenticator_adds_geoapify_api_key_without_mutating_input():
    secrets = SecretProvider("id", client=FakeSecrets("abc123"))
    params = {"text": "Cancun"}
    assert QueryParamKeyAuthenticator(secrets).apply(params) == {
        "text": "Cancun",
        "apiKey": "abc123",
    }
    assert params == {"text": "Cancun"}


def test_client_get_sends_params_and_api_key(monkeypatch):
    urlopen = FakeUrlopen(FakeResponse(b'{"results": []}'))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    response = geoapify_client(timeout=7).get("https://geo/search", {"text": "Cancun"})

    request = urlopen.requests[0]
    assert response == HttpResponse(200, b'{"results": []}')
    assert split_url(request) == (
        "https://geo/search",
        {"text": ["Cancun"], "apiKey": ["abc123"]},
    )
    assert request.get_method() == "GET"
    assert request.data is None
    assert request.get_header("Content-type") is None
    assert urlopen.timeouts == [7]


def test_client_post_json_sends_json_body(monkeypatch):
    urlopen = FakeUrlopen(FakeResponse(b"{}", status=201))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    payload = {"categories": ["beach"], "filter": {"type": "Polygon"}}
    response = geoapify_client().post_json("https://geo/places", payload)

    request = urlopen.requests[0]
    assert response.status == 201
    assert split_url(request) == ("https://geo/places", {"apiKey": ["abc123"]})
    assert request.get_method() == "POST"
    assert json.loads(request.data) == payload
    assert request.get_header("Content-type") == "application/json"


def test_client_reraises_http_errors(monkeypatch):
    error = urllib.error.HTTPError(
        "https://geo/search", 401, "Unauthorized", {}, io.BytesIO(b"bad key")
    )
    monkeypatch.setattr(handler.urllib.request, "urlopen", FakeUrlopen(error))
    with pytest.raises(urllib.error.HTTPError):
        geoapify_client().get("https://geo/search", {})


def test_client_reraises_connection_errors(monkeypatch):
    monkeypatch.setattr(
        handler.urllib.request, "urlopen", FakeUrlopen(urllib.error.URLError("down"))
    )
    with pytest.raises(urllib.error.URLError):
        geoapify_client().post_json("https://geo/places", {})


@pytest.mark.parametrize(
    "other, overlaps",
    [
        (BoundingBox(1, 1, 3, 3), True),
        # Touching edges count as overlapping.
        (BoundingBox(2, 0, 4, 2), True),
        (BoundingBox(-1, -1, 3, 3), True),
        (BoundingBox(2.1, 0, 4, 2), False),
        (BoundingBox(0, 2.1, 2, 4), False),
        (BoundingBox(-3, -3, -0.1, -0.1), False),
    ],
)
def test_bounding_box_overlaps(other, overlaps):
    box = BoundingBox(0, 0, 2, 2)
    assert box.overlaps(other) is overlaps
    assert other.overlaps(box) is overlaps


def test_bounding_box_union_covers_both_boxes():
    assert BoundingBox(0, 0, 2, 2).union(BoundingBox(1, -1, 3, 1)) == BoundingBox(
        0, -1, 3, 2
    )


def test_bounding_box_ring_is_closed_counter_clockwise():
    assert BoundingBox(0, 0, 2, 1).ring() == [[0, 0], [2, 0], [2, 1], [0, 1], [0, 0]]


def test_s3_raw_writer_puts_json_object_and_returns_key():
    puts = []
    s3 = types.SimpleNamespace(put_object=lambda **kwargs: puts.append(kwargs))
    writer = S3RawWriter("raw-bucket", client=s3)

    assert writer.write("k.json", b"{}") == "k.json"
    assert writer.bucket == "raw-bucket"
    assert puts == [
        {
            "Bucket": "raw-bucket",
            "Key": "k.json",
            "Body": b"{}",
            "ContentType": "application/json",
        }
    ]


def test_build_locator_reads_from_hotel_room_rates_bucket(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    locator = build_locator(config(aws_hotel_room_rates_s3_bucket="hotels-bucket"))
    assert locator._bucket == "hotels-bucket"
    assert locator._prefix == "hotel_room_rates"


def test_build_service_writes_to_destination_activities_bucket(monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda *_a, **_k: None)
    service = build_service(config(aws_destination_activities_s3_bucket="activities"))
    assert service._writer.bucket == "activities"


def test_lambda_handler_builds_once_and_passes_event(monkeypatch):
    monkeypatch.setenv("AWS_HOTEL_ROOM_RATES_S3_BUCKET", "hotels")
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "raw-bucket")
    built = []

    class StubLocator:
        def locate(self, event):
            return ["located", event["written"][0]["destination"]]

    class StubService:
        def run(self, destinations, overrides):
            return {"destinations": destinations, "overrides": overrides}

    def fake_build_service(cfg):
        built.append(cfg)
        return StubService()

    monkeypatch.setattr(handler, "_service", None)
    monkeypatch.setattr(handler, "_locator", None)
    monkeypatch.setattr(handler, "build_locator", lambda cfg: StubLocator())
    monkeypatch.setattr(handler, "build_service", fake_build_service)

    event = {
        "written": [{"destination": "Cancun, Mexico"}],
        "query_params": {"categories": ["beach"]},
    }
    first = handler.lambda_handler(event, None)
    handler.lambda_handler(event, None)

    assert first == {
        "destinations": ["located", "Cancun, Mexico"],
        "overrides": {"categories": ["beach"]},
    }
    assert len(built) == 1
    assert built[0].aws_destination_activities_s3_bucket == "raw-bucket"


def test_lambda_handler_defaults_overrides_to_empty(monkeypatch):
    class StubLocator:
        def locate(self, event):
            return []

    class StubService:
        def run(self, destinations, overrides):
            return overrides

    monkeypatch.setattr(handler, "_locator", StubLocator())
    monkeypatch.setattr(handler, "_service", StubService())
    assert handler.lambda_handler(None, None) == {}

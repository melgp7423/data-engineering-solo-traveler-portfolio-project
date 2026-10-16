import io
import json
import sys
import types
from datetime import date

import pyarrow.parquet as pq
import pytest

# boto3 ships with the Lambda runtime but isn't in requirements.txt, so CI may
# not have it. Every test injects fakes, so a stub module is enough.
try:
    import boto3  # noqa: F401
except ImportError:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_a, **_k: None)

from transformation.transform_destination_activities.handler import (
    ActivitiesTransformer,
    DestinationGrouper,
    InvalidRawPayloadError,
    RawObjectLocator,
    S3ParquetWriter,
    S3RawReader,
    TransformConfig,
    TransformedKeyBuilder,
    TransformError,
    TransformService,
)

PAGE_0 = "extract_date=2026-10-09/places_021651_fde87987_page000.json"
PAGE_1 = "extract_date=2026-10-09/places_021651_fde87987_page001.json"
GEOCODE = "extract_date=2026-10-09/geocode_021651_fde87987.json"


def make_feature(**overrides):
    properties = {
        "name": "DTE",
        "country": "United States",
        "country_code": "us",
        "state": "Michigan",
        "city": "Detroit",
        "postcode": "48226",
        "street": "3rd Avenue",
        "lon": -83.0576393,
        "lat": 42.3325668,
        "formatted": "DTE, 3rd Avenue, Detroit, MI 48226, United States of America",
        "categories": [
            "tourism",
            "tourism.attraction",
            "tourism.attraction.artwork",
            "tourism.attraction.artwork.sculpture",
        ],
        "details": ["details.artwork"],
        "datasource": {"sourcename": "openstreetmap", "raw": {"osm_id": 12094489902}},
        "place_id": "51b81dbf5cb0c354c05921de848c912a4540f00103f9012e45e3d002000000920303445445",
    }
    properties.update(overrides)
    properties = {k: v for k, v in properties.items() if v is not ...}
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "Point", "coordinates": [-83.0576393, 42.3325668]},
    }


def make_payload(features):
    return {"type": "FeatureCollection", "features": features}


# Fakes
class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class FakeS3:
    def __init__(self, objects=None, pages=None):
        self.objects = dict(objects or {})
        self.puts = []
        self.paginator = FakePaginator(pages or [])

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


def build_service(s3, today=date(2026, 10, 9), transformed_prefix=""):
    return TransformService(
        locator=RawObjectLocator("raw-bucket", "", client=s3, today=lambda: today),
        reader=S3RawReader(client=s3),
        transformer=ActivitiesTransformer(),
        grouper=DestinationGrouper(),
        key_builder=TransformedKeyBuilder("", transformed_prefix),
        writer=S3ParquetWriter("transformed-bucket", client=s3),
    )


def read_parquet(put):
    return pq.read_table(io.BytesIO(put["Body"]))


def raw(payload):
    return json.dumps(payload).encode()


# ActivitiesTransformer
def test_keeps_only_requested_fields_and_every_category():
    rows = ActivitiesTransformer().transform(make_payload([make_feature()]))

    assert rows == [
        {
            "search_city": None,
            "name": "DTE",
            "country_code": "us",
            "state": "Michigan",
            "city": "Detroit",
            "formatted": "DTE, 3rd Avenue, Detroit, MI 48226, United States of America",
            "longitude": -83.0576393,
            "latitude": 42.3325668,
            "categories": [
                "tourism",
                "tourism.attraction",
                "tourism.attraction.artwork",
                "tourism.attraction.artwork.sculpture",
            ],
        }
    ]


@pytest.mark.parametrize(
    "field",
    ["name", "country_code", "state", "city", "formatted", "categories"],
)
def test_drops_feature_missing_a_required_field(field):
    rows = ActivitiesTransformer().transform(
        make_payload([make_feature(**{field: ...}), make_feature()])
    )

    assert len(rows) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", ""),
        ("name", "   "),
        ("name", None),
        ("city", ""),
        ("state", None),
        ("country_code", None),
        ("formatted", ""),
        ("categories", []),
        ("categories", None),
        ("categories", "tourism"),
    ],
)
def test_drops_feature_with_empty_required_field(field, value):
    rows = ActivitiesTransformer().transform(
        make_payload([make_feature(**{field: value})])
    )

    assert rows == []


def test_keeps_search_city_added_by_extract():
    rows = ActivitiesTransformer().transform(
        make_payload([make_feature(search_city=" Detroit, MI ")])
    )

    assert rows[0]["search_city"] == "Detroit, MI"


def test_reads_coordinates_from_properties_not_geometry():
    feature = make_feature(lon=-80.1973615, lat=25.7752802)
    feature["geometry"]["coordinates"] = [0.0, 0.0]

    rows = ActivitiesTransformer().transform(make_payload([feature]))

    assert (rows[0]["longitude"], rows[0]["latitude"]) == (-80.1973615, 25.7752802)


@pytest.mark.parametrize("lon,lat", [(..., ...), (None, None), ("-83.05", True)])
def test_keeps_feature_with_missing_coordinates_as_null(lon, lat):
    rows = ActivitiesTransformer().transform(
        make_payload([make_feature(lon=lon, lat=lat)])
    )

    assert (rows[0]["longitude"], rows[0]["latitude"]) == (None, None)


@pytest.mark.parametrize(
    "raw_name,expected",
    [
        ('"Flora" The Gatherer of Dreams', "Flora The Gatherer of Dreams"),
        ("'Flora' The Gatherer of Dreams", "Flora The Gatherer of Dreams"),
        ("\u201cFlora\u201d The Gatherer", "Flora The Gatherer"),
        ("Flora\\ The Gatherer/", "Flora The Gatherer"),
        ("Art/Design Center", "Art Design Center"),
        ("Joe's Stone Crab", "Joe's Stone Crab"),
        ("DTE", "DTE"),
    ],
)
def test_cleans_quotes_and_slashes_from_name(raw_name, expected):
    rows = ActivitiesTransformer().transform(
        make_payload([make_feature(name=raw_name)])
    )

    assert rows[0]["name"] == expected


def test_drops_feature_whose_name_is_only_quotes_and_slashes():
    rows = ActivitiesTransformer().transform(make_payload([make_feature(name='" / "')]))

    assert rows == []


@pytest.mark.parametrize(
    "raw_name",
    [
        "Untitled",
        "untitled",
        "UNTITLED",
        "Untitled #3",
        "Untitled_7",
        "(Untitled)",
        '"Untitled"',
        "Sculpture (untitled)",
        "Mural: Untitled 2019",
        "Un-titled",
        "Un titled",
        "UnTitled",
        "\uff35\uff4e\uff54\uff49\uff54\uff4c\uff45\uff44",  # full-width "Untitled"
    ],
)
def test_drops_feature_with_untitled_name(raw_name):
    rows = ActivitiesTransformer().transform(make_payload([make_feature(name=raw_name)]))

    assert rows == []


@pytest.mark.parametrize("raw_name", ["Titled Gallery", "Fun Titled Show", "Untold Stories"])
def test_keeps_names_that_only_resemble_untitled(raw_name):
    rows = ActivitiesTransformer().transform(make_payload([make_feature(name=raw_name)]))

    assert [r["name"] for r in rows] == [raw_name]


def test_skips_features_without_properties():
    rows = ActivitiesTransformer().transform(
        make_payload([{"type": "Feature"}, "junk", make_feature()])
    )

    assert len(rows) == 1


def test_rejects_payload_without_features():
    with pytest.raises(InvalidRawPayloadError):
        ActivitiesTransformer().transform({"type": "FeatureCollection"})


# DestinationGrouper
def test_groups_every_city_in_a_destination_into_one_table_and_drops_duplicates():
    sf = "extract_date=2026-10-16/destination=san_francisco_united_states"
    detroit = "extract_date=2026-10-16/destination=detroit_mi"
    transformer = ActivitiesTransformer()
    sf_rows = transformer.transform(
        make_payload(
            [
                make_feature(
                    name="SFMOMA", city="San Francisco", formatted="SFMOMA, SF"
                ),
                make_feature(
                    name="SFMOMA", city="San Francisco", formatted="SFMOMA, SF"
                ),  # same place again
                # A nearby city found by the same search stays in the same file.
                make_feature(
                    name="Swede's Beach",
                    city="Sausalito",
                    formatted="Swede's Beach, Sausalito",
                ),
            ]
        )
    )
    detroit_rows = transformer.transform(make_payload([make_feature(name="DTE")]))

    tables = DestinationGrouper().group({sf: sf_rows, detroit: detroit_rows})

    assert sorted(tables) == [detroit, sf]
    assert tables[sf].schema == ActivitiesTransformer.SCHEMA
    assert "county" not in tables[sf].column_names
    assert tables[sf].column("name").to_pylist() == ["SFMOMA", "Swede's Beach"]
    assert tables[sf].column("city").to_pylist() == ["San Francisco", "Sausalito"]
    assert tables[detroit].column("name").to_pylist() == ["DTE"]


# TransformedKeyBuilder
def test_key_builder_names_file_after_destination_folder():
    builder = TransformedKeyBuilder("destination_activities/", "activities/")
    partition = builder.partition(
        "destination_activities/extract_date=2026-10-16/"
        "destination=san_francisco_united_states/places_1_a_page000.json"
    )

    assert partition == "extract_date=2026-10-16/destination=san_francisco_united_states"
    assert builder.destination(partition) == "san_francisco_united_states"
    assert builder.build(partition) == (
        "activities/extract_date=2026-10-16/destination=san_francisco_united_states/"
        "activities_san_francisco_united_states.parquet"
    )


def test_key_builder_uses_plain_name_without_destination_folder():
    builder = TransformedKeyBuilder("destination_activities/", "activities/")
    partition = builder.partition(f"destination_activities/{PAGE_0}")

    assert partition == "extract_date=2026-10-09"
    assert builder.destination(partition) is None
    assert (
        builder.build(partition)
        == "activities/extract_date=2026-10-09/activities.parquet"
    )


# RawObjectLocator
def test_locator_uses_places_keys_from_extract_output_and_skips_geocode():
    locator = RawObjectLocator("raw-bucket")
    event = {
        "Payload": {
            "bucket": "b",
            "geocode_key": GEOCODE,
            "places_keys": [PAGE_0, PAGE_1],
        }
    }

    assert locator.locate(event) == ("b", [PAGE_0, PAGE_1])


def test_locator_lists_only_places_pages_in_partition():
    s3 = FakeS3(
        pages=[{"Contents": [{"Key": GEOCODE}, {"Key": PAGE_1}, {"Key": PAGE_0}]}]
    )
    locator = RawObjectLocator(
        "raw-bucket", "", client=s3, today=lambda: date(2026, 10, 9)
    )

    assert locator.locate({}) == ("raw-bucket", [PAGE_0, PAGE_1])
    assert s3.paginator.calls == [
        {"Bucket": "raw-bucket", "Prefix": "extract_date=2026-10-09/"}
    ]


def test_locator_decodes_s3_notification_keys():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "b"},
                    "object": {"key": PAGE_0.replace("=", "%3D")},
                }
            }
        ]
    }

    assert RawObjectLocator("raw-bucket").locate(event) == ("b", [PAGE_0])


def test_locator_errors_when_partition_has_no_places_pages():
    s3 = FakeS3(pages=[{"Contents": [{"Key": GEOCODE}]}])
    locator = RawObjectLocator(
        "raw-bucket", "", client=s3, today=lambda: date(2026, 10, 9)
    )

    with pytest.raises(ValueError):
        locator.locate({})


# TransformService
def test_service_combines_pages_into_one_parquet_per_destination():
    s3 = FakeS3(
        objects={
            ("raw-bucket", PAGE_0): raw(
                make_payload(
                    [
                        make_feature(name="DTE"),
                        make_feature(name=...),  # no name -> dropped
                        make_feature(
                            name="The Alamo",
                            state="Texas",
                            city="San Antonio",
                            formatted="The Alamo, San Antonio, TX",
                        ),
                    ]
                )
            ),
            ("raw-bucket", PAGE_1): raw(
                make_payload(
                    [
                        make_feature(
                            name="Spirit of Detroit",
                            formatted="Spirit of Detroit, Detroit, MI",
                        ),
                    ]
                )
            ),
        }
    )

    result = build_service(s3, transformed_prefix="activities").run(
        {"places_keys": [PAGE_0, PAGE_1]}
    )

    assert result["status"] == "SUCCEEDED"
    assert result["file_count"] == 1
    assert result["row_count"] == 3
    puts = {p["Key"]: p for p in s3.puts}
    assert set(puts) == {"activities/extract_date=2026-10-09/activities.parquet"}
    wayne = read_parquet(puts["activities/extract_date=2026-10-09/activities.parquet"])
    assert wayne.column_names == [
        "search_city",
        "name",
        "country_code",
        "state",
        "city",
        "formatted",
        "longitude",
        "latitude",
        "categories",
    ]
    assert wayne.column("name").to_pylist() == ["DTE", "The Alamo", "Spirit of Detroit"]
    assert wayne.column("city").to_pylist() == ["Detroit", "San Antonio", "Detroit"]
    assert [w["destination"] for w in result["written"]] == [None]
    assert all(p["Bucket"] == "transformed-bucket" for p in s3.puts)


def test_service_reports_partial_when_one_page_is_bad():
    s3 = FakeS3(
        objects={
            ("raw-bucket", PAGE_0): raw(make_payload([make_feature()])),
            ("raw-bucket", PAGE_1): b"not json",
        }
    )

    result = build_service(s3).run({"places_keys": [PAGE_0, PAGE_1]})

    assert result["status"] == "PARTIAL"
    assert [f["source_key"] for f in result["failed"]] == [PAGE_1]
    assert result["file_count"] == 1


def test_service_raises_when_every_page_fails():
    s3 = FakeS3(objects={("raw-bucket", PAGE_0): raw({"type": "FeatureCollection"})})

    with pytest.raises(TransformError):
        build_service(s3).run({"places_keys": [PAGE_0]})


def test_service_raises_when_no_feature_is_complete():
    s3 = FakeS3(
        objects={("raw-bucket", PAGE_0): raw(make_payload([make_feature(name=...)]))}
    )

    with pytest.raises(TransformError):
        build_service(s3).run({"places_keys": [PAGE_0]})
    assert s3.puts == []


def test_service_reads_and_writes_same_bucket_separated_by_prefix():
    raw_key = "raw/destination_activities/" + PAGE_0
    s3 = FakeS3(
        objects={("travel-bucket", raw_key): raw(make_payload([make_feature()]))}
    )
    service = TransformService(
        locator=RawObjectLocator(
            "travel-bucket", "raw/destination_activities/", client=s3
        ),
        reader=S3RawReader(client=s3),
        transformer=ActivitiesTransformer(),
        grouper=DestinationGrouper(),
        key_builder=TransformedKeyBuilder(
            "raw/destination_activities/", "transformed/destination_activities/"
        ),
        writer=S3ParquetWriter("travel-bucket", client=s3),
    )

    result = service.run({"places_keys": [raw_key]})

    assert result["source_bucket"] == result["bucket"] == "travel-bucket"
    assert [(p["Bucket"], p["Key"]) for p in s3.puts] == [
        (
            "travel-bucket",
            "transformed/destination_activities/extract_date=2026-10-09/"
            "activities.parquet",
        )
    ]


def test_service_writes_search_city_column_in_per_destination_folders():
    detroit = "extract_date=2026-10-13/destination=detroit_mi/places_1_a_page000.json"
    ann_arbor = (
        "extract_date=2026-10-13/destination=ann_arbor_mi/places_1_a_page000.json"
    )
    s3 = FakeS3(
        objects={
            ("raw-bucket", detroit): raw(
                make_payload([make_feature(search_city="Detroit, MI")])
            ),
            # Same place found from a second search city is kept for both.
            ("raw-bucket", ann_arbor): raw(
                make_payload([make_feature(search_city="Ann Arbor, MI")])
            ),
        }
    )

    result = build_service(s3).run({"places_keys": [detroit, ann_arbor]})

    puts = {p["Key"]: read_parquet(p) for p in s3.puts}
    assert sorted(puts) == [
        "extract_date=2026-10-13/destination=ann_arbor_mi/activities_ann_arbor_mi.parquet",
        "extract_date=2026-10-13/destination=detroit_mi/activities_detroit_mi.parquet",
    ]
    assert puts[
        "extract_date=2026-10-13/destination=detroit_mi/activities_detroit_mi.parquet"
    ].column("search_city").to_pylist() == ["Detroit, MI"]
    assert result["row_count"] == 2


# TransformConfig
def test_config_requires_bucket(monkeypatch):
    monkeypatch.delenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_S3_BUCKET"):
        TransformConfig.from_env()


def test_config_rejects_placeholder_bucket():
    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_S3_BUCKET"):
        TransformConfig(
            aws_destination_activities_s3_bucket="<YOUR_RAW_BUCKET_NAME>"
        ).validate()


def test_config_ignores_old_raw_and_transformed_bucket_variables(monkeypatch):
    monkeypatch.delenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", raising=False)
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET", "raw")
    monkeypatch.setenv(
        "AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET", "transformed"
    )

    with pytest.raises(ValueError, match="AWS_DESTINATION_ACTIVITIES_S3_BUCKET"):
        TransformConfig.from_env()


def test_config_reads_bucket_arn_and_prefixes(monkeypatch):
    monkeypatch.setenv(
        "AWS_DESTINATION_ACTIVITIES_S3_BUCKET", " arn:aws:s3:::travel-bucket "
    )
    monkeypatch.setenv("RAW_PREFIX", " raw/destination_activities/ ")
    monkeypatch.setenv("TRANSFORMED_PREFIX", " transformed/destination_activities/ ")

    config = TransformConfig.from_env()

    assert config.aws_destination_activities_s3_bucket == "travel-bucket"
    assert config.raw_prefix == "raw/destination_activities/"
    assert config.transformed_prefix == "transformed/destination_activities/"


def test_config_prefixes_default_to_empty(monkeypatch):
    monkeypatch.setenv("AWS_DESTINATION_ACTIVITIES_S3_BUCKET", "travel-bucket")
    monkeypatch.delenv("RAW_PREFIX", raising=False)
    monkeypatch.delenv("TRANSFORMED_PREFIX", raising=False)

    config = TransformConfig.from_env()

    assert config.raw_prefix == ""
    assert config.transformed_prefix == ""

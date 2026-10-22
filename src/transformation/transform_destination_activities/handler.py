import io
import json
import logging
import os
import posixpath
import re
import unicodedata
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Configuration
@dataclass(frozen=True)
class TransformConfig:

    aws_destination_activities_s3_bucket: str
    raw_prefix: str = ""
    transformed_prefix: str = ""

    @staticmethod
    def _bucket_from_env(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if value.startswith("arn:"):
            value = value.split(":::", 1)[-1]
        return value

    @classmethod
    def from_env(cls) -> "TransformConfig":
        config = cls(
            aws_destination_activities_s3_bucket=cls._bucket_from_env(
                "AWS_DESTINATION_ACTIVITIES_S3_BUCKET"
            ),
            raw_prefix=os.environ.get("RAW_PREFIX", "").strip(),
            transformed_prefix=os.environ.get("TRANSFORMED_PREFIX", "").strip(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "AWS_DESTINATION_ACTIVITIES_S3_BUCKET": self.aws_destination_activities_s3_bucket
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )


def _folder(prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def _is_places_key(key: str) -> bool:
    return posixpath.basename(key).startswith("places_") and key.endswith(".json")


# Locating the input
class RawObjectLocator:

    def __init__(
        self,
        default_bucket: str,
        raw_prefix: str = "",
        client=None,
        today: Callable[[], date] = None,
    ):
        self._default_bucket = default_bucket
        self._raw_prefix = _folder(raw_prefix)
        self._client = client
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def locate(self, event: Dict) -> Tuple[str, List[str]]:
        bucket, keys = self._locate(event)
        places_keys = [k for k in keys if _is_places_key(k)]
        if not places_keys:
            raise ValueError(f"No places_*.json files to transform in {keys}")
        return bucket, places_keys

    def _locate(self, event: Dict) -> Tuple[str, List[str]]:
        if not isinstance(event, dict):
            raise ValueError("Event must be a JSON object")

        if isinstance(event.get("Payload"), dict):
            event = event["Payload"]

        records = event.get("Records")
        if records:
            # S3 notifications URL-encode keys ("=" -> "%3D", spaces -> "+").
            bucket = records[0]["s3"]["bucket"]["name"]
            return bucket, [
                urllib.parse.unquote_plus(r["s3"]["object"]["key"]) for r in records
            ]

        detail = event.get("detail")
        if isinstance(detail, dict) and "object" in detail:
            return detail["bucket"]["name"], [detail["object"]["key"]]

        bucket = event.get("bucket") or self._default_bucket
        places_keys = event.get("places_keys")
        if places_keys:
            return bucket, list(places_keys)
        key = event.get("key")
        if key:
            return bucket, [key]
        extract_date = event.get("extract_date") or f"{self._today():%Y-%m-%d}"
        return bucket, self._partition_keys(bucket, extract_date)

    def _partition_keys(self, bucket: str, extract_date: str) -> List[str]:
        partition = f"{self._raw_prefix}extract_date={extract_date}/"
        client = self._client or boto3.client("s3")
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=partition
        ):
            keys.extend(
                obj["Key"]
                for obj in page.get("Contents", [])
                if _is_places_key(obj["Key"])
            )
        if not keys:
            raise ValueError(
                f"s3://{bucket}/{partition} has no places_*.json files — "
                "run extract_destination_activities first or pass {'extract_date'} / {'bucket', 'key'}"
            )
        logger.info(
            "Found %d raw places files in s3://%s/%s", len(keys), bucket, partition
        )
        return sorted(keys)


# S3 read
class S3RawReader:

    def __init__(self, client=None):
        self._client = client or boto3.client("s3")

    def read(self, bucket: str, key: str) -> Dict:
        body = self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        logger.info("Read s3://%s/%s (%d bytes)", bucket, key, len(body))
        return json.loads(body)


# Transformation
class InvalidRawPayloadError(Exception):
    """Raised when the raw JSON is missing the sections we need."""


class ActivitiesTransformer:

    STRING_FIELDS = ("name", "country_code", "state", "city", "formatted")
    COORDINATE_FIELDS = (("longitude", "lon"), ("latitude", "lat"))

    # Double quotes and slashes anywhere; single quotes only when they wrap a
    # word, so apostrophes like "Joe's" survive.
    # '"Flora" The Gatherer of Dreams' -> 'Flora The Gatherer of Dreams'
    _NAME_JUNK = re.compile(
        r"[\"\u201c\u201d\u201e/\\]|(?<!\w)['\u2018\u2019`]|['\u2018\u2019`](?!\w)"
    )

    # Placeholder names: "Untitled", "UNTITLED #3", "(untitled)", "Untitled_7",
    # "Un-titled", "un titled". The separated form needs a word break before
    # "un" so names like "Fun Titled Show" survive.
    _UNTITLED = re.compile(r"untitled|\bun[\W_]+titled")

    SCHEMA = pa.schema(
        [
            pa.field("search_city", pa.string()),
            pa.field("name", pa.string()),
            pa.field("country_code", pa.string()),
            pa.field("state", pa.string()),
            pa.field("city", pa.string()),
            pa.field("formatted", pa.string()),
            pa.field("longitude", pa.float64()),
            pa.field("latitude", pa.float64()),
            pa.field("categories", pa.list_(pa.string())),
        ]
    )

    def transform(self, payload: Dict) -> List[Dict[str, object]]:
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list):
            raise InvalidRawPayloadError("Raw payload has no features array")

        rows = [
            row for row in (self._clean_feature(f) for f in features) if row is not None
        ]
        logger.info("Kept %d of %d features", len(rows), len(features))
        return rows

    def _clean_feature(self, feature: object) -> Optional[Dict[str, object]]:
        properties = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(properties, dict):
            return None

        # Added by the extract; null for pages extracted before it was.
        search_city = properties.get("search_city")
        row: Dict[str, object] = {
            "search_city": (
                search_city.strip()
                if isinstance(search_city, str) and search_city.strip()
                else None
            )
        }
        for field_name in self.STRING_FIELDS:
            value = properties.get(field_name)
            if not isinstance(value, str) or not value.strip():
                return None
            row[field_name] = value.strip()

        row["name"] = self.clean_name(row["name"])
        if not row["name"] or self.is_untitled(row["name"]):
            return None

        # From properties.lon / properties.lat, not geometry.coordinates.
        for column, source in self.COORDINATE_FIELDS:
            value = properties.get(source)
            row[column] = (
                float(value)
                if isinstance(value, (int, float)) and not isinstance(value, bool)
                else None
            )

        categories = properties.get("categories")
        if not isinstance(categories, list):
            return None
        row["categories"] = [c for c in categories if isinstance(c, str) and c]
        if not row["categories"]:
            return None
        return row

    @classmethod
    def clean_name(cls, name: str) -> str:
        return re.sub(r"\s+", " ", cls._NAME_JUNK.sub(" ", name)).strip()

    @classmethod
    def is_untitled(cls, name: str) -> bool:
        # NFKC + casefold so any casing or full-width letters still match.
        return bool(
            cls._UNTITLED.search(unicodedata.normalize("NFKC", name).casefold())
        )


class DestinationGrouper:

    def __init__(self, schema: pa.Schema = ActivitiesTransformer.SCHEMA):
        self._schema = schema

    def group(
        self, rows_by_partition: Dict[str, List[Dict[str, object]]]
    ) -> Dict[str, pa.Table]:
        # Every page under one destination=<slug>/ folder goes into one table,
        # so the combine job reads a single file per destination.
        tables: Dict[str, pa.Table] = {}
        for partition, rows in sorted(rows_by_partition.items()):
            seen = set()
            unique = []
            for row in rows:
                identity = (row["name"], row["formatted"])
                if identity in seen:
                    continue
                seen.add(identity)
                unique.append(row)
            if unique:
                tables[partition] = pa.Table.from_pylist(unique, schema=self._schema)
        return tables


# S3 write
class TransformedKeyBuilder:

    def __init__(self, raw_prefix: str, transformed_prefix: str):
        self._raw_prefix = _folder(raw_prefix)
        self._transformed_prefix = _folder(transformed_prefix)

    def partition(self, raw_key: str) -> str:
        relative = (
            raw_key[len(self._raw_prefix) :]
            if raw_key.startswith(self._raw_prefix)
            else raw_key
        )
        return posixpath.dirname(relative)

    @staticmethod
    def destination(partition: str) -> Optional[str]:
        # "extract_date=2026-10-16/destination=san_francisco_united_states"
        # -> "san_francisco_united_states"; None for pages without that folder.
        for part in reversed(partition.split("/")):
            if part.startswith("destination="):
                return part[len("destination=") :] or None
        return None

    def build(self, partition: str) -> str:
        folder = f"{partition}/" if partition else ""
        destination = self.destination(partition)
        name = f"activities_{self._slug(destination)}" if destination else "activities"
        return f"{self._transformed_prefix}{folder}{name}.parquet"

    @staticmethod
    def _slug(part: str) -> str:
        # "San José" -> "san_josé", "Winston-Salem" -> "winston_salem"
        slug = re.sub(r"[^\w]+", "_", part.lower()).strip("_")
        return slug or "unknown"


class S3ParquetWriter:

    def __init__(self, bucket: str, client=None):
        self._bucket = bucket
        self._client = client or boto3.client("s3")

    @property
    def bucket(self) -> str:
        return self._bucket

    def write(self, key: str, table: pa.Table) -> str:
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/vnd.apache.parquet",
        )
        return key


# Orchestration
class TransformError(Exception):
    """Raised when every raw file failed to transform, or nothing was left to write."""


class TransformService:
    """Coordinates locate -> read -> transform (per page) -> group by destination -> name -> store."""

    def __init__(
        self,
        locator: RawObjectLocator,
        reader: S3RawReader,
        transformer: ActivitiesTransformer,
        grouper: DestinationGrouper,
        key_builder: TransformedKeyBuilder,
        writer: S3ParquetWriter,
    ):
        self._locator = locator
        self._reader = reader
        self._transformer = transformer
        self._grouper = grouper
        self._key_builder = key_builder
        self._writer = writer

    def run(self, event: Dict) -> Dict[str, object]:
        raw_bucket, raw_keys = self._locator.locate(event)

        rows_by_partition: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        read, failed = [], []
        for raw_key in raw_keys:
            try:
                rows = self._transformer.transform(
                    self._reader.read(raw_bucket, raw_key)
                )
            except (InvalidRawPayloadError, ValueError) as err:
                # One bad page shouldn't throw away the others.
                # (json.JSONDecodeError is a ValueError.)
                logger.error(
                    "Could not transform s3://%s/%s: %s", raw_bucket, raw_key, err
                )
                failed.append({"source_key": raw_key, "error": str(err)})
                continue
            rows_by_partition[self._key_builder.partition(raw_key)].extend(rows)
            read.append(raw_key)

        if not read:
            raise TransformError(
                f"All {len(failed)} raw places files failed to transform: {failed}"
            )

        tables = self._grouper.group(rows_by_partition)
        if not tables:
            raise TransformError(f"No complete activities found in {read}")

        written = []
        for partition, table in tables.items():
            destination = self._key_builder.destination(partition)
            key = self._writer.write(self._key_builder.build(partition), table)
            logger.info(
                "Wrote %d activities for %s to s3://%s/%s",
                table.num_rows,
                destination or partition,
                self._writer.bucket,
                key,
            )
            written.append(
                {
                    "destination": destination,
                    "key": key,
                    "row_count": table.num_rows,
                }
            )

        return {
            "status": "PARTIAL" if failed else "SUCCEEDED",
            "source_bucket": raw_bucket,
            "source_keys": read,
            "bucket": self._writer.bucket,
            "file_count": len(written),
            "row_count": sum(item["row_count"] for item in written),
            "written": written,
            "failed": failed,
        }


def build_service(config: TransformConfig) -> TransformService:
    return TransformService(
        locator=RawObjectLocator(
            config.aws_destination_activities_s3_bucket, config.raw_prefix
        ),
        reader=S3RawReader(),
        transformer=ActivitiesTransformer(),
        grouper=DestinationGrouper(),
        key_builder=TransformedKeyBuilder(config.raw_prefix, config.transformed_prefix),
        writer=S3ParquetWriter(config.aws_destination_activities_s3_bucket),
    )


_service: Optional[TransformService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(TransformConfig.from_env())
    return _service.run(event or {})

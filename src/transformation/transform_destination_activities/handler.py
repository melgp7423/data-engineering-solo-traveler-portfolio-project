"""
Lambda: read every raw Geoapify Places JSON page in one ingest_date partition
from S3, keep only complete activities and the fields the pipeline needs,
group them by city, and write one Parquet file per city back to S3 —
OOP / Single Responsibility Principle.

Input  (raw bucket):         <RAW_PREFIX>ingest_date=YYYY-MM-DD/places_HHMMSS_<run>_page000.json   (one or more pages per run)
Output (transformed bucket): <TRANSFORMED_PREFIX>ingest_date=YYYY-MM-DD/activities_detroit.parquet  (one per city)

The geocode_*.json files in the same partition are ignored.

A feature is kept only if its "properties" has a non-empty value for every
one of: name, country_code, city, formatted, categories (a non-empty list).
Anything else is dropped. A city's activities can be spread across several
pages, so all pages in the partition are read before grouping.

Output layout: one row per activity (columnar):

    name, country_code, city, formatted, categories (list of strings)

Classes and their one job:
    TransformConfig            -> read & validate settings
    RawObjectLocator           -> work out which raw S3 objects to transform from the event
    S3RawReader                -> read the raw JSON from S3
    ActivitiesTransformer      -> raw payload -> list of clean activity rows
    CityGrouper                -> rows -> one pyarrow Table per city
    TransformedKeyBuilder      -> name the Parquet object
    S3ParquetWriter            -> serialize the Table to Parquet and write it to S3
    TransformService           -> orchestrate locate -> read -> transform -> group -> name -> store
    lambda_handler             -> Lambda entry point

Environment variables (placeholders shown):
    AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET           = <YOUR_RAW_BUCKET_NAME>
    AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET   = <YOUR_TRANSFORMED_BUCKET_NAME>
    RAW_PREFIX                                              = ""   folder above ingest_date= in the raw bucket ("" = bucket root)
    TRANSFORMED_PREFIX                                      = ""   folder above ingest_date= in the transformed bucket

Supported events:
    Step Functions (output of the extract Lambda):  {"bucket": "...", "places_keys": [...], "ingest_date": "YYYY-MM-DD", ...}
                                                    -> those places pages
    Schedule / manual test ({}):                    every places_*.json in today's (UTC) partition
    Another day:                                    {"ingest_date": "YYYY-MM-DD"}
    One file only:                                  {"bucket": "...", "key": "..."}
    S3 ObjectCreated notification:                  {"Records": [{"s3": {...}}]} -> those files

Requires pyarrow (see requirements.txt).
"""

import io
import json
import logging
import os
import posixpath
import re
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


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TransformConfig:
    """Holds and validates all runtime settings."""

    aws_destination_activities_raw_data_s3_bucket: str
    aws_destination_activities_transformed_data_s3_bucket: str
    raw_prefix: str = ""
    transformed_prefix: str = ""

    @staticmethod
    def _bucket_from_env(name: str) -> str:
        """Read a bucket env var, accepting a bare name or an ARN (arn:aws:s3:::<bucket>)."""
        value = os.environ.get(name, "").strip()
        if value.startswith("arn:"):
            value = value.split(":::", 1)[-1]
        return value

    @classmethod
    def from_env(cls) -> "TransformConfig":
        config = cls(
            aws_destination_activities_raw_data_s3_bucket=cls._bucket_from_env(
                "AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET"
            ),
            aws_destination_activities_transformed_data_s3_bucket=cls._bucket_from_env(
                "AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET"
            ),
            raw_prefix=os.environ.get("RAW_PREFIX", ""),
            transformed_prefix=os.environ.get("TRANSFORMED_PREFIX", ""),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET": self.aws_destination_activities_raw_data_s3_bucket,
            "AWS_DESTINATION_ACTIVITIES_TRANSFORMED_DATA_S3_BUCKET": (
                self.aws_destination_activities_transformed_data_s3_bucket
            ),
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def _folder(prefix: str) -> str:
    """Normalize a prefix to "" (bucket root) or "some/folder/"."""
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def _is_places_key(key: str) -> bool:
    """Only the places_*.json pages hold activities; geocode_*.json does not."""
    return posixpath.basename(key).startswith("places_") and key.endswith(".json")


# ---------------------------------------------------------------------------
# Locating the input
# ---------------------------------------------------------------------------
class RawObjectLocator:
    """
    Works out which raw places pages to transform. Accepted shapes:
      - Step Functions lambda:invoke result, which wraps the payload:
        {"Payload": {...}, "StatusCode": 200, ...}
      - extract_destination_activities output: {"bucket", "places_keys": [...]}
      - S3 notification: {"Records": [{"s3": ...}, ...]} with URL-encoded keys
      - S3 event via EventBridge: {"detail": {"bucket": {"name"}, "object": {"key"}}}
      - a single file: {"bucket", "key"}
      - otherwise every places_*.json under <raw_prefix>ingest_date=<date>/,
        where <date> is the event's "ingest_date" or today in UTC
    The bucket falls back to the configured raw bucket when the event has none.
    Keys that aren't places pages (e.g. geocode_*.json) are skipped.
    """

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
            return bucket, [urllib.parse.unquote_plus(r["s3"]["object"]["key"]) for r in records]

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
        ingest_date = event.get("ingest_date") or f"{self._today():%Y-%m-%d}"
        return bucket, self._partition_keys(bucket, ingest_date)

    def _partition_keys(self, bucket: str, ingest_date: str) -> List[str]:
        partition = f"{self._raw_prefix}ingest_date={ingest_date}/"
        client = self._client or boto3.client("s3")
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=partition):
            keys.extend(obj["Key"] for obj in page.get("Contents", []) if _is_places_key(obj["Key"]))
        if not keys:
            raise ValueError(
                f"s3://{bucket}/{partition} has no places_*.json files — "
                "run extract_destination_activities first or pass {'ingest_date'} / {'bucket', 'key'}"
            )
        logger.info("Found %d raw places files in s3://%s/%s", len(keys), bucket, partition)
        return sorted(keys)


# ---------------------------------------------------------------------------
# S3 read
# ---------------------------------------------------------------------------
class S3RawReader:
    """Reads and parses a raw JSON object from S3."""

    def __init__(self, client=None):
        self._client = client or boto3.client("s3")

    def read(self, bucket: str, key: str) -> Dict:
        body = self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        logger.info("Read s3://%s/%s (%d bytes)", bucket, key, len(body))
        return json.loads(body)


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
class InvalidRawPayloadError(Exception):
    """Raised when the raw JSON is missing the sections we need."""


class ActivitiesTransformer:
    """
    Turns a Places FeatureCollection into clean activity rows. A feature is
    kept only when its properties has a non-empty name, country_code, city
    and formatted, and a non-empty categories list; every category string is
    kept as-is.
    """

    STRING_FIELDS = ("name", "country_code", "city", "formatted")

    SCHEMA = pa.schema([
        pa.field("name", pa.string()),
        pa.field("country_code", pa.string()),
        pa.field("city", pa.string()),
        pa.field("formatted", pa.string()),
        pa.field("categories", pa.list_(pa.string())),
    ])

    def transform(self, payload: Dict) -> List[Dict[str, object]]:
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list):
            raise InvalidRawPayloadError("Raw payload has no features array")

        rows = [row for row in (self._clean_feature(f) for f in features) if row is not None]
        logger.info("Kept %d of %d features", len(rows), len(features))
        return rows

    def _clean_feature(self, feature: object) -> Optional[Dict[str, object]]:
        properties = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(properties, dict):
            return None

        row: Dict[str, object] = {}
        for field_name in self.STRING_FIELDS:
            value = properties.get(field_name)
            if not isinstance(value, str) or not value.strip():
                return None
            row[field_name] = value.strip()

        categories = properties.get("categories")
        if not isinstance(categories, list):
            return None
        row["categories"] = [c for c in categories if isinstance(c, str) and c]
        if not row["categories"]:
            return None
        return row


class CityGrouper:
    """
    Groups rows by (partition, city) and builds one Table per group. The
    partition is the raw key's folder (e.g. "ingest_date=2026-10-09"), so
    pages from different days never end up in the same file. The same place
    showing up on two pages or in two runs that day is only kept once.
    """

    def __init__(self, schema: pa.Schema = ActivitiesTransformer.SCHEMA):
        self._schema = schema

    def group(self, rows_by_partition: Dict[str, List[Dict[str, object]]]) -> Dict[Tuple[str, str], pa.Table]:
        grouped: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
        seen = set()
        for partition, rows in rows_by_partition.items():
            for row in rows:
                identity = (partition, row["city"], row["name"], row["formatted"])
                if identity in seen:
                    continue
                seen.add(identity)
                grouped[(partition, row["city"])].append(row)

        return {
            group_key: pa.Table.from_pylist(rows, schema=self._schema)
            for group_key, rows in sorted(grouped.items())
        }


# ---------------------------------------------------------------------------
# S3 write
# ---------------------------------------------------------------------------
class TransformedKeyBuilder:
    """
    Puts each city's file in the same ingest_date= partition as its raw pages,
    under the transformed prefix, named after the city:
        ingest_date=2026-10-09/places_021651_fde87987_page000.json  (city "Detroit")
     -> ingest_date=2026-10-09/activities_detroit.parquet
    """

    def __init__(self, raw_prefix: str, transformed_prefix: str):
        self._raw_prefix = _folder(raw_prefix)
        self._transformed_prefix = _folder(transformed_prefix)

    def partition(self, raw_key: str) -> str:
        relative = raw_key[len(self._raw_prefix):] if raw_key.startswith(self._raw_prefix) else raw_key
        return posixpath.dirname(relative)

    def build(self, partition: str, city: str) -> str:
        folder = f"{partition}/" if partition else ""
        return f"{self._transformed_prefix}{folder}activities_{self._slug(city)}.parquet"

    @staticmethod
    def _slug(city: str) -> str:
        # "San José" -> "san_josé", "Winston-Salem" -> "winston_salem"
        slug = re.sub(r"[^\w]+", "_", city.lower()).strip("_")
        return slug or "unknown"


class S3ParquetWriter:
    """Serializes a Table to Snappy-compressed Parquet and writes it to S3."""

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


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
class TransformError(Exception):
    """Raised when every raw file failed to transform, or nothing was left to write."""


class TransformService:
    """Coordinates locate -> read -> transform (per page) -> group by city -> name -> store."""

    def __init__(
        self,
        locator: RawObjectLocator,
        reader: S3RawReader,
        transformer: ActivitiesTransformer,
        grouper: CityGrouper,
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
                rows = self._transformer.transform(self._reader.read(raw_bucket, raw_key))
            except (InvalidRawPayloadError, ValueError) as err:
                # One bad page shouldn't throw away the others.
                # (json.JSONDecodeError is a ValueError.)
                logger.error("Could not transform s3://%s/%s: %s", raw_bucket, raw_key, err)
                failed.append({"source_key": raw_key, "error": str(err)})
                continue
            rows_by_partition[self._key_builder.partition(raw_key)].extend(rows)
            read.append(raw_key)

        if not read:
            raise TransformError(f"All {len(failed)} raw places files failed to transform: {failed}")

        tables = self._grouper.group(rows_by_partition)
        if not tables:
            raise TransformError(f"No complete activities found in {read}")

        written = []
        for (partition, city), table in tables.items():
            key = self._writer.write(self._key_builder.build(partition, city), table)
            logger.info("Wrote %d activities for %s to s3://%s/%s", table.num_rows, city, self._writer.bucket, key)
            written.append({"city": city, "key": key, "row_count": table.num_rows})

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
    """Composition root: the only place that wires the pieces together."""
    return TransformService(
        locator=RawObjectLocator(config.aws_destination_activities_raw_data_s3_bucket, config.raw_prefix),
        reader=S3RawReader(),
        transformer=ActivitiesTransformer(),
        grouper=CityGrouper(),
        key_builder=TransformedKeyBuilder(config.raw_prefix, config.transformed_prefix),
        writer=S3ParquetWriter(config.aws_destination_activities_transformed_data_s3_bucket),
    )


_service: Optional[TransformService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(TransformConfig.from_env())
    return _service.run(event or {})

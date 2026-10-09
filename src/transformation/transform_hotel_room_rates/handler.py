"""
Lambda: read every raw Google Hotels JSON file in one ingest_date partition
from S3, keep only the fields the pipeline needs, and write each one back to
S3 as Parquet — OOP / Single Responsibility Principle.

Input  (raw bucket):         <RAW_PREFIX>ingest_date=YYYY-MM-DD/hotels_ABQ_....json   (many per day, one per city)
Output (transformed bucket): <TRANSFORMED_PREFIX>ingest_date=YYYY-MM-DD/hotels_ABQ_....parquet

Output layout: one row per property (columnar). Every field in
search_metadata and search_parameters is kept and repeated on each row with
a "search_metadata_" / "search_parameters_" prefix, so each property row
carries the search it came from:

    search_metadata_id, ..., search_metadata_total_time_taken,
    search_parameters_engine, ..., search_parameters_hotel_class,
    name, link, check_in_time, check_out_time, total_rate_extracted_lowest,
    extracted_hotel_class, overall_rating, amenities (list of strings)

Classes and their one job:
    TransformConfig          -> read & validate settings
    RawObjectLocator         -> work out which raw S3 objects to transform from the event
    S3RawReader              -> read the raw JSON from S3
    HotelsTransformer        -> raw payload -> pyarrow Table (metadata + parameters + properties)
    TransformedKeyBuilder    -> name the Parquet object
    S3ParquetWriter          -> serialize the Table to Parquet and write it to S3
    TransformService         -> orchestrate locate -> (read -> transform -> name -> store) per file
    lambda_handler           -> Lambda entry point

Environment variables (placeholders shown):
    AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET           = <YOUR_RAW_BUCKET_NAME>
    AWS_HOTEL_ROOM_RATES_TRANSFORMED_DATA_S3_BUCKET   = <YOUR_TRANSFORMED_BUCKET_NAME>
    RAW_PREFIX                                        = ""   folder above ingest_date= in the raw bucket ("" = bucket root)
    TRANSFORMED_PREFIX                                = ""   folder above ingest_date= in the transformed bucket

Supported events:
    Step Functions (output of the extract Lambda):  {"bucket": "...", "ingest_date": "YYYY-MM-DD", ...}
                                                    -> every .json in that partition
    Schedule / manual test ({}):                    every .json in today's (UTC) partition
    Another day:                                    {"ingest_date": "YYYY-MM-DD"}
    One file only:                                  {"bucket": "...", "key": "..."}
    S3 ObjectCreated notification:                  {"Records": [{"s3": {...}}]} -> those files

Requires pyarrow (see requirements.txt).
"""

import io
import json
import logging
import os
import urllib.parse
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

    aws_hotel_room_rates_raw_data_s3_bucket: str
    aws_hotel_room_rates_transformed_data_s3_bucket: str
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
            aws_hotel_room_rates_raw_data_s3_bucket=cls._bucket_from_env("AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET"),
            aws_hotel_room_rates_transformed_data_s3_bucket=cls._bucket_from_env(
                "AWS_HOTEL_ROOM_RATES_TRANSFORMED_DATA_S3_BUCKET"
            ),
            raw_prefix=os.environ.get("RAW_PREFIX", ""),
            transformed_prefix=os.environ.get("TRANSFORMED_PREFIX", ""),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET": self.aws_hotel_room_rates_raw_data_s3_bucket,
            "AWS_HOTEL_ROOM_RATES_TRANSFORMED_DATA_S3_BUCKET": self.aws_hotel_room_rates_transformed_data_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def _folder(prefix: str) -> str:
    """Normalize a prefix to "" (bucket root) or "some/folder/"."""
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


# ---------------------------------------------------------------------------
# Locating the input
# ---------------------------------------------------------------------------
class RawObjectLocator:
    """
    Works out which raw objects to transform. Accepted shapes:
      - Step Functions lambda:invoke result, which wraps the payload:
        {"Payload": {...}, "StatusCode": 200, ...}
      - S3 notification: {"Records": [{"s3": ...}, ...]} with URL-encoded keys
      - S3 event via EventBridge: {"detail": {"bucket": {"name"}, "object": {"key"}}}
      - a single file: {"bucket", "key"}
      - otherwise every .json under <raw_prefix>ingest_date=<date>/, where
        <date> is the event's "ingest_date" (the extract Lambda returns one)
        or today in UTC
    The bucket falls back to the configured raw bucket when the event has none.
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
            keys.extend(obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".json"))
        if not keys:
            raise ValueError(
                f"s3://{bucket}/{partition} has no .json files — "
                "run extract_hotel_room_rates first or pass {'ingest_date'} / {'bucket', 'key'}"
            )
        logger.info("Found %d raw hotel files in s3://%s/%s", len(keys), bucket, partition)
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


class HotelsTransformer:
    """
    Flattens the raw payload into one row per property. All of
    search_metadata and search_parameters is kept and copied onto every row;
    from each property only PROPERTY_FIELDS are kept. Columns get explicit
    types so every file has the same schema for Glue/Athena, no matter which
    optional fields a given response contains.
    """

    METADATA_PREFIX = "search_metadata_"
    PARAMETERS_PREFIX = "search_parameters_"

    # Known search_metadata fields and their types. Any other field SerpApi
    # adds later is still kept, as a string column (see _section_columns).
    METADATA_FIELDS = {
        "id": pa.string(),
        "status": pa.string(),
        "json_endpoint": pa.string(),
        "markdown_endpoint": pa.string(),
        "created_at": pa.timestamp("ms", tz="UTC"),
        "processed_at": pa.timestamp("ms", tz="UTC"),
        "google_hotels_url": pa.string(),
        "raw_html_file": pa.string(),
        "prettify_html_file": pa.string(),
        "total_time_taken": pa.float64(),  # seconds
    }

    PARAMETER_FIELDS = {
        "engine": pa.string(),
        "q": pa.string(),
        "gl": pa.string(),
        "hl": pa.string(),
        "currency": pa.string(),
        # Plain "YYYY-MM-DD" strings, same as the flights Parquet.
        "check_in_date": pa.string(),
        "check_out_date": pa.string(),
        "adults": pa.int64(),
        "children": pa.int64(),
        "hotel_class": pa.string(),  # e.g. "3,4,5"
    }

    PROPERTY_FIELDS = {
        "name": pa.string(),
        "link": pa.string(),
        "check_in_time": pa.string(),
        "check_out_time": pa.string(),
        "total_rate_extracted_lowest": pa.float64(),  # whole stay, in search_parameters_currency
        "extracted_hotel_class": pa.int64(),
        "overall_rating": pa.float64(),
        "amenities": pa.list_(pa.string()),
    }

    def transform(self, payload: Dict) -> pa.Table:
        metadata = payload.get("search_metadata")
        if not isinstance(metadata, dict) or not metadata:
            raise InvalidRawPayloadError("Raw payload has no search_metadata")
        parameters = payload.get("search_parameters")
        if not isinstance(parameters, dict) or not parameters:
            raise InvalidRawPayloadError("Raw payload has no search_parameters")
        properties = payload.get("properties")
        if not isinstance(properties, list):
            raise InvalidRawPayloadError("Raw payload has no properties array")

        section_columns = {
            **self._section_columns(metadata, self.METADATA_FIELDS, self.METADATA_PREFIX),
            **self._section_columns(parameters, self.PARAMETER_FIELDS, self.PARAMETERS_PREFIX),
        }
        rows = [self._clean_property(p) for p in properties if isinstance(p, dict)]

        columns: Dict[str, pa.Array] = {}
        fields: List[pa.Field] = []
        for name, (value, arrow_type) in section_columns.items():
            columns[name] = pa.array([value] * len(rows), type=arrow_type)
            fields.append(pa.field(name, arrow_type))
        for name, arrow_type in self.PROPERTY_FIELDS.items():
            columns[name] = pa.array([row[name] for row in rows], type=arrow_type)
            fields.append(pa.field(name, arrow_type))

        return pa.Table.from_pydict(columns, schema=pa.schema(fields))

    def _section_columns(
        self, section: Dict, known_fields: Dict[str, pa.DataType], prefix: str
    ) -> Dict[str, Tuple[object, pa.DataType]]:
        columns = {}
        for field_name, arrow_type in known_fields.items():
            value = section.get(field_name)
            if isinstance(value, dict) and len(value) == 1:
                # SerpApi sometimes wraps numbers, e.g. "total_time_taken": {"float": 2.25}
                value = next(iter(value.values()))
            if pa.types.is_timestamp(arrow_type):
                value = self._parse_timestamp(value)
            elif pa.types.is_string(arrow_type) and value is not None:
                value = str(value)
            columns[prefix + field_name] = (value, arrow_type)
        for field_name, value in section.items():
            if field_name not in known_fields:
                logger.warning("Unexpected %s field %r kept as string", prefix.rstrip("_"), field_name)
                text = value if isinstance(value, str) or value is None else json.dumps(value)
                columns[prefix + field_name] = (text, pa.string())
        return columns

    @staticmethod
    def _clean_property(prop: Dict) -> Dict[str, object]:
        total_rate = prop.get("total_rate") if isinstance(prop.get("total_rate"), dict) else {}
        amenities = prop.get("amenities") if isinstance(prop.get("amenities"), list) else []
        return {
            "name": prop.get("name"),
            "link": prop.get("link"),
            "check_in_time": prop.get("check_in_time"),
            "check_out_time": prop.get("check_out_time"),
            "total_rate_extracted_lowest": total_rate.get("extracted_lowest"),
            "extracted_hotel_class": prop.get("extracted_hotel_class"),
            "overall_rating": prop.get("overall_rating"),
            "amenities": [str(a) for a in amenities if a],
        }

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        # SerpApi uses both "2026-10-09T02:18:19.193Z" and "2026-10-07 00:47:06 UTC".
        if not value:
            return None
        if value.endswith(" UTC"):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# S3 write
# ---------------------------------------------------------------------------
class TransformedKeyBuilder:
    """
    Mirrors the raw key under the transformed prefix and swaps .json for
    .parquet, so the ingest_date= partition and file name carry over:
        ingest_date=2026-10-09/hotels_ABQ_2026-10-16_2026-10-23_021651_fde87987.json
     -> ingest_date=2026-10-09/hotels_ABQ_2026-10-16_2026-10-23_021651_fde87987.parquet
    """

    def __init__(self, raw_prefix: str, transformed_prefix: str):
        self._raw_prefix = _folder(raw_prefix)
        self._transformed_prefix = _folder(transformed_prefix)

    def build(self, raw_key: str) -> str:
        relative = raw_key[len(self._raw_prefix):] if raw_key.startswith(self._raw_prefix) else raw_key
        stem = relative[: -len(".json")] if relative.endswith(".json") else relative
        return f"{self._transformed_prefix}{stem}.parquet"


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
    """Raised when every raw file in a run failed to transform."""


class TransformService:
    """Coordinates locate -> (read -> transform -> name -> store) for each raw file."""

    def __init__(
        self,
        locator: RawObjectLocator,
        reader: S3RawReader,
        transformer: HotelsTransformer,
        key_builder: TransformedKeyBuilder,
        writer: S3ParquetWriter,
    ):
        self._locator = locator
        self._reader = reader
        self._transformer = transformer
        self._key_builder = key_builder
        self._writer = writer

    def run(self, event: Dict) -> Dict[str, object]:
        raw_bucket, raw_keys = self._locator.locate(event)

        written, failed = [], []
        for raw_key in raw_keys:
            try:
                payload = self._reader.read(raw_bucket, raw_key)
                table = self._transformer.transform(payload)
            except (InvalidRawPayloadError, ValueError) as err:
                # One bad city file shouldn't throw away the others.
                # (json.JSONDecodeError and pyarrow.ArrowInvalid are ValueErrors.)
                logger.error("Could not transform s3://%s/%s: %s", raw_bucket, raw_key, err)
                failed.append({"source_key": raw_key, "error": str(err)})
                continue
            key = self._writer.write(self._key_builder.build(raw_key), table)
            logger.info("Wrote %d properties to s3://%s/%s", table.num_rows, self._writer.bucket, key)
            written.append({
                "source_key": raw_key,
                "key": key,
                "search_id": payload["search_metadata"].get("id"),
                "row_count": table.num_rows,
            })

        if not written:
            raise TransformError(f"All {len(failed)} raw hotel files failed to transform: {failed}")

        return {
            "status": "PARTIAL" if failed else "SUCCEEDED",
            "source_bucket": raw_bucket,
            "bucket": self._writer.bucket,
            "file_count": len(written),
            "row_count": sum(item["row_count"] for item in written),
            "written": written,
            "failed": failed,
        }


def build_service(config: TransformConfig) -> TransformService:
    """Composition root: the only place that wires the pieces together."""
    return TransformService(
        locator=RawObjectLocator(config.aws_hotel_room_rates_raw_data_s3_bucket, config.raw_prefix),
        reader=S3RawReader(),
        transformer=HotelsTransformer(),
        key_builder=TransformedKeyBuilder(config.raw_prefix, config.transformed_prefix),
        writer=S3ParquetWriter(config.aws_hotel_room_rates_transformed_data_s3_bucket),
    )


_service: Optional[TransformService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(TransformConfig.from_env())
    return _service.run(event or {})

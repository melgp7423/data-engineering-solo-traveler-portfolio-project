"""
Lambda: read one raw Google Flights deals JSON file from S3, keep only the
fields the pipeline needs, and write it back to S3 as Parquet — OOP / Single
Responsibility Principle.

Input  (raw bucket):        airplane_ticket_rates/ingest_date=YYYY-MM-DD/deals_....json
Output (transformed bucket): airplane_ticket_rates/ingest_date=YYYY-MM-DD/deals_....parquet

Output layout: one row per deal (columnar). Every field in search_metadata
is kept and repeated on each row with a "search_metadata_" prefix, so each
deal row carries the search it came from:

    search_metadata_id, search_metadata_status, ..., search_metadata_total_time_taken,
    name, price, outbound_date, return_date, flight_duration, description

An empty or missing description becomes "NO DESCRIPTION".

Classes and their one job:
    TransformConfig          -> read & validate settings
    RawObjectLocator         -> work out which raw S3 object to transform from the event
    S3RawReader              -> read the raw JSON from S3
    DealsTransformer         -> raw payload -> pyarrow Table (metadata + cleaned deals)
    TransformedKeyBuilder    -> name the Parquet object
    S3ParquetWriter          -> serialize the Table to Parquet and write it to S3
    TransformService         -> orchestrate locate -> read -> transform -> name -> store
    lambda_handler           -> Lambda entry point

Environment variables (placeholders shown):
    AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET          = <YOUR_RAW_BUCKET_NAME>
    AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET  = <YOUR_TRANSFORMED_BUCKET_NAME>
    RAW_PREFIX                                      = airplane_ticket_rates/
    TRANSFORMED_PREFIX                              = airplane_ticket_rates/

Supported events:
    Step Functions (output of the extract Lambda): {"bucket": "...", "key": "..."}
    S3 ObjectCreated notification:                 {"Records": [{"s3": {...}}]}
    Schedule / manual test with no key ({}):       newest .json in today's
                                                   ingest_date partition
    Same, for another day:                         {"ingest_date": "YYYY-MM-DD"}

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

    aws_airplane_ticket_raw_data_s3_bucket: str
    aws_airplane_ticket_transformed_data_s3_bucket: str
    raw_prefix: str = "airplane_ticket_rates/"
    transformed_prefix: str = "airplane_ticket_rates/"

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
            aws_airplane_ticket_raw_data_s3_bucket=cls._bucket_from_env("AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET"),
            aws_airplane_ticket_transformed_data_s3_bucket=cls._bucket_from_env(
                "AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET"
            ),
            raw_prefix=os.environ.get("RAW_PREFIX", "airplane_ticket_rates/"),
            transformed_prefix=os.environ.get("TRANSFORMED_PREFIX", "airplane_ticket_rates/"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET": self.aws_airplane_ticket_raw_data_s3_bucket,
            "AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET": self.aws_airplane_ticket_transformed_data_s3_bucket,
        }
        missing = [k for k, v in required.items() if not v or v.startswith("<")]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# Locating the input
# ---------------------------------------------------------------------------
class RawObjectLocator:
    """
    Works out which raw object to transform. Accepted shapes:
      - extract Lambda output passed straight through: {"bucket", "key"}
      - Step Functions lambda:invoke result, which wraps that output:
        {"Payload": {"bucket", "key"}, "StatusCode": 200, ...}
      - S3 notification: {"Records": [{"s3": ...}]} with a URL-encoded key
      - S3 event via EventBridge: {"detail": {"bucket": {"name"}, "object": {"key"}}}
      - no key at all (schedule, console test): the newest .json under
        <raw_prefix>/ingest_date=<today UTC>/ in the raw bucket, or under the
        event's "ingest_date" partition when it gives one
    The bucket falls back to the configured raw bucket when the event only
    carries a key.
    """

    def __init__(
        self,
        default_bucket: str,
        raw_prefix: str = "airplane_ticket_rates/",
        client=None,
        today: Callable[[], date] = None,
    ):
        self._default_bucket = default_bucket
        self._raw_prefix = raw_prefix.rstrip("/")
        self._client = client
        self._today = today or (lambda: datetime.now(timezone.utc).date())

    def locate(self, event: Dict) -> Tuple[str, str]:
        if not isinstance(event, dict):
            raise ValueError("Event must be a JSON object")

        if isinstance(event.get("Payload"), dict):
            event = event["Payload"]

        records = event.get("Records")
        if records:
            s3 = records[0]["s3"]
            # S3 notifications URL-encode keys ("=" -> "%3D", spaces -> "+").
            return s3["bucket"]["name"], urllib.parse.unquote_plus(s3["object"]["key"])

        detail = event.get("detail")
        if isinstance(detail, dict) and "object" in detail:
            return detail["bucket"]["name"], detail["object"]["key"]

        key = event.get("key")
        if key:
            return event.get("bucket") or self._default_bucket, key
        return self._default_bucket, self._latest_key(event.get("ingest_date") or f"{self._today():%Y-%m-%d}")

    def _latest_key(self, ingest_date: str) -> str:
        partition = f"{self._raw_prefix}/ingest_date={ingest_date}/"
        client = self._client or boto3.client("s3")
        latest = None
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=self._default_bucket, Prefix=partition):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and (latest is None or obj["LastModified"] > latest["LastModified"]):
                    latest = obj
        if latest is None:
            raise ValueError(
                f"Event has no raw object key and s3://{self._default_bucket}/{partition} has no .json "
                "files — run extract_airplane_ticket_rates first or pass {'bucket', 'key'}"
            )
        logger.info("No key in event; using newest raw object %s", latest["Key"])
        return latest["Key"]


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


class DealsTransformer:
    """
    Flattens the raw payload into one row per deal. All of search_metadata
    is kept and copied onto every row; from each deal only DEAL_FIELDS are
    kept. Columns get explicit types so every file has the same schema for
    Glue/Athena, no matter which optional fields a given response contains.
    """

    METADATA_PREFIX = "search_metadata_"
    NO_DESCRIPTION = "NO DESCRIPTION"

    # Known search_metadata fields and their types. Any other field SerpApi
    # adds later is still kept, as a string column (see _metadata_columns).
    METADATA_FIELDS = {
        "id": pa.string(),
        "status": pa.string(),
        "json_endpoint": pa.string(),
        "markdown_endpoint": pa.string(),
        "created_at": pa.timestamp("s", tz="UTC"),
        "processed_at": pa.timestamp("s", tz="UTC"),
        "google_flights_deals_url": pa.string(),
        "raw_html_file": pa.string(),
        "prettify_html_file": pa.string(),
        "total_time_taken": pa.float64(),
    }

    DEAL_FIELDS = {
        "name": pa.string(),
        "price": pa.int64(),
        # Plain "YYYY-MM-DD" strings: date32 columns render as
        # "2026-10-15T00:00:00.000Z" in many Parquet viewers.
        "outbound_date": pa.string(),
        "return_date": pa.string(),
        "flight_duration": pa.int64(),  # minutes
        "description": pa.string(),
    }

    def transform(self, payload: Dict) -> pa.Table:
        metadata = payload.get("search_metadata")
        if not isinstance(metadata, dict) or not metadata:
            raise InvalidRawPayloadError("Raw payload has no search_metadata")
        deals = payload.get("deals")
        if not isinstance(deals, list):
            raise InvalidRawPayloadError("Raw payload has no deals array")

        metadata_columns = self._metadata_columns(metadata)
        rows = [self._clean_deal(deal) for deal in deals]

        columns: Dict[str, pa.Array] = {}
        fields: List[pa.Field] = []
        for name, (value, arrow_type) in metadata_columns.items():
            columns[name] = pa.array([value] * len(rows), type=arrow_type)
            fields.append(pa.field(name, arrow_type))
        for name, arrow_type in self.DEAL_FIELDS.items():
            columns[name] = pa.array([row[name] for row in rows], type=arrow_type)
            fields.append(pa.field(name, arrow_type))

        return pa.Table.from_pydict(columns, schema=pa.schema(fields))

    def _metadata_columns(self, metadata: Dict) -> Dict[str, Tuple[object, pa.DataType]]:
        columns = {}
        for field_name, arrow_type in self.METADATA_FIELDS.items():
            value = metadata.get(field_name)
            if pa.types.is_timestamp(arrow_type):
                value = self._parse_timestamp(value)
            columns[self.METADATA_PREFIX + field_name] = (value, arrow_type)
        for field_name, value in metadata.items():
            if field_name not in self.METADATA_FIELDS:
                logger.warning("Unexpected search_metadata field %r kept as string", field_name)
                text = value if isinstance(value, str) or value is None else json.dumps(value)
                columns[self.METADATA_PREFIX + field_name] = (text, pa.string())
        return columns

    def _clean_deal(self, deal: Dict) -> Dict[str, object]:
        description = (deal.get("description") or "").strip()
        return {
            "name": deal.get("name"),
            "price": deal.get("price"),
            "outbound_date": self._parse_date(deal.get("outbound_date")),
            "return_date": self._parse_date(deal.get("return_date")),
            "flight_duration": deal.get("flight_duration"),
            "description": description or self.NO_DESCRIPTION,
        }

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[str]:
        # Validate and keep only the YYYY-MM-DD part, dropping any time / "Z" suffix.
        return date.fromisoformat(value[:10]).isoformat() if value else None

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        # SerpApi format: "2026-10-07 00:47:06 UTC"
        if not value:
            return None
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# S3 write
# ---------------------------------------------------------------------------
class TransformedKeyBuilder:
    """
    Mirrors the raw key under the transformed prefix and swaps .json for
    .parquet, so the ingest_date= partition and file name carry over:
        airplane_ticket_rates/ingest_date=2026-10-07/deals_PHX_....json
     -> airplane_ticket_rates/ingest_date=2026-10-07/deals_PHX_....parquet
    """

    def __init__(self, raw_prefix: str, transformed_prefix: str):
        self._raw_prefix = raw_prefix.rstrip("/") + "/"
        self._transformed_prefix = transformed_prefix.rstrip("/") + "/"

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
class TransformService:
    """Coordinates locate -> read -> transform -> name -> store."""

    def __init__(
        self,
        locator: RawObjectLocator,
        reader: S3RawReader,
        transformer: DealsTransformer,
        key_builder: TransformedKeyBuilder,
        writer: S3ParquetWriter,
    ):
        self._locator = locator
        self._reader = reader
        self._transformer = transformer
        self._key_builder = key_builder
        self._writer = writer

    def run(self, event: Dict) -> Dict[str, object]:
        raw_bucket, raw_key = self._locator.locate(event)
        payload = self._reader.read(raw_bucket, raw_key)
        table = self._transformer.transform(payload)
        key = self._writer.write(self._key_builder.build(raw_key), table)

        logger.info("Wrote %d deals to s3://%s/%s", table.num_rows, self._writer.bucket, key)
        return {
            "status": "SUCCEEDED",
            "source_bucket": raw_bucket,
            "source_key": raw_key,
            "bucket": self._writer.bucket,
            "key": key,
            "search_id": payload["search_metadata"].get("id"),
            "row_count": table.num_rows,
        }


def build_service(config: TransformConfig) -> TransformService:
    """Composition root: the only place that wires the pieces together."""
    return TransformService(
        locator=RawObjectLocator(config.aws_airplane_ticket_raw_data_s3_bucket, config.raw_prefix),
        reader=S3RawReader(),
        transformer=DealsTransformer(),
        key_builder=TransformedKeyBuilder(config.raw_prefix, config.transformed_prefix),
        writer=S3ParquetWriter(config.aws_airplane_ticket_transformed_data_s3_bucket),
    )


_service: Optional[TransformService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(TransformConfig.from_env())
    return _service.run(event)

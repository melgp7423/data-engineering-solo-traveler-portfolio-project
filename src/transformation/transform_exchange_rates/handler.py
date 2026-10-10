"""
Lambda: read every raw freecurrencyapi JSON file in one ingest_date partition
from S3, turn the rates into columns, and write each one back to S3 as
Parquet — OOP / Single Responsibility Principle.

Input  (raw bucket):         <RAW_PREFIX>ingest_date=YYYY-MM-DD/rates_USD_2026-10-10_223747_4dc31908.json
Output (transformed bucket): <TRANSFORMED_PREFIX>ingest_date=YYYY-MM-DD/rates_USD_2026-10-10_223747_4dc31908.parquet

Raw payload:
    {"data": {"CAD": 1.4257001775, "MXN": 18.3700024185, "USD": 1}}

Output layout: one row per currency (columnar). The API response carries no
date or base currency, so both are parsed from the file name
(rates_<BASE>_<YYYY-MM-DD>_...):

    rate_date (date), base_currency, currency, rate

Classes and their one job:
    TransformConfig          -> read & validate settings
    RawObjectLocator         -> work out which raw S3 objects to transform from the event
    S3RawReader              -> read the raw JSON from S3
    RawKeyParser             -> raw key -> (base currency, rate date)
    ExchangeRatesTransformer -> raw payload + key -> pyarrow Table
    TransformedKeyBuilder    -> name the Parquet object
    S3ParquetWriter          -> serialize the Table to Parquet and write it to S3
    TransformService         -> orchestrate locate -> (read -> transform -> name -> store) per file
    lambda_handler           -> Lambda entry point

Environment variables (placeholders shown):
    AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET           = <YOUR_RAW_BUCKET_NAME>
    AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET   = <YOUR_TRANSFORMED_BUCKET_NAME>
    RAW_PREFIX                                      = ""   folder above ingest_date= in the raw bucket ("" = bucket root)
    TRANSFORMED_PREFIX                              = ""   folder above ingest_date= in the transformed bucket

Supported events:
    Step Functions (output of the extract Lambda):  {"bucket": "...", "key": "...", ...} -> that file
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
import re
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

    aws_exchange_rates_raw_data_s3_bucket: str
    aws_exchange_rates_transformed_data_s3_bucket: str
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
            aws_exchange_rates_raw_data_s3_bucket=cls._bucket_from_env("AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET"),
            aws_exchange_rates_transformed_data_s3_bucket=cls._bucket_from_env(
                "AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET"
            ),
            raw_prefix=os.environ.get("RAW_PREFIX", ""),
            transformed_prefix=os.environ.get("TRANSFORMED_PREFIX", ""),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required = {
            "AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET": self.aws_exchange_rates_raw_data_s3_bucket,
            "AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET": self.aws_exchange_rates_transformed_data_s3_bucket,
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
      - a single file: {"bucket", "key"} (the extract Lambda returns both)
      - otherwise every .json under <raw_prefix>ingest_date=<date>/, where
        <date> is the event's "ingest_date" or today in UTC
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
                "run extract_exchange_rates first or pass {'ingest_date'} / {'bucket', 'key'}"
            )
        logger.info("Found %d raw exchange rate files in s3://%s/%s", len(keys), bucket, partition)
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
    """Raised when the raw JSON or its file name is missing what we need."""


class RawKeyParser:
    """
    Pulls the base currency and rate date out of the raw file name:
        .../rates_USD_2026-10-10_223747_4dc31908.json -> ("USD", date(2026, 10, 10))
    """

    PATTERN = re.compile(r"^rates_(?P<base>[A-Z]{3})_(?P<date>\d{4}-\d{2}-\d{2})_")

    def parse(self, raw_key: str) -> Tuple[str, date]:
        file_name = raw_key.rsplit("/", 1)[-1]
        match = self.PATTERN.match(file_name)
        if not match:
            raise InvalidRawPayloadError(
                f"File name {file_name!r} does not match rates_<BASE>_<YYYY-MM-DD>_..."
            )
        return match["base"], date.fromisoformat(match["date"])


class ExchangeRatesTransformer:
    """
    Turns {"data": {"CAD": 1.42, ...}} into one row per currency, with the
    rate date and base currency from the file name on every row. Columns get
    explicit types so every file has the same schema for Glue/Athena.
    """

    SCHEMA = pa.schema([
        pa.field("rate_date", pa.date32()),
        pa.field("base_currency", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("rate", pa.float64()),  # units of currency per 1 base_currency
    ])

    def __init__(self, key_parser: RawKeyParser = None):
        self._key_parser = key_parser or RawKeyParser()

    def transform(self, payload: Dict, raw_key: str) -> pa.Table:
        rates = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rates, dict) or not rates:
            raise InvalidRawPayloadError("Raw payload has no data object")
        bad = [c for c, r in rates.items() if isinstance(r, bool) or not isinstance(r, (int, float))]
        if bad:
            raise InvalidRawPayloadError(f"Non-numeric rates for: {', '.join(bad)}")

        base_currency, rate_date = self._key_parser.parse(raw_key)
        currencies = sorted(rates)
        return pa.Table.from_pydict(
            {
                "rate_date": [rate_date] * len(currencies),
                "base_currency": [base_currency] * len(currencies),
                "currency": currencies,
                "rate": [float(rates[c]) for c in currencies],
            },
            schema=self.SCHEMA,
        )


# ---------------------------------------------------------------------------
# S3 write
# ---------------------------------------------------------------------------
class TransformedKeyBuilder:
    """
    Mirrors the raw key under the transformed prefix and swaps .json for
    .parquet, so the ingest_date= partition and file name carry over:
        ingest_date=2026-10-10/rates_USD_2026-10-10_223747_4dc31908.json
     -> ingest_date=2026-10-10/rates_USD_2026-10-10_223747_4dc31908.parquet
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
        transformer: ExchangeRatesTransformer,
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
                table = self._transformer.transform(payload, raw_key)
            except (InvalidRawPayloadError, ValueError) as err:
                # One bad file shouldn't throw away the others.
                # (json.JSONDecodeError and pyarrow.ArrowInvalid are ValueErrors.)
                logger.error("Could not transform s3://%s/%s: %s", raw_bucket, raw_key, err)
                failed.append({"source_key": raw_key, "error": str(err)})
                continue
            key = self._writer.write(self._key_builder.build(raw_key), table)
            logger.info("Wrote %d rates to s3://%s/%s", table.num_rows, self._writer.bucket, key)
            written.append({
                "source_key": raw_key,
                "key": key,
                "rate_date": f"{table.column('rate_date')[0].as_py():%Y-%m-%d}",
                "row_count": table.num_rows,
            })

        if not written:
            raise TransformError(f"All {len(failed)} raw exchange rate files failed to transform: {failed}")

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
        locator=RawObjectLocator(config.aws_exchange_rates_raw_data_s3_bucket, config.raw_prefix),
        reader=S3RawReader(),
        transformer=ExchangeRatesTransformer(),
        key_builder=TransformedKeyBuilder(config.raw_prefix, config.transformed_prefix),
        writer=S3ParquetWriter(config.aws_exchange_rates_transformed_data_s3_bucket),
    )


_service: Optional[TransformService] = None


def lambda_handler(event, context):
    global _service
    if _service is None:
        _service = build_service(TransformConfig.from_env())
    return _service.run(event or {})

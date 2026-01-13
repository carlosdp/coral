from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

from google.cloud import storage


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without = uri[len("gs://") :]
    bucket, blob = without.split("/", 1)
    return bucket, blob


def write_bytes(uri: str, payload: bytes) -> None:
    if uri.startswith("gs://"):
        bucket_name, blob_name = _parse_gcs_uri(uri)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(payload)
        return
    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def read_bytes(uri: str) -> bytes:
    if uri.startswith("gs://"):
        bucket_name, blob_name = _parse_gcs_uri(uri)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()
    return Path(uri).read_bytes()

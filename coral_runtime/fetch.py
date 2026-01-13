from __future__ import annotations

import io
import tarfile
from pathlib import Path

import requests
from google.cloud import storage


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without = uri[len("gs://") :]
    bucket, blob = without.split("/", 1)
    return bucket, blob


def _download_gcs(uri: str) -> bytes:
    bucket_name, blob_name = _parse_gcs_uri(uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()


def _download_http(uri: str) -> bytes:
    resp = requests.get(uri, timeout=120)
    resp.raise_for_status()
    return resp.content


def fetch_bundle(uri: str, dest: Path) -> None:
    if uri.startswith("gs://"):
        payload = _download_gcs(uri)
    else:
        payload = _download_http(uri)
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        tar.extractall(dest)

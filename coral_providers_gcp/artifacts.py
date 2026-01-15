from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from google.cloud import storage

from coral.providers.base import BundleRef


@dataclass
class GCSArtifactStore:
    project: str
    bucket: str

    def _client(self) -> storage.Client:
        return storage.Client(project=self.project)

    def bundle_uri(self, bundle_hash: str) -> str:
        return f"gs://{self.bucket}/coral/bundles/{bundle_hash}.tar.gz"

    def result_uri(self, call_id: str) -> str:
        return f"gs://{self.bucket}/coral/results/{call_id}.bin"

    def put_bundle(self, bundle_path: str, bundle_hash: str) -> BundleRef:
        uri = self.bundle_uri(bundle_hash)
        client = self._client()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(f"coral/bundles/{bundle_hash}.tar.gz")
        if not blob.exists():
            blob.upload_from_filename(bundle_path)
        return BundleRef(uri=uri, hash=bundle_hash)

    def get_result(self, result_ref: str) -> bytes:
        client = self._client()
        if not result_ref.startswith("gs://"):
            raise ValueError("Expected GCS URI")
        _, path = result_ref.split("gs://", 1)
        bucket_name, blob_name = path.split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()

    def signed_url(self, uri: str, ttl_seconds: int, method: str = "GET") -> Optional[str]:
        if not uri.startswith("gs://"):
            return None
        _, path = uri.split("gs://", 1)
        bucket_name, blob_name = path.split("/", 1)
        client = self._client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.generate_signed_url(expiration=ttl_seconds, method=method)

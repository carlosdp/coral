from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import google.auth
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request

from google.cloud import storage

from coral.providers.base import BundleRef


@dataclass
class GCSArtifactStore:
    project: str
    bucket: str
    signer_service_account: str | None = None

    def _client(self) -> storage.Client:
        return storage.Client(project=self.project)

    def _signing_credentials(self) -> Optional[Credentials]:
        try:
            source_credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        except DefaultCredentialsError:
            return None
        if hasattr(source_credentials, "sign_bytes") and hasattr(
            source_credentials, "signer_email"
        ):
            return source_credentials
        if not self.signer_service_account:
            return None
        if not source_credentials.valid:
            source_credentials.refresh(Request())
        return impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=self.signer_service_account,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=3600,
        )

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
        signing_credentials = self._signing_credentials()
        try:
            return blob.generate_signed_url(
                expiration=ttl_seconds,
                method=method,
                credentials=signing_credentials,
            )
        except AttributeError as exc:
            message = str(exc)
            if "need a private key" in message:
                return None
            raise

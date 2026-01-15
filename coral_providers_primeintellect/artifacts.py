from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from coral.providers.base import BundleRef
from coral_providers_gcp.artifacts import GCSArtifactStore


@dataclass
class PrimeArtifactStore:
    gcs: GCSArtifactStore
    signed_ttl_seconds: int = 3600

    def __post_init__(self):
        self._result_map: Dict[str, str] = {}

    def put_bundle(self, bundle_path: str, bundle_hash: str) -> BundleRef:
        ref = self.gcs.put_bundle(bundle_path, bundle_hash)
        signed = self.gcs.signed_url(ref.uri, self.signed_ttl_seconds, method="GET")
        return BundleRef(uri=signed or ref.uri, hash=bundle_hash)

    def result_uri(self, call_id: str) -> str:
        gs_uri = self.gcs.result_uri(call_id)
        signed = self.gcs.signed_url(gs_uri, self.signed_ttl_seconds, method="PUT")
        if signed:
            self._result_map[signed] = gs_uri
            return signed
        return gs_uri

    def get_result(self, result_ref: str) -> bytes:
        gs_uri = self._result_map.get(result_ref, result_ref)
        return self.gcs.get_result(gs_uri)

    def signed_url(self, uri: str, ttl_seconds: int, method: str = "GET") -> Optional[str]:
        return self.gcs.signed_url(uri, ttl_seconds, method=method)

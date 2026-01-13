from __future__ import annotations

from dataclasses import dataclass

from google.cloud import batch_v1

from coral.providers.base import RunHandle


@dataclass
class GCPCleanupManager:
    project: str
    region: str

    def _client(self) -> batch_v1.BatchServiceClient:
        return batch_v1.BatchServiceClient()

    def _job_parent(self) -> str:
        return f"projects/{self.project}/locations/{self.region}"

    def cleanup(self, handle: RunHandle, detached: bool) -> None:
        if detached:
            return
        name = f"{self._job_parent()}/jobs/{handle.provider_ref}"
        self._client().delete_job(name=name)

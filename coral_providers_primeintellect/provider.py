from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from coral.config import Profile
from coral.errors import ConfigError
from coral.providers.base import Provider
from coral_providers_gcp.artifacts import GCSArtifactStore
from coral_providers_gcp.build import CloudBuildImageBuilder
from coral_providers_primeintellect.api import PrimeClient
from coral_providers_primeintellect.artifacts import PrimeArtifactStore
from coral_providers_primeintellect.execute import PrimeExecutor
from coral_providers_primeintellect.logs import PrimeLogStreamer


@dataclass
class PrimeConfig:
    api_key: str
    team_id: Optional[str]
    gcp_project: str
    gcp_region: str
    artifact_repo: str
    gcs_bucket: str
    gpu_type: str
    gpu_count: int
    regions: list[str]
    provider_type: Optional[str]
    registry_credentials_id: Optional[str]


class PrimeIntellectProvider(Provider):
    name = "prime"

    def __init__(self):
        self.config: PrimeConfig | None = None
        self._status_cb = None
        self._artifacts = None

    def set_status_callback(self, cb):
        self._status_cb = cb

    def configure(self, profile: Profile) -> None:
        data = profile.data
        missing = [
            key
            for key in [
                "api_key",
                "gcp_project",
                "gcp_region",
                "artifact_repo",
                "gcs_bucket",
            ]
            if key not in data
        ]
        if missing:
            raise ConfigError(f"Missing Prime Intellect config keys: {', '.join(missing)}")
        self.config = PrimeConfig(
            api_key=data["api_key"],
            team_id=data.get("team_id"),
            gcp_project=data["gcp_project"],
            gcp_region=data["gcp_region"],
            artifact_repo=data["artifact_repo"],
            gcs_bucket=data["gcs_bucket"],
            gpu_type=data.get("gpu_type", "CPU_NODE"),
            gpu_count=int(data.get("gpu_count", 1)),
            regions=data.get("regions", ["united_states"]),
            provider_type=data.get("provider_type", "primeintellect"),
            registry_credentials_id=data.get("registry_credentials_id"),
        )

    def _ensure_config(self) -> PrimeConfig:
        if not self.config:
            raise ConfigError("Prime provider not configured. Set profile.prime values.")
        return self.config

    def get_builder(self):
        cfg = self._ensure_config()
        return CloudBuildImageBuilder(
            project=cfg.gcp_project,
            region=cfg.gcp_region,
            artifact_repo=cfg.artifact_repo,
            gcs_bucket=cfg.gcs_bucket,
        )

    def get_artifacts(self):
        cfg = self._ensure_config()
        if self._artifacts is None:
            gcs = GCSArtifactStore(project=cfg.gcp_project, bucket=cfg.gcs_bucket)
            self._artifacts = PrimeArtifactStore(gcs=gcs)
        return self._artifacts

    def get_executor(self):
        cfg = self._ensure_config()
        client = PrimeClient(api_key=cfg.api_key, team_id=cfg.team_id)
        artifacts = self.get_artifacts()
        return PrimeExecutor(
            client=client,
            project=cfg.gcp_project,
            artifact_store=artifacts,
            gpu_type=cfg.gpu_type,
            gpu_count=cfg.gpu_count,
            regions=cfg.regions,
            provider_type=cfg.provider_type,
            registry_credentials_id=cfg.registry_credentials_id,
            status_cb=self._status_cb,
        )

    def get_log_streamer(self):
        cfg = self._ensure_config()
        client = PrimeClient(api_key=cfg.api_key, team_id=cfg.team_id)
        return PrimeLogStreamer(client=client)

    def get_cleanup(self):
        return self

    def cleanup(self, handle, detached: bool) -> None:
        if detached:
            return
        self.get_executor().cancel(handle)

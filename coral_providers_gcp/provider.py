from __future__ import annotations

import os
from dataclasses import dataclass

from coral.config import Profile
from coral.errors import ConfigError
from coral.providers.base import Provider
from coral_providers_gcp.artifacts import GCSArtifactStore
from coral_providers_gcp.build import CloudBuildImageBuilder
from coral_providers_gcp.cleanup import GCPCleanupManager
from coral_providers_gcp.execute import BatchExecutor, GKEExecutor
from coral_providers_gcp.logs import GCPLogStreamer


@dataclass
class GCPConfig:
    project: str
    region: str
    artifact_repo: str
    gcs_bucket: str
    execution: str
    service_account: str | None = None
    machine_type: str | None = None


class GCPProvider(Provider):
    name = "gcp"

    def __init__(self):
        self.config: GCPConfig | None = None

    def configure(self, profile: Profile) -> None:
        data = profile.data
        credentials_path = data.get("credentials_path")
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(
                credentials_path
            )
        missing = [k for k in ["project", "region", "artifact_repo", "gcs_bucket"] if k not in data]
        if missing:
            raise ConfigError(f"Missing GCP config keys: {', '.join(missing)}")
        self.config = GCPConfig(
            project=data["project"],
            region=data["region"],
            artifact_repo=data["artifact_repo"],
            gcs_bucket=data["gcs_bucket"],
            execution=data.get("execution", "batch"),
            service_account=data.get("service_account"),
            machine_type=data.get("machine_type"),
        )

    def _ensure_config(self) -> GCPConfig:
        if not self.config:
            raise ConfigError("GCP provider not configured. Run with --profile or set config.")
        return self.config

    def get_builder(self):
        cfg = self._ensure_config()
        return CloudBuildImageBuilder(
            project=cfg.project,
            region=cfg.region,
            artifact_repo=cfg.artifact_repo,
            gcs_bucket=cfg.gcs_bucket,
        )

    def get_artifacts(self):
        cfg = self._ensure_config()
        return GCSArtifactStore(project=cfg.project, bucket=cfg.gcs_bucket)

    def get_executor(self):
        cfg = self._ensure_config()
        artifacts = self.get_artifacts()
        if cfg.execution == "gke":
            return GKEExecutor(project=cfg.project, region=cfg.region)
        return BatchExecutor(
            project=cfg.project,
            region=cfg.region,
            artifact_store=artifacts,
            machine_type=cfg.machine_type,
            service_account=cfg.service_account,
        )

    def get_log_streamer(self):
        cfg = self._ensure_config()
        return GCPLogStreamer(project=cfg.project)

    def get_cleanup(self):
        cfg = self._ensure_config()
        return GCPCleanupManager(project=cfg.project, region=cfg.region)

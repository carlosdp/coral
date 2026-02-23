from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from coral.config import Profile
from coral.errors import ConfigError
from coral.providers.base import Provider
from coral_providers_gcp.build import DockerHubImageBuilder
from coral_providers_primeintellect.api import PrimeClient
from coral_providers_primeintellect.artifacts import PrimeArtifactStore
from coral_providers_primeintellect.execute import PrimeExecutor
from coral_providers_primeintellect.logs import PrimeLogStreamer


@dataclass
class PrimeConfig:
    api_key: str
    team_id: Optional[str]
    gcp_project: Optional[str]
    gcp_region: Optional[str]
    artifact_repo: Optional[str]
    gcs_bucket: Optional[str]
    service_account: Optional[str]
    credentials_path: Optional[str]
    regions: list[str]
    provider_type: Optional[str]
    registry_credentials_id: Optional[str]
    custom_template_id: Optional[str]
    docker_repository: Optional[str]


class PrimeIntellectProvider(Provider):
    name = "prime"

    def __init__(self):
        self.config: PrimeConfig | None = None
        self._status_cb = None
        self._artifacts = None
        self._executor = None

    def set_status_callback(self, cb):
        self._status_cb = cb

    @staticmethod
    def _optional_value(value: object) -> Optional[str]:
        if value is None:
            return None
        value_str = str(value).strip()
        if not value_str or value_str.lower() == "none":
            return None
        return value_str

    def configure(self, profile: Profile) -> None:
        data = profile.data
        credentials_path = self._optional_value(data.get("credentials_path"))
        missing = [key for key in ["api_key"] if key not in data]
        if missing:
            raise ConfigError(f"Missing Prime Intellect config keys: {', '.join(missing)}")
        self.config = PrimeConfig(
            api_key=data["api_key"],
            team_id=self._optional_value(data.get("team_id")),
            gcp_project=self._optional_value(data.get("gcp_project")),
            gcp_region=self._optional_value(data.get("gcp_region")),
            artifact_repo=self._optional_value(data.get("artifact_repo")),
            gcs_bucket=self._optional_value(data.get("gcs_bucket")),
            service_account=self._optional_value(data.get("service_account")),
            credentials_path=credentials_path,
            regions=data.get("regions", ["united_states"]),
            provider_type=self._optional_value(data.get("provider_type")),
            registry_credentials_id=self._optional_value(data.get("registry_credentials_id")),
            custom_template_id=self._optional_value(data.get("custom_template_id")),
            docker_repository=self._optional_value(data.get("docker_repository")),
        )
        self._artifacts = None
        self._executor = None

    def _ensure_config(self) -> PrimeConfig:
        if not self.config:
            raise ConfigError("Prime provider not configured. Set profile.prime values.")
        return self.config

    def get_builder(self):
        cfg = self._ensure_config()
        return DockerHubImageBuilder(repository=cfg.docker_repository or "train")

    def get_artifacts(self):
        if self._artifacts is None:
            self._artifacts = PrimeArtifactStore()
        return self._artifacts

    def get_executor(self):
        cfg = self._ensure_config()
        if self._executor is None:
            client = PrimeClient(api_key=cfg.api_key, team_id=cfg.team_id)
            artifacts = self.get_artifacts()
            self._executor = PrimeExecutor(
                client=client,
                project=cfg.gcp_project or "",
                artifact_store=artifacts,
                gpu_type="CPU_NODE",
                gpu_count=1,
                regions=cfg.regions,
                provider_type=cfg.provider_type,
                registry_credentials_id=cfg.registry_credentials_id,
                custom_template_id=cfg.custom_template_id,
                status_cb=self._status_cb,
            )
        else:
            self._executor.status_cb = self._status_cb
        return self._executor

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

    def ensure_custom_template(self, image) -> str:
        cfg = self._ensure_config()
        executor = PrimeExecutor(
            client=PrimeClient(api_key=cfg.api_key, team_id=cfg.team_id),
            project=cfg.gcp_project or "",
            artifact_store=object(),
            gpu_type="CPU_NODE",
            gpu_count=1,
            regions=cfg.regions,
            provider_type=cfg.provider_type,
            registry_credentials_id=cfg.registry_credentials_id,
            custom_template_id=cfg.custom_template_id,
            status_cb=self._status_cb,
        )
        return executor.ensure_custom_template(image)

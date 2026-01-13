from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - py<3.11
    import tomli as tomllib

from coral.errors import ConfigError

CONFIG_DIR = Path.home() / ".coral"
CONFIG_PATH = CONFIG_DIR / "config.toml"


@dataclass
class Profile:
    name: str
    provider: str
    data: Dict[str, Any]


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    return tomllib.loads(CONFIG_PATH.read_text())


def save_config(data: Dict[str, Any]) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Coral configuration",
        "",
        "[profile.default]",
        "provider = \"gcp\"",
        "",
        "[profile.default.gcp]",
        "project = \"my-gcp-project\"",
        "region = \"us-central1\"",
        "artifact_repo = \"coral\"",
        "gcs_bucket = \"coral-artifacts-myproj\"",
        "execution = \"batch\"",
        "service_account = \"coral-runner@myproj.iam.gserviceaccount.com\"",
        "",
    ]
    CONFIG_PATH.write_text("\n".join(lines))


def get_profile(name: str | None) -> Profile:
    data = load_config()
    profiles = data.get("profile", {})
    profile_name = name or os.environ.get("CORAL_PROFILE", "default")
    profile = profiles.get(profile_name)
    if profile is None:
        raise ConfigError(f"Profile '{profile_name}' not found in {CONFIG_PATH}")
    provider = profile.get("provider")
    if not provider:
        raise ConfigError(f"Profile '{profile_name}' missing provider")
    provider_data = profile.get(provider, {})
    return Profile(name=profile_name, provider=provider, data=provider_data)

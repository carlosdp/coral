from __future__ import annotations

from importlib.metadata import entry_points
from typing import Dict

from coral.errors import ProviderError
from coral.providers.base import Provider


def available_providers() -> Dict[str, Provider]:
    providers: Dict[str, Provider] = {}
    for ep in entry_points(group="coral.providers"):
        provider_cls = ep.load()
        provider: Provider = provider_cls()
        providers[provider.name] = provider
    return providers


def load(name: str) -> Provider:
    for ep in entry_points(group="coral.providers"):
        if ep.name == name:
            provider_cls = ep.load()
            return provider_cls()
    raise ProviderError(f"Provider '{name}' not found")

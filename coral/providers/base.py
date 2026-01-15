from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Protocol

from coral.spec import CallSpec, ImageSpec, ResourceSpec


@dataclass(frozen=True)
class ImageRef:
    uri: str
    digest: str
    metadata: Dict[str, str]


@dataclass(frozen=True)
class BundleRef:
    uri: str
    hash: str


@dataclass(frozen=True)
class RunHandle:
    run_id: str
    call_id: str
    provider_ref: str


@dataclass(frozen=True)
class RunResult:
    call_id: str
    success: bool
    output: bytes


class ImageBuilder(Protocol):
    def resolve_image(self, spec: ImageSpec, copy_sources: Iterable[str] | None = None) -> ImageRef:
        ...


class ArtifactStore(Protocol):
    def put_bundle(self, bundle_path: str, bundle_hash: str) -> BundleRef:
        ...

    def get_result(self, result_ref: str) -> bytes:
        ...

    def result_uri(self, call_id: str) -> str:
        ...

    def signed_url(self, uri: str, ttl_seconds: int, method: str = "GET") -> str | None:
        ...


class Executor(Protocol):
    def submit(
        self,
        call_spec: CallSpec,
        image: ImageRef,
        bundle: BundleRef,
        resources: ResourceSpec,
        env: Dict[str, str],
        labels: Dict[str, str],
    ) -> RunHandle:
        ...

    def wait(self, handle: RunHandle) -> RunResult:
        ...

    def cancel(self, handle: RunHandle) -> None:
        ...


class LogStreamer(Protocol):
    def stream(self, handle: RunHandle) -> Iterable[str]:
        ...


class CleanupManager(Protocol):
    def cleanup(self, handle: RunHandle, detached: bool) -> None:
        ...


class Provider(Protocol):
    name: str

    def get_builder(self) -> ImageBuilder:
        ...

    def get_artifacts(self) -> ArtifactStore:
        ...

    def get_executor(self) -> Executor:
        ...

    def get_log_streamer(self) -> LogStreamer:
        ...

    def get_cleanup(self) -> CleanupManager:
        ...

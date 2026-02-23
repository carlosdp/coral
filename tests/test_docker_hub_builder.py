from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import coral
from coral.entrypoint import RunSession
from coral.image import build_plan_hash
from coral.providers.base import ImageRef
from coral.spec import ImageSpec
from coral_providers_gcp.build import DockerHubImageBuilder


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict | None = None,
        *,
        reason: str = "OK",
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.reason = reason
        self.text = text

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        return self._payload


def _completed(
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=["docker"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _is_docker_cmd(cmd: list[str], *parts: str) -> bool:
    return (
        len(cmd) > len(parts)
        and Path(cmd[0]).name == "docker"
        and cmd[1 : 1 + len(parts)] == list(parts)
    )


def test_docker_hub_builder_requires_docker_login(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = DockerHubImageBuilder()
    spec = ImageSpec(base_image="python:3.11-slim")

    def fake_run(cmd, **kwargs):
        if _is_docker_cmd(cmd, "info", "--format"):
            return _completed(stdout='{"Username": ""}')
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("coral_providers_gcp.build.subprocess.run", fake_run)
    monkeypatch.setattr(builder, "_docker_config_username", lambda: "")

    with pytest.raises(RuntimeError, match="docker login"):
        builder.resolve_image(spec)


def test_docker_hub_builder_reuses_existing_public_image(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = DockerHubImageBuilder()
    spec = ImageSpec(base_image="python:3.11-slim")
    image_hash = build_plan_hash(spec)

    def fake_run(cmd, **kwargs):
        if _is_docker_cmd(cmd, "info", "--format"):
            return _completed(stdout='{"Username": "alice"}')
        raise AssertionError(f"Unexpected command: {cmd}")

    def fake_get(url: str, timeout: int):
        expected = f"/namespaces/alice/repositories/coral/tags/{image_hash}"
        assert url.endswith(expected)
        return _FakeResponse(
            200,
            payload={"images": [{"digest": "sha256:remote"}]},
        )

    monkeypatch.setattr("coral_providers_gcp.build.subprocess.run", fake_run)
    monkeypatch.setattr("coral_providers_gcp.build.requests.get", fake_get)

    image_ref = builder.resolve_image(spec)
    assert image_ref.uri == f"docker.io/alice/coral:{image_hash}"
    assert image_ref.digest == "sha256:remote"
    assert image_ref.metadata["hash"] == image_hash
    assert image_ref.metadata["docker_user"] == "alice"


def test_docker_hub_builder_builds_and_pushes_when_tag_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = DockerHubImageBuilder()
    spec = ImageSpec(base_image="python:3.11-slim")
    image_hash = build_plan_hash(spec)
    commands: list[list[str]] = []
    tag_checks = {"count": 0}

    def fake_run(cmd, **kwargs):
        commands.append(list(cmd))
        if _is_docker_cmd(cmd, "info", "--format"):
            return _completed(stdout='{"Username": "alice"}')
        if _is_docker_cmd(cmd, "build", "-t"):
            return _completed()
        if _is_docker_cmd(cmd, "push"):
            return _completed()
        if _is_docker_cmd(cmd, "image", "inspect"):
            return _completed(stdout="docker.io/alice/coral@sha256:local")
        raise AssertionError(f"Unexpected command: {cmd}")

    def fake_get(url: str, timeout: int):
        tag_checks["count"] += 1
        if tag_checks["count"] == 1:
            return _FakeResponse(404, reason="Not Found")
        return _FakeResponse(
            200,
            payload={"images": [{"digest": "sha256:remote"}]},
        )

    monkeypatch.setattr("coral_providers_gcp.build.subprocess.run", fake_run)
    monkeypatch.setattr("coral_providers_gcp.build.requests.get", fake_get)
    monkeypatch.setattr("coral_providers_gcp.build.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        builder,
        "_stage_context",
        lambda plan, copy_sources: Path("/tmp/coral-fake-build-context"),
    )

    image_ref = builder.resolve_image(spec)
    assert image_ref.uri == f"docker.io/alice/coral:{image_hash}"
    assert image_ref.digest == "sha256:remote"
    assert any(
        _is_docker_cmd(command, "build", "-t") and command[3:] == [image_ref.uri, "."]
        for command in commands
    )
    assert any(
        _is_docker_cmd(command, "push") and command[2:] == [image_ref.uri]
        for command in commands
    )


def test_default_images_do_not_copy_project_sources() -> None:
    class FakeBuilder:
        def __init__(self) -> None:
            self.copy_sources: list[str] | None = None

        def resolve_image(self, spec, copy_sources=None):
            self.copy_sources = list(copy_sources or [])
            return ImageRef(uri="docker.io/alice/coral:test", digest="", metadata={})

    class FakeProvider:
        name = "fake"

        def __init__(self) -> None:
            self.builder = FakeBuilder()

        def get_builder(self):
            return self.builder

    app = coral.App(name="no-copy-default")
    provider = FakeProvider()
    with RunSession(provider=provider, app=app, detached=True) as session:
        session.prepare_image()

    assert provider.builder.copy_sources == []

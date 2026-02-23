from __future__ import annotations

import subprocess
from dataclasses import dataclass

import pytest
import requests

import coral
from coral.entrypoint import RunSession
from coral.providers.base import BundleRef, ImageRef, RunHandle
from coral.spec import CallSpec, ResourceSpec
from coral_providers_primeintellect.api import PrimeClient
from coral_providers_primeintellect.execute import PrimeExecutor


def _executor(custom_template_id: str | None = None) -> PrimeExecutor:
    return PrimeExecutor(
        client=PrimeClient(api_key="test"),
        project="test",
        artifact_store=object(),
        regions=["united_states"],
        gpu_type="CPU_NODE",
        gpu_count=1,
        custom_template_id=custom_template_id,
    )


def test_prime_gpu_defaults_to_executor_values() -> None:
    gpu_type, gpu_count = _executor()._requested_gpu(ResourceSpec())
    assert gpu_type == "CPU_NODE"
    assert gpu_count == 1


def test_prime_gpu_parses_type_and_count_from_resource_spec() -> None:
    gpu_type, gpu_count = _executor()._requested_gpu(ResourceSpec(gpu="RTX4090_24GB:1"))
    assert gpu_type == "RTX4090_24GB"
    assert gpu_count == 1


def test_prime_gpu_rejects_invalid_count() -> None:
    with pytest.raises(RuntimeError, match="Invalid Prime GPU count"):
        _executor()._requested_gpu(ResourceSpec(gpu="RTX4090_24GB:not-a-number"))


def test_prime_custom_template_requires_configured_id() -> None:
    image = ImageRef(uri="docker.io/carlosdp/train:abc", digest="", metadata={})
    with pytest.raises(RuntimeError, match="custom_template_id is required"):
        _executor(custom_template_id=None).ensure_custom_template(image)


def test_prime_custom_template_uses_configured_id() -> None:
    image = ImageRef(uri="docker.io/carlosdp/train:abc", digest="", metadata={})
    template_id = _executor(custom_template_id="tmpl-123").ensure_custom_template(image)
    assert template_id == "tmpl-123"


def test_prime_sync_latest_template_image_tags_and_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = ImageRef(uri="docker.io/carlosdp/train:abc123", digest="", metadata={})
    executor = _executor(custom_template_id="tmpl-123")
    commands: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        commands.append(list(cmd))
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(executor, "_docker_cli", lambda: "docker")
    monkeypatch.setattr(executor, "_docker_has_image", lambda docker, ref: True)
    monkeypatch.setattr("coral_providers_primeintellect.execute.subprocess.run", fake_run)

    executor._sync_latest_template_image(image)

    assert commands == [
        ["docker", "tag", "docker.io/carlosdp/train:abc123", "docker.io/carlosdp/train:latest"],
        ["docker", "push", "docker.io/carlosdp/train:latest"],
    ]


def test_prime_encode_pod_env_vars_chunks_large_internal_values() -> None:
    executor = _executor(custom_template_id="tmpl-123")
    encoded = executor._encode_pod_env_vars(
        {
            "CORAL_CALLSPEC_B64": "x" * 2500,
            "CORAL_BUNDLE_B64": "y" * 1100,
            "CORAL_RESULT_STDOUT": "1",
        }
    )
    encoded_map = {item["key"]: item["value"] for item in encoded}
    assert encoded_map["CORAL_CALLSPEC_B64_CHUNKS"] == "3"
    assert encoded_map["CORAL_BUNDLE_B64_CHUNKS"] == "2"
    assert encoded_map["CORAL_RESULT_STDOUT"] == "1"
    assert len(encoded_map["CORAL_CALLSPEC_B64_0000"]) == 1000


class _FakePrimeClient:
    api_key = "test-key"
    team_id = None
    base_url = "https://api.primeintellect.ai"

    def __init__(self) -> None:
        self.create_payload: dict | None = None

    def availability_gpus(self, **_kwargs):
        return [
            {
                "status": "Available",
                "cloudId": "cloud-1",
                "gpuType": "CPU_NODE",
                "socket": "s",
                "gpuCount": 1,
                "dataCenter": "dc",
                "country": "us",
                "security": "public",
                "provider": "prime",
            }
        ]

    def create_pod(self, payload):
        self.create_payload = payload
        return {"id": "pod-123"}


def test_prime_image_build_submit_uses_custom_template_and_inline_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    client = _FakePrimeClient()
    executor = PrimeExecutor(
        client=client,  # type: ignore[arg-type]
        project="test",
        artifact_store=object(),
        regions=["united_states"],
        gpu_type="CPU_NODE",
        gpu_count=1,
        custom_template_id="tmpl-123",
    )
    monkeypatch.setattr(executor, "_ensure_ssh_key_id", lambda: "ssh-key-1")
    monkeypatch.setattr(executor, "_sync_latest_template_image", lambda _image: None)

    call_spec = CallSpec(
        call_id="call-123",
        module="example",
        qualname="process",
        args_b64="",
        kwargs_b64="",
        serialization="1",
        result_ref="",
        stdout_mode="stream",
        log_labels={},
    )
    image = ImageRef(uri="docker.io/carlosdp/train:abc123", digest="", metadata={})
    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(b"bundle-bytes")
    bundle = BundleRef(uri=str(bundle_path), hash="bundle-hash")

    handle = executor.submit(
        call_spec=call_spec,
        image=image,
        bundle=bundle,
        resources=ResourceSpec(),
        env={},
        labels={},
    )

    assert handle.provider_ref == "pod-123"
    assert executor._run_mode(call_spec.call_id) == "image_build_inline"
    assert client.create_payload is not None
    assert client.create_payload["pod"]["image"] == "custom_template"
    assert client.create_payload["pod"]["customTemplateId"] == "tmpl-123"
    assert "sshKeyId" not in client.create_payload["pod"]
    env_vars = {
        item["key"]: item["value"]
        for item in client.create_payload["pod"]["envVars"]
    }
    assert env_vars["CORAL_RESULT_STDOUT"] == "1"
    assert "CORAL_BUNDLE_B64" in env_vars


def test_prime_image_build_retries_when_provider_rejects_custom_template(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    class _RetryClient(_FakePrimeClient):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        def availability_gpus(self, **_kwargs):
            return [
                {
                    "status": "Available",
                    "cloudId": "cloud-1",
                    "gpuType": "A100_80GB",
                    "socket": "PCIe",
                    "gpuCount": 1,
                    "dataCenter": "dc1",
                    "country": "US",
                    "security": "secure_cloud",
                    "provider": "runpod",
                },
                {
                    "status": "Available",
                    "cloudId": "cloud-2",
                    "gpuType": "A100_80GB",
                    "socket": "PCIe",
                    "gpuCount": 1,
                    "dataCenter": "dc2",
                    "country": "US",
                    "security": "secure_cloud",
                    "provider": "crusoecloud",
                },
            ]

        def create_pod(self, payload):
            self.attempts += 1
            if self.attempts == 1:
                response = requests.Response()
                response.status_code = 400
                response._content = (
                    b'{"detail":"Provider Runpod is not supported for image CUSTOM_TEMPLATE."}'
                )
                raise requests.HTTPError("unsupported provider", response=response)
            self.create_payload = payload
            return {"id": "pod-123"}

    client = _RetryClient()
    executor = PrimeExecutor(
        client=client,  # type: ignore[arg-type]
        project="test",
        artifact_store=object(),
        regions=["united_states"],
        gpu_type="A100_80GB",
        gpu_count=1,
        custom_template_id="tmpl-123",
    )
    monkeypatch.setattr(executor, "_ensure_ssh_key_id", lambda: "ssh-key-1")
    monkeypatch.setattr(executor, "_sync_latest_template_image", lambda _image: None)

    call_spec = CallSpec(
        call_id="call-123",
        module="example",
        qualname="process",
        args_b64="",
        kwargs_b64="",
        serialization="1",
        result_ref="",
        stdout_mode="stream",
        log_labels={},
    )
    image = ImageRef(uri="docker.io/carlosdp/train:abc123", digest="", metadata={})
    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(b"bundle-bytes")
    bundle = BundleRef(uri=str(bundle_path), hash="bundle-hash")

    handle = executor.submit(
        call_spec=call_spec,
        image=image,
        bundle=bundle,
        resources=ResourceSpec(gpu="A100_80GB:1"),
        env={},
        labels={},
    )

    assert handle.provider_ref == "pod-123"
    assert client.attempts == 2
    assert client.create_payload is not None
    assert client.create_payload["provider"]["type"] == "crusoecloud"


@dataclass
class _TrackingArtifacts:
    put_bundle_calls: int = 0
    result_uri_calls: int = 0

    def put_bundle(self, bundle_path: str, bundle_hash: str) -> BundleRef:
        self.put_bundle_calls += 1
        return BundleRef(uri="artifact://bundle", hash=bundle_hash)

    def result_uri(self, call_id: str) -> str:
        self.result_uri_calls += 1
        return f"artifact://result/{call_id}"

    def get_result(self, result_ref: str) -> bytes:
        raise NotImplementedError

    def signed_url(self, uri: str, ttl_seconds: int, method: str = "GET") -> str | None:
        return uri


class _TrackingExecutor:
    def __init__(self) -> None:
        self.last_bundle_uri: str | None = None
        self.last_result_ref: str | None = None

    def submit(self, call_spec, image, bundle, resources, env, labels):
        self.last_bundle_uri = bundle.uri
        self.last_result_ref = call_spec.result_ref
        return RunHandle(run_id="run-1", call_id=call_spec.call_id, provider_ref="pod-1")

    def wait(self, handle):
        raise NotImplementedError

    def cancel(self, handle):
        return None


class _TrackingBuilder:
    def resolve_image(self, spec, copy_sources=None):
        return ImageRef(uri="docker.io/carlosdp/train:abc123", digest="", metadata={})


class _TrackingPrimeProvider:
    name = "prime"

    def __init__(self) -> None:
        self.artifacts = _TrackingArtifacts()
        self.executor = _TrackingExecutor()
        self.builder = _TrackingBuilder()

    def set_status_callback(self, cb):
        return None

    def get_builder(self):
        return self.builder

    def get_artifacts(self):
        return self.artifacts

    def get_executor(self):
        return self.executor


def test_prime_run_session_does_not_use_artifact_store_for_submit() -> None:
    app = coral.App(name="prime-local-bundle")

    @app.function()
    def process(text: str) -> str:
        return text.upper()

    provider = _TrackingPrimeProvider()
    with RunSession(provider=provider, app=app, detached=False) as session:
        session.submit(app.get_function("process").spec, ("hello",), {})

    assert provider.artifacts.put_bundle_calls == 0
    assert provider.artifacts.result_uri_calls == 0
    assert provider.executor.last_result_ref == ""
    assert provider.executor.last_bundle_uri is not None
    assert provider.executor.last_bundle_uri.endswith(".tar.gz")

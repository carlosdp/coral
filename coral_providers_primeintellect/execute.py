from __future__ import annotations

import base64
import hashlib
import os
import shutil
import tarfile
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Dict

from coral.providers.base import RunHandle, RunResult
from coral.spec import CallSpec, ResourceSpec
from coral_providers_primeintellect.api import PrimeClient


@dataclass
class PrimeExecutor:
    client: PrimeClient
    project: str
    artifact_store: object
    gpu_type: str
    gpu_count: int
    regions: list[str]
    provider_type: str | None = None
    registry_credentials_id: str | None = None
    custom_template_id: str | None = None
    status_cb: Callable[[str], None] | None = None
    _result_refs: Dict[str, str] | None = None
    _template_ids: Dict[str, str] | None = None

    def _status(self, message: str) -> None:
        if self.status_cb:
            self.status_cb(message)

    def _store_result_ref(self, call_id: str, result_ref: str) -> None:
        if self._result_refs is None:
            self._result_refs = {}
        self._result_refs[call_id] = result_ref

    def _store_template_id(self, key: str, template_id: str) -> None:
        if self._template_ids is None:
            self._template_ids = {}
        self._template_ids[key] = template_id

    def _select_offer(self) -> Dict[str, str]:
        offers = self.client.availability_gpus(
            gpu_type=self.gpu_type,
            gpu_count=self.gpu_count,
            regions=self.regions,
        )
        if self.provider_type:
            offers = [
                offer
                for offer in offers
                if offer.get("provider") == self.provider_type
                or offer.get("providerType") == self.provider_type
            ]
        if not offers:
            raise RuntimeError("No available Prime Intellect offers")
        for offer in offers:
            if offer.get("status") == "Available":
                return offer
        return offers[0]

    def _resolve_custom_template_id(self, image) -> str | None:
        cache_key = self._template_cache_key(image)
        if self._template_ids and cache_key in self._template_ids:
            return self._template_ids[cache_key]
        for key in ("prime_custom_template_id", "prime_template_id", "custom_template_id"):
            value = image.metadata.get(key)
            if value:
                return value
        return self.custom_template_id

    def _template_cache_key(self, image) -> str:
        return image.digest or image.uri

    def _template_name_tag(self, image) -> tuple[str, str]:
        digest = (image.digest or "").replace("sha256:", "")
        if not digest:
            digest = hashlib.sha256(image.uri.encode("utf-8")).hexdigest()
        return "coral", digest[:48]

    def _find_image(self, image_name: str, image_tag: str) -> Dict[str, str] | None:
        images = self.client.list_images()
        for item in images:
            if item.get("imageName") == image_name and item.get("imageTag") == image_tag:
                return item
        return None

    def _wait_for_image_ready(self, image_name: str, image_tag: str, timeout: int = 1800) -> Dict[str, str]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            item = self._find_image(image_name, image_tag)
            if item:
                status = item.get("status")
                if status == "COMPLETED":
                    return item
                if status in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(
                        f"Prime Intellect image build failed for {image_name}:{image_tag}"
                    )
            time.sleep(5)
        raise RuntimeError(
            f"Timed out waiting for Prime Intellect image build for {image_name}:{image_tag}"
        )

    def _create_template_build_context(self, image_uri: str) -> tuple[str, str]:
        tmpdir = tempfile.mkdtemp(prefix="coral-prime-template-")
        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w", encoding="utf-8") as handle:
            handle.write(f"FROM {image_uri}\n")
        archive_path = os.path.join(tmpdir, "context.tar.gz")
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(dockerfile_path, arcname="Dockerfile")
        return archive_path, tmpdir

    def _ensure_custom_template_id(self, image) -> str:
        template_id = self._resolve_custom_template_id(image)
        if template_id:
            return template_id

        cache_key = self._template_cache_key(image)
        image_name, image_tag = self._template_name_tag(image)

        existing = self._find_image(image_name, image_tag)
        if existing:
            status = existing.get("status")
            if status == "COMPLETED":
                template_id = existing.get("id")
                if template_id:
                    self._store_template_id(cache_key, template_id)
                    return template_id
            if status in {"PENDING", "UPLOADING", "BUILDING"}:
                ready = self._wait_for_image_ready(image_name, image_tag)
                template_id = ready.get("id")
                if template_id:
                    self._store_template_id(cache_key, template_id)
                    return template_id

        if self.registry_credentials_id:
            check = self.client.check_docker_image(image.uri, self.registry_credentials_id)
        else:
            check = self.client.check_docker_image(image.uri)
        if check.get("accessible") is False:
            raise RuntimeError(
                f"Prime Intellect cannot access image {image.uri}: {check.get('details')}"
            )

        self._status("Creating Prime Intellect custom template")
        archive_path, tmpdir = self._create_template_build_context(image.uri)
        try:
            build = self.client.create_image_build(
                image_name=image_name,
                image_tag=image_tag,
                dockerfile_path="Dockerfile",
                team_id=self.client.team_id,
            )
            build_id = build.get("build_id") or build.get("buildId")
            upload_url = build.get("upload_url") or build.get("uploadUrl")
            if not build_id or not upload_url:
                raise RuntimeError(f"Unexpected image build response: {build}")
            self.client.upload_build_context(upload_url, archive_path)
            self.client.start_image_build(build_id)

            deadline = time.time() + 1800
            status = None
            while time.time() < deadline:
                build_status = self.client.get_image_build(build_id)
                status = build_status.get("status")
                if status == "COMPLETED":
                    break
                if status in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(
                        f"Prime Intellect image build failed: {build_status.get('errorMessage')}"
                    )
                time.sleep(5)
            if status != "COMPLETED":
                raise RuntimeError("Timed out waiting for Prime Intellect image build to finish")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        ready = self._wait_for_image_ready(image_name, image_tag)
        template_id = ready.get("id") or ready.get("fullImagePath") or f"{image_name}:{image_tag}"
        if not template_id:
            raise RuntimeError(
                f"Prime Intellect image build completed but template ID was not found for {image_name}:{image_tag}"
            )
        self._store_template_id(cache_key, template_id)
        return template_id

    def submit(
        self,
        call_spec: CallSpec,
        image,
        bundle,
        resources: ResourceSpec,
        env: Dict[str, str],
        labels: Dict[str, str],
    ) -> RunHandle:
        call_spec_b64 = base64.b64encode(call_spec.to_json().encode("utf-8")).decode("utf-8")
        env_vars = {
            "CORAL_CALLSPEC_B64": call_spec_b64,
            "CORAL_BUNDLE_URI": bundle.uri,
            "CORAL_RESULT_URI": call_spec.result_ref,
            "PYTHONUNBUFFERED": "1",
        }
        env_vars.update(env)

        offer = self._select_offer()
        custom_template_id = None
        offer_images = offer.get("images") or []
        if image.uri in offer_images:
            pod_image = image.uri
        else:
            custom_template_id = self._ensure_custom_template_id(image)
            pod_image = "custom_template"
        provider_type = offer.get("provider") or offer.get("providerType") or self.provider_type
        if not provider_type:
            raise RuntimeError("Missing provider type for Prime Intellect pod creation")
        payload = {
            "pod": {
                "cloudId": offer.get("cloudId"),
                "gpuType": offer.get("gpuType"),
                "socket": offer.get("socket"),
                "gpuCount": offer.get("gpuCount", self.gpu_count),
                "image": pod_image,
                "dataCenterId": offer.get("dataCenter") or offer.get("dataCenterId"),
                "country": offer.get("country"),
                "security": offer.get("security"),
                "envVars": [
                    {"key": key, "value": value} for key, value in env_vars.items()
                ],
            },
            "provider": {"type": provider_type},
        }
        if pod_image == "custom_template":
            if not custom_template_id:
                custom_template_id = self._ensure_custom_template_id(image)
            payload["pod"]["customTemplateId"] = custom_template_id
        if self.client.team_id:
            payload["team"] = {"teamId": self.client.team_id}
        payload["pod"] = {key: value for key, value in payload["pod"].items() if value is not None}
        self._status("Spawning container")
        response = self.client.create_pod(payload)
        pod_id = response.get("data", {}).get("podId") or response.get("data", {}).get("pod_id")
        if not pod_id:
            raise RuntimeError(f"Unexpected Prime Intellect response: {response}")
        self._store_result_ref(call_spec.call_id, call_spec.result_ref)
        return RunHandle(
            run_id=call_spec.log_labels.get("coral_run_id", ""),
            call_id=call_spec.call_id,
            provider_ref=pod_id,
        )

    def wait(self, handle: RunHandle) -> RunResult:
        self._status("Container running")
        last_status = None
        while True:
            status_resp = self.client.get_pods_status([handle.provider_ref])
            entries = status_resp.get("data", [])
            status = None
            if entries:
                status = entries[0].get("status") or entries[0].get("state")
            if status and status != last_status:
                self._status(f"Prime {status.lower()}")
                last_status = status
            if status in {"SUCCEEDED", "FAILED", "STOPPED"}:
                break
            time.sleep(5)
        result_ref = None
        if self._result_refs:
            result_ref = self._result_refs.get(handle.call_id)
        if result_ref is None and hasattr(self.artifact_store, "gcs"):
            result_ref = self.artifact_store.gcs.result_uri(handle.call_id)
        if result_ref is None:
            raise RuntimeError("Missing result reference for Prime Intellect run")
        output = self.artifact_store.get_result(result_ref)
        success = status == "SUCCEEDED"
        return RunResult(call_id=handle.call_id, success=success, output=output)

    def cancel(self, handle: RunHandle) -> None:
        self.client.delete_pod(handle.provider_ref)

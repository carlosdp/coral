from __future__ import annotations

import base64
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
    status_cb: Callable[[str], None] | None = None
    _result_refs: Dict[str, str] | None = None

    def _status(self, message: str) -> None:
        if self.status_cb:
            self.status_cb(message)

    def _store_result_ref(self, call_id: str, result_ref: str) -> None:
        if self._result_refs is None:
            self._result_refs = {}
        self._result_refs[call_id] = result_ref

    def _select_offer(self) -> Dict[str, str]:
        offers = self.client.availability_gpus(
            gpu_type=self.gpu_type,
            gpu_count=self.gpu_count,
            regions=self.regions,
            provider=self.provider_type,
        )
        if not offers:
            raise RuntimeError("No available Prime Intellect offers")
        for offer in offers:
            if offer.get("status") == "Available":
                return offer
        return offers[0]

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

        image_uri = image.uri
        if self.registry_credentials_id:
            self.client.check_docker_image(image_uri, self.registry_credentials_id)

        offer = self._select_offer()
        payload = {
            "name": f"coral-{call_spec.call_id}",
            "cloudId": offer.get("cloudId"),
            "gpuType": offer.get("gpuType"),
            "socket": offer.get("socket"),
            "gpuCount": offer.get("gpuCount", self.gpu_count),
            "image": image_uri,
            "dataCenterId": offer.get("dataCenterId"),
            "country": offer.get("country"),
            "security": offer.get("security"),
            "envVars": [
                {"name": key, "value": value} for key, value in env_vars.items()
            ],
            "provider": {"type": offer.get("providerType") or self.provider_type},
        }
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

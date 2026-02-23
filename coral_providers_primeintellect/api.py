from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class PrimeClient:
    api_key: str
    team_id: Optional[str] = None
    base_url: str = "https://api.primeintellect.ai"
    app_base_url: str = "https://app.primeintellect.ai"

    def _headers(self) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if self.team_id:
            headers["X-Prime-Team-ID"] = self.team_id
        return headers

    def _app_headers(self) -> Dict[str, str]:
        auth_override = os.environ.get("CORAL_PRIME_APP_AUTHORIZATION", "").strip()
        cookie = os.environ.get("CORAL_PRIME_APP_COOKIE", "").strip()
        auth_header = auth_override or f"Bearer {self.api_key}"
        headers = {
            "Authorization": auth_header,
            "User-Agent": "Mozilla/5.0 (Coral CLI)",
            "Accept": "application/json",
            "Origin": self.app_base_url,
            "Referer": f"{self.app_base_url}/",
        }
        if self.team_id:
            headers["X-Prime-Team-ID"] = self.team_id
        if cookie:
            headers["Cookie"] = cookie
        return headers

    def _raise_for_status(self, resp: requests.Response) -> None:
        if resp.ok:
            return
        try:
            body_text = json.dumps(resp.json(), ensure_ascii=True)
        except ValueError:
            body_text = resp.text.strip()
        if len(body_text) > 4000:
            body_text = body_text[:4000] + "...(truncated)"
        message = (
            f"{resp.status_code} Client Error: {resp.reason} for url: {resp.url}"
            f"\nResponse body: {body_text}"
        )
        raise requests.HTTPError(message, response=resp)

    def _trpc_unpack_result(self, payload: Any, path: str) -> Any:
        item: Dict[str, Any] | None = None
        if isinstance(payload, list):
            if payload and isinstance(payload[0], dict):
                item = payload[0]
        elif isinstance(payload, dict):
            if "0" in payload and isinstance(payload["0"], dict):
                item = payload["0"]
            else:
                item = payload
        if not isinstance(item, dict):
            raise requests.HTTPError(
                f"Unexpected TRPC response shape for {path}: {payload!r}"
            )

        if "error" in item:
            error_payload = item.get("error")
            raise requests.HTTPError(
                f"Prime TRPC error on {path}: {json.dumps(error_payload, ensure_ascii=True)}"
            )

        result = item.get("result")
        if not isinstance(result, dict):
            raise requests.HTTPError(
                f"Missing TRPC result payload for {path}: {item!r}"
            )
        data = result.get("data")
        if isinstance(data, dict) and "json" in data:
            return data["json"]
        return data

    def _trpc_query(self, path: str, input_json: Any = None) -> Any:
        input_payload = json.dumps({"0": {"json": input_json}}, separators=(",", ":"))
        resp = requests.get(
            f"{self.app_base_url}/api/trpc/{path}",
            headers=self._app_headers(),
            params={"batch": "1", "input": input_payload},
            timeout=30,
        )
        if resp.status_code in {401, 403}:
            auth_override = os.environ.get("CORAL_PRIME_APP_AUTHORIZATION", "").strip()
            cookie = os.environ.get("CORAL_PRIME_APP_COOKIE", "").strip()
            if not auth_override and not cookie:
                raise requests.HTTPError(
                    "Prime template TRPC authentication failed. Set CORAL_PRIME_APP_COOKIE "
                    "or CORAL_PRIME_APP_AUTHORIZATION with a valid app session credential."
                )
        self._raise_for_status(resp)
        return self._trpc_unpack_result(resp.json(), path)

    def _trpc_mutation(
        self,
        path: str,
        input_json: Dict[str, Any],
        meta_values: Optional[Dict[str, Any]] = None,
    ) -> Any:
        body: Dict[str, Any] = {"0": {"json": input_json}}
        if meta_values:
            body["0"]["meta"] = {"values": meta_values, "v": 1}
        resp = requests.post(
            f"{self.app_base_url}/api/trpc/{path}?batch=1",
            headers=self._app_headers(),
            json=body,
            timeout=30,
        )
        if resp.status_code in {401, 403}:
            auth_override = os.environ.get("CORAL_PRIME_APP_AUTHORIZATION", "").strip()
            cookie = os.environ.get("CORAL_PRIME_APP_COOKIE", "").strip()
            if not auth_override and not cookie:
                raise requests.HTTPError(
                    "Prime template TRPC authentication failed. Set CORAL_PRIME_APP_COOKIE "
                    "or CORAL_PRIME_APP_AUTHORIZATION with a valid app session credential."
                )
        self._raise_for_status(resp)
        return self._trpc_unpack_result(resp.json(), path)

    def _template_meta_values(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        nullable_fields = ("containerStartCommand", "registryCredentialsId", "teamId")
        for field in nullable_fields:
            if payload.get(field) is None:
                values[field] = ["undefined"]
        restrictions = payload.get("resourceRestrictions")
        if isinstance(restrictions, dict):
            for field in ("ram", "disk", "vcpu"):
                if restrictions.get(field) is None:
                    values[f"resourceRestrictions.{field}"] = ["undefined"]
        return values

    def list_templates(self) -> List[Dict[str, Any]]:
        data = self._trpc_query("templates.getTemplates", None)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def create_template(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = self._trpc_mutation(
            "templates.createTemplate",
            payload,
            meta_values=self._template_meta_values(payload),
        )
        if isinstance(data, dict):
            return data
        raise requests.HTTPError(f"Unexpected createTemplate payload: {data!r}")

    def update_template(self, template_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        update_payload = dict(payload)
        update_payload["id"] = template_id
        data = self._trpc_mutation(
            "templates.updateTemplate",
            update_payload,
            meta_values=self._template_meta_values(update_payload),
        )
        if isinstance(data, dict):
            return data
        raise requests.HTTPError(f"Unexpected updateTemplate payload: {data!r}")

    def availability_gpus(
        self,
        gpu_type: str,
        gpu_count: int,
        regions: Optional[List[str]] = None,
        provider: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
        }
        if regions:
            params["regions"] = regions
        resp = requests.get(
            f"{self.base_url}/api/v1/availability/gpus",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        self._raise_for_status(resp)
        items = resp.json().get("items", [])
        if provider:
            items = [
                item
                for item in items
                if item.get("provider") == provider or item.get("providerType") == provider
            ]
        return items

    def check_docker_image(
        self,
        image: str,
        registry_credentials_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"image": image}
        if registry_credentials_id:
            payload["registry_credentials_id"] = registry_credentials_id
        resp = requests.post(
            f"{self.base_url}/api/v1/template/check-docker-image",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        self._raise_for_status(resp)
        return resp.json()

    def create_pod(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.base_url}/api/v1/pods/",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        self._raise_for_status(resp)
        return resp.json()

    def get_pod(self, pod_id: str) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/{pod_id}",
            headers=self._headers(),
            timeout=30,
        )
        self._raise_for_status(resp)
        return resp.json()

    def get_pods_status(self, pod_ids: List[str]) -> Dict[str, Any]:
        params = {"pod_ids": pod_ids}
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/status",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        self._raise_for_status(resp)
        return resp.json()

    def get_pod_logs(self, pod_id: str, tail: int = 200) -> str:
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/{pod_id}/log",
            headers=self._headers(),
            params={"tail": tail},
            timeout=30,
        )
        self._raise_for_status(resp)
        try:
            data = resp.json()
            if isinstance(data, dict):
                return data.get("data") or data.get("log") or ""
        except ValueError:
            pass
        return resp.text

    def delete_pod(self, pod_id: str, ignore_missing: bool = False) -> None:
        resp = requests.delete(
            f"{self.base_url}/api/v1/pods/{pod_id}",
            headers=self._headers(),
            timeout=30,
        )
        if ignore_missing and resp.status_code == 404:
            return
        self._raise_for_status(resp)

    def list_ssh_keys(self, offset: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
        resp = requests.get(
            f"{self.base_url}/api/v1/ssh_keys/",
            headers=self._headers(),
            params={"offset": offset, "limit": limit},
            timeout=30,
        )
        self._raise_for_status(resp)
        data = resp.json()
        return data.get("data") or []

    def upload_ssh_key(self, name: str, public_key: str) -> Dict[str, Any]:
        payload = {
            "name": name,
            "publicKey": public_key,
        }
        resp = requests.post(
            f"{self.base_url}/api/v1/ssh_keys/",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        self._raise_for_status(resp)
        return resp.json()

    def set_primary_ssh_key(self, key_id: str, is_primary: bool = True) -> Dict[str, Any]:
        payload = {"isPrimary": is_primary}
        resp = requests.patch(
            f"{self.base_url}/api/v1/ssh_keys/{key_id}",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        self._raise_for_status(resp)
        return resp.json()

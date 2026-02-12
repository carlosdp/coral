from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class PrimeClient:
    api_key: str
    team_id: Optional[str] = None
    base_url: str = "https://api.primeintellect.ai"

    def _headers(self) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if self.team_id:
            headers["X-Prime-Team-ID"] = self.team_id
        return headers

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
        resp.raise_for_status()
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
        resp.raise_for_status()
        return resp.json()

    def create_pod(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.base_url}/api/v1/pods/",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def get_pod(self, pod_id: str) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/{pod_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_pods_status(self, pod_ids: List[str]) -> Dict[str, Any]:
        params = {"pod_ids": pod_ids}
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/status",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def list_images(self) -> List[Dict[str, Any]]:
        resp = requests.get(
            f"{self.base_url}/api/v1/images",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data") or data.get("items") or []

    def create_image_build(
        self,
        image_name: str,
        image_tag: str,
        dockerfile_path: str = "Dockerfile",
        team_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "image_name": image_name,
            "image_tag": image_tag,
            "dockerfile_path": dockerfile_path,
        }
        if team_id:
            payload["teamId"] = team_id
        resp = requests.post(
            f"{self.base_url}/api/v1/images/build",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_image_build(self, build_id: str) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/api/v1/images/build/{build_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def start_image_build(self, build_id: str) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.base_url}/api/v1/images/build/{build_id}/start",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def upload_build_context(self, upload_url: str, archive_path: str) -> None:
        with open(archive_path, "rb") as archive:
            resp = requests.put(
                upload_url,
                data=archive,
                timeout=300,
            )
        resp.raise_for_status()

    def get_pod_logs(self, pod_id: str, tail: int = 200) -> str:
        resp = requests.get(
            f"{self.base_url}/api/v1/pods/{pod_id}/log",
            headers=self._headers(),
            params={"tail": tail},
            timeout=30,
        )
        resp.raise_for_status()
        try:
            data = resp.json()
            if isinstance(data, dict):
                return data.get("data") or data.get("log") or ""
        except ValueError:
            pass
        return resp.text

    def delete_pod(self, pod_id: str) -> None:
        resp = requests.delete(
            f"{self.base_url}/api/v1/pods/{pod_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()

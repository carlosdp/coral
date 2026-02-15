from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class LocalSource:
    name: str
    path: str
    mode: str = "sync"
    ignore: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ImageSpec:
    base_image: str
    python_version: str = "3.11"
    apt_packages: List[str] = field(default_factory=list)
    pip_packages: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    workdir: str = "/opt/coral"
    local_sources: List[LocalSource] = field(default_factory=list)


@dataclass(frozen=True)
class ResourceSpec:
    cpu: int = 1
    memory: str = "2Gi"
    gpu: Optional[str] = None
    timeout: int = 3600
    retries: int = 0


@dataclass(frozen=True)
class FunctionSpec:
    name: str
    module: str
    qualname: str
    source_file: str
    resources: ResourceSpec
    image: Optional[ImageSpec] = None
    build_image: bool = True


@dataclass(frozen=True)
class AppSpec:
    name: str
    image: ImageSpec
    include_source: bool = True


@dataclass(frozen=True)
class BundleSpec:
    bundle_path: str
    bundle_hash: str


@dataclass(frozen=True)
class CallSpec:
    call_id: str
    module: str
    qualname: str
    args_b64: str
    kwargs_b64: str
    serialization: str
    result_ref: str
    stdout_mode: str
    log_labels: Dict[str, str]
    protocol_version: str = "1"

    def to_json(self) -> str:
        return json.dumps(
            {
                "call_id": self.call_id,
                "module": self.module,
                "qualname": self.qualname,
                "args_b64": self.args_b64,
                "kwargs_b64": self.kwargs_b64,
                "serialization": self.serialization,
                "result_ref": self.result_ref,
                "stdout_mode": self.stdout_mode,
                "log_labels": self.log_labels,
                "protocol_version": self.protocol_version,
            }
        )

    @staticmethod
    def from_json(payload: str) -> "CallSpec":
        data = json.loads(payload)
        return CallSpec(
            call_id=data["call_id"],
            module=data["module"],
            qualname=data["qualname"],
            args_b64=data["args_b64"],
            kwargs_b64=data["kwargs_b64"],
            serialization=data["serialization"],
            result_ref=data["result_ref"],
            stdout_mode=data.get("stdout_mode", "stream"),
            log_labels=data.get("log_labels", {}),
            protocol_version=data.get("protocol_version", "1"),
        )

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict


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

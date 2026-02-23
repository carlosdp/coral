from __future__ import annotations

import base64
import io
import os
import sys
import tarfile
from pathlib import Path

from coral_runtime.fetch import fetch_bundle
from coral_runtime.invoke import invoke
from coral_runtime.io import write_bytes
from coral_runtime.serialization import dumps
from coral_runtime.spec import CallSpec

RESULT_MARKER = "__CORAL_RESULT_B64__:"
ERROR_MARKER = "__CORAL_ERROR_B64__:"


def _add_bundle_paths(dest: Path) -> None:
    extra_paths = [str(dest)]
    for child in dest.iterdir():
        if child.is_dir():
            extra_paths.append(str(child))
    existing = os.environ.get("PYTHONPATH", "")
    combined = os.pathsep.join(extra_paths + ([existing] if existing else []))
    os.environ["PYTHONPATH"] = combined
    for path in reversed(extra_paths):
        if path not in sys.path:
            sys.path.insert(0, path)


def _extract_bundle_b64(payload_b64: str, dest: Path) -> None:
    payload = base64.b64decode(payload_b64.encode("utf-8"))
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        tar.extractall(dest)


def _chunked_env(name: str) -> str | None:
    direct = os.environ.get(name)
    if direct:
        return direct
    chunks_raw = os.environ.get(f"{name}_CHUNKS")
    if not chunks_raw:
        return None
    chunks = int(chunks_raw)
    parts: list[str] = []
    for idx in range(chunks):
        part = os.environ.get(f"{name}_{idx:04d}")
        if part is None:
            raise RuntimeError(f"Missing chunk {idx} for {name}")
        parts.append(part)
    return "".join(parts)


def main() -> None:
    callspec_b64 = _chunked_env("CORAL_CALLSPEC_B64")
    if not callspec_b64:
        raise RuntimeError("CORAL_CALLSPEC_B64 not set")

    bundle_uri = os.environ.get("CORAL_BUNDLE_URI") or os.environ.get("CORAL_BUNDLE_GCS_URI")
    bundle_b64 = _chunked_env("CORAL_BUNDLE_B64")
    result_uri = os.environ.get("CORAL_RESULT_URI") or os.environ.get("CORAL_RESULT_GCS_URI")

    if bundle_b64:
        dest = Path("/opt/coral/src")
        _extract_bundle_b64(bundle_b64, dest)
        _add_bundle_paths(dest)
    elif bundle_uri:
        dest = Path("/opt/coral/src")
        fetch_bundle(bundle_uri, dest)
        _add_bundle_paths(dest)

    callspec_json = base64.b64decode(callspec_b64.encode("utf-8")).decode("utf-8")
    call_spec = CallSpec.from_json(callspec_json)

    success, payload = invoke(
        module=call_spec.module,
        qualname=call_spec.qualname,
        args_b64=call_spec.args_b64,
        kwargs_b64=call_spec.kwargs_b64,
    )

    if success:
        result_bytes = dumps(payload)
    else:
        result_bytes = payload

    if result_uri:
        write_bytes(result_uri, result_bytes)
    if os.environ.get("CORAL_RESULT_STDOUT") == "1":
        marker = RESULT_MARKER if success else ERROR_MARKER
        encoded = base64.b64encode(result_bytes).decode("utf-8")
        print(marker + encoded, flush=True)

    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

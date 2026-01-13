from __future__ import annotations

import base64
import os
from pathlib import Path

from coral_runtime.fetch import fetch_bundle
from coral_runtime.invoke import invoke
from coral_runtime.io import write_bytes
from coral_runtime.serialization import dumps
from coral_runtime.spec import CallSpec


def main() -> None:
    callspec_b64 = os.environ.get("CORAL_CALLSPEC_B64")
    if not callspec_b64:
        raise RuntimeError("CORAL_CALLSPEC_B64 not set")

    bundle_uri = os.environ.get("CORAL_BUNDLE_URI") or os.environ.get("CORAL_BUNDLE_GCS_URI")
    result_uri = os.environ.get("CORAL_RESULT_URI") or os.environ.get("CORAL_RESULT_GCS_URI")

    if bundle_uri:
        dest = Path("/opt/coral/src")
        fetch_bundle(bundle_uri, dest)
        os.environ["PYTHONPATH"] = f"{dest}:{os.environ.get('PYTHONPATH', '')}"

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

    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

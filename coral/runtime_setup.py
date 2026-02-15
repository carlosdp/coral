from __future__ import annotations

import base64
import json
import textwrap

from coral.image import build_plan
from coral.spec import ImageSpec

CORAL_IMAGE_BUILD_DISABLED_ENV = "CORAL_IMAGE_BUILD_DISABLED"
CORAL_RUNTIME_SETUP_B64_ENV = "CORAL_RUNTIME_SETUP_B64"
CORAL_IMAGE_BUILD_DISABLED_METADATA = "coral_image_build_disabled"


def runtime_setup_payload(image: ImageSpec) -> dict:
    plan = build_plan(image)
    return {
        "apt_packages": plan.get("apt_packages", []),
        "pip_packages": plan.get("pip_packages", []),
        "runtime_requirements": plan.get("runtime_requirements", []),
        "env": plan.get("env", {}),
        "workdir": plan.get("workdir", "/opt/coral"),
    }


def encode_runtime_setup_payload(image: ImageSpec) -> str:
    payload = runtime_setup_payload(image)
    encoded = base64.b64encode(json.dumps(payload, sort_keys=True).encode("utf-8"))
    return encoded.decode("utf-8")


RUNTIME_BOOTSTRAP_SCRIPT = textwrap.dedent(
    """
    import base64
    import importlib
    import io
    import json
    import os
    import shutil
    import subprocess
    import sys
    import tarfile
    import traceback
    from pathlib import Path


    def _parse_gcs_uri(uri):
        if not uri.startswith("gs://"):
            raise ValueError(f"Not a GCS URI: {uri}")
        without = uri[len("gs://") :]
        bucket, blob = without.split("/", 1)
        return bucket, blob


    def _run(cmd, env=None):
        subprocess.run(cmd, check=True, env=env)


    def _decode_setup():
        payload = os.environ.get("CORAL_RUNTIME_SETUP_B64")
        if not payload:
            return {}
        raw = base64.b64decode(payload.encode("utf-8")).decode("utf-8")
        return json.loads(raw)


    def _ensure_pip():
        try:
            _run([sys.executable, "-m", "pip", "--version"])
        except Exception:
            _run([sys.executable, "-m", "ensurepip", "--upgrade"])


    def _apply_runtime_setup(setup):
        for key, value in (setup.get("env") or {}).items():
            os.environ.setdefault(str(key), str(value))

        workdir = setup.get("workdir")
        if workdir:
            Path(workdir).mkdir(parents=True, exist_ok=True)
            os.chdir(workdir)

        apt_packages = [str(item) for item in (setup.get("apt_packages") or [])]
        if apt_packages:
            if not shutil.which("apt-get"):
                raise RuntimeError("apt-get is not available in the runtime image")
            apt_env = dict(os.environ)
            apt_env["DEBIAN_FRONTEND"] = "noninteractive"
            _run(["apt-get", "update"], env=apt_env)
            _run(["apt-get", "install", "-y", *apt_packages], env=apt_env)

        pip_packages = []
        for item in (setup.get("pip_packages") or []):
            item = str(item).strip()
            if item:
                pip_packages.append(item)
        for item in (setup.get("runtime_requirements") or []):
            item = str(item).strip()
            if item:
                pip_packages.append(item)
        pip_packages = list(dict.fromkeys(pip_packages))
        if pip_packages:
            _ensure_pip()
            _run([sys.executable, "-m", "pip", "install", "--no-cache-dir", *pip_packages])


    def _download(uri):
        if uri.startswith("http://") or uri.startswith("https://"):
            import requests

            resp = requests.get(uri, timeout=120)
            resp.raise_for_status()
            return resp.content
        if uri.startswith("gs://"):
            from google.cloud import storage

            bucket_name, blob_name = _parse_gcs_uri(uri)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.download_as_bytes()
        return Path(uri).read_bytes()


    def _write(uri, payload):
        if uri.startswith("http://") or uri.startswith("https://"):
            import requests

            resp = requests.put(uri, data=payload, timeout=120)
            resp.raise_for_status()
            return
        if uri.startswith("gs://"):
            from google.cloud import storage

            bucket_name, blob_name = _parse_gcs_uri(uri)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(payload)
            return
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


    def _fetch_bundle():
        bundle_uri = os.environ.get("CORAL_BUNDLE_URI") or os.environ.get("CORAL_BUNDLE_GCS_URI")
        if not bundle_uri:
            return
        payload = _download(bundle_uri)
        dest = Path("/opt/coral/src")
        dest.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
            tar.extractall(dest)

        extra_paths = [str(dest)]
        for child in sorted(dest.iterdir()):
            if child.is_dir():
                extra_paths.append(str(child))
        existing = os.environ.get("PYTHONPATH", "")
        combined = os.pathsep.join(extra_paths + ([existing] if existing else []))
        os.environ["PYTHONPATH"] = combined
        for path in reversed(extra_paths):
            if path not in sys.path:
                sys.path.insert(0, path)


    def _resolve_target(module_name, qualname):
        module = importlib.import_module(module_name)
        target = module
        for part in qualname.split("."):
            if part == "<locals>":
                continue
            target = getattr(target, part)
        return target


    def _invoke():
        import cloudpickle

        callspec_b64 = os.environ.get("CORAL_CALLSPEC_B64")
        if not callspec_b64:
            raise RuntimeError("CORAL_CALLSPEC_B64 not set")
        call = json.loads(base64.b64decode(callspec_b64.encode("utf-8")).decode("utf-8"))

        args = cloudpickle.loads(base64.b64decode(call["args_b64"].encode("utf-8")))
        kwargs = cloudpickle.loads(base64.b64decode(call["kwargs_b64"].encode("utf-8")))
        target = _resolve_target(call["module"], call["qualname"])
        if hasattr(target, "_fn") and callable(getattr(target, "_fn")):
            target = target._fn
        result = target(*args, **kwargs)
        return cloudpickle.dumps(result)


    def main():
        setup = _decode_setup()
        _apply_runtime_setup(setup)
        _fetch_bundle()

        success = True
        try:
            payload = _invoke()
        except Exception:
            success = False
            payload = traceback.format_exc().encode("utf-8")

        result_uri = os.environ.get("CORAL_RESULT_URI") or os.environ.get("CORAL_RESULT_GCS_URI")
        if result_uri:
            _write(result_uri, payload)

        if not success:
            raise SystemExit(1)


    if __name__ == "__main__":
        main()
    """
).strip()

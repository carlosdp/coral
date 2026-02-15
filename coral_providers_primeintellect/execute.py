from __future__ import annotations

import base64
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict

from coral.providers.base import RunHandle, RunResult
from coral.runtime_setup import (
    CORAL_IMAGE_BUILD_DISABLED_ENV,
    CORAL_IMAGE_BUILD_DISABLED_METADATA,
)
from coral.spec import CallSpec, ResourceSpec
from coral_providers_primeintellect.api import PrimeClient

SSH_RESULT_MARKER = "__CORAL_RESULT_B64__:"
SSH_ERROR_MARKER = "__CORAL_ERROR_B64__:"

SSH_HOST_RUNNER_SCRIPT = textwrap.dedent(
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

    RESULT_MARKER = "__CORAL_RESULT_B64__:"
    ERROR_MARKER = "__CORAL_ERROR_B64__:"

    def _run(cmd, env=None):
        subprocess.run(cmd, check=True, env=env)

    def _as_root(cmd):
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            return cmd
        if shutil.which("sudo"):
            return ["sudo", "-n", *cmd]
        return cmd

    def _ensure_pip():
        try:
            _run([sys.executable, "-m", "pip", "--version"])
            return
        except Exception:
            pass

        if shutil.which("apt-get"):
            apt_env = dict(os.environ)
            apt_env["DEBIAN_FRONTEND"] = "noninteractive"
            try:
                _run(_as_root(["apt-get", "update"]), env=apt_env)
                _run(_as_root(["apt-get", "install", "-y", "python3-pip"]), env=apt_env)
                _run([sys.executable, "-m", "pip", "--version"])
                return
            except Exception:
                pass

        try:
            _run([sys.executable, "-m", "ensurepip", "--upgrade"])
            _run([sys.executable, "-m", "pip", "--version"])
            return
        except Exception as exc:
            raise RuntimeError(
                "pip is required for Prime no-build runtime setup and could not be installed"
            ) from exc

    def _use_user_site():
        in_venv = getattr(sys, "base_prefix", sys.prefix) != sys.prefix
        return not in_venv and not (hasattr(os, "geteuid") and os.geteuid() == 0)

    def _setup():
        setup = {}
        setup_b64 = os.environ.get("CORAL_RUNTIME_SETUP_B64", "")
        if setup_b64:
            setup = json.loads(base64.b64decode(setup_b64.encode("utf-8")).decode("utf-8"))

        for key, value in (setup.get("env") or {}).items():
            os.environ.setdefault(str(key), str(value))

        user_env_b64 = os.environ.get("CORAL_USER_ENV_B64", "")
        if user_env_b64:
            user_env = json.loads(base64.b64decode(user_env_b64.encode("utf-8")).decode("utf-8"))
            for key, value in user_env.items():
                os.environ[str(key)] = str(value)

        workdir = setup.get("workdir")
        if workdir:
            try:
                Path(workdir).mkdir(parents=True, exist_ok=True)
                os.chdir(workdir)
            except PermissionError:
                fallback = Path("/tmp/coral")
                fallback.mkdir(parents=True, exist_ok=True)
                os.chdir(fallback)

        apt_packages = [
            str(item) for item in (setup.get("apt_packages") or []) if str(item).strip()
        ]
        if apt_packages:
            if not shutil.which("apt-get"):
                raise RuntimeError("apt-get is not available in the runtime image")
            apt_env = dict(os.environ)
            apt_env["DEBIAN_FRONTEND"] = "noninteractive"
            _run(_as_root(["apt-get", "update"]), env=apt_env)
            _run(_as_root(["apt-get", "install", "-y", *apt_packages]), env=apt_env)

        pip_packages = []
        for key in ("pip_packages", "runtime_requirements"):
            for item in (setup.get(key) or []):
                item = str(item).strip()
                if item:
                    pip_packages.append(item)
        pip_packages = list(dict.fromkeys(pip_packages))
        if pip_packages:
            _ensure_pip()
            pip_install = [sys.executable, "-m", "pip", "install", "--no-cache-dir"]
            if _use_user_site():
                pip_install.append("--user")
            _run([*pip_install, *pip_packages])

    def _load_bundle():
        src = None
        candidates = (
            Path("/opt/coral/src"),
            Path("/tmp/coral/src"),
            Path.home() / ".coral" / "src",
        )
        for candidate in candidates:
            try:
                candidate.mkdir(parents=True, exist_ok=True)
                src = candidate
                break
            except PermissionError:
                continue
        if src is None:
            raise RuntimeError("Could not create a writable bundle directory")
        with tarfile.open("/tmp/coral_bundle.tar.gz", mode="r:gz") as tar:
            tar.extractall(src)

        extra_paths = [str(src)]
        for child in sorted(src.iterdir()):
            if child.is_dir():
                if (child / "__init__.py").exists():
                    continue
                extra_paths.append(str(child))
        existing = os.environ.get("PYTHONPATH", "")
        os.environ["PYTHONPATH"] = os.pathsep.join(extra_paths + ([existing] if existing else []))
        for path in reversed(extra_paths):
            if path not in sys.path:
                sys.path.insert(0, path)

    def _resolve(module_name, qualname):
        module = importlib.import_module(module_name)
        target = module
        for part in qualname.split("."):
            if part == "<locals>":
                continue
            target = getattr(target, part)
        return target

    def _load_cloudpickle():
        try:
            import cloudpickle
            return cloudpickle
        except Exception:
            _ensure_pip()
            pip_install = [sys.executable, "-m", "pip", "install", "--no-cache-dir"]
            if _use_user_site():
                pip_install.append("--user")
            _run([*pip_install, "cloudpickle"])
            import cloudpickle
            return cloudpickle

    def _invoke():
        cloudpickle = _load_cloudpickle()
        call_b64 = os.environ.get("CORAL_CALLSPEC_B64")
        if not call_b64:
            raise RuntimeError("CORAL_CALLSPEC_B64 not set")
        call = json.loads(base64.b64decode(call_b64.encode("utf-8")).decode("utf-8"))
        args = cloudpickle.loads(base64.b64decode(call["args_b64"].encode("utf-8")))
        kwargs = cloudpickle.loads(base64.b64decode(call["kwargs_b64"].encode("utf-8")))
        target = _resolve(call["module"], call["qualname"])
        if hasattr(target, "_fn") and callable(getattr(target, "_fn")):
            target = target._fn
        result = target(*args, **kwargs)
        return cloudpickle.dumps(result)

    def main():
        _setup()
        _load_bundle()
        try:
            payload = _invoke()
            marker = RESULT_MARKER
            code = 0
        except Exception:
            payload = traceback.format_exc().encode("utf-8")
            marker = ERROR_MARKER
            code = 1
        encoded = base64.b64encode(payload).decode("utf-8")
        print(marker + encoded, flush=True)
        raise SystemExit(code)

    if __name__ == "__main__":
        main()
    """
).strip()


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
    _run_modes: Dict[str, str] | None = None
    _host_callspec_b64: Dict[str, str] | None = None
    _host_bundle_paths: Dict[str, str] | None = None
    _host_setup_b64: Dict[str, str] | None = None
    _host_user_env_b64: Dict[str, str] | None = None
    _ssh_key_id: str | None = None
    _ssh_private_key_path: str | None = None

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

    def _store_run_mode(self, call_id: str, mode: str) -> None:
        if self._run_modes is None:
            self._run_modes = {}
        self._run_modes[call_id] = mode

    def _store_host_execution(
        self,
        call_id: str,
        callspec_b64: str,
        bundle_path: str,
        setup_b64: str,
        user_env_b64: str,
    ) -> None:
        if self._host_callspec_b64 is None:
            self._host_callspec_b64 = {}
        if self._host_bundle_paths is None:
            self._host_bundle_paths = {}
        if self._host_setup_b64 is None:
            self._host_setup_b64 = {}
        if self._host_user_env_b64 is None:
            self._host_user_env_b64 = {}
        self._host_callspec_b64[call_id] = callspec_b64
        self._host_bundle_paths[call_id] = bundle_path
        self._host_setup_b64[call_id] = setup_b64
        self._host_user_env_b64[call_id] = user_env_b64

    def _run_mode(self, call_id: str) -> str:
        if not self._run_modes:
            return "image_build"
        return self._run_modes.get(call_id, "image_build")

    def _select_offer(self, timeout: int = 90, poll_interval: int = 5) -> Dict[str, str]:
        deadline = time.time() + timeout
        while True:
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
            if offers:
                for offer in offers:
                    if offer.get("status") == "Available":
                        return offer
                return offers[0]
            if time.time() >= deadline:
                provider_hint = ""
                if self.provider_type:
                    provider_hint = f" for provider '{self.provider_type}'"
                raise RuntimeError(
                    f"No available Prime Intellect offers{provider_hint} "
                    f"after waiting {timeout}s"
                )
            self._status("Waiting for available Prime offer")
            time.sleep(poll_interval)

    def _default_offer_image(self, offer: Dict[str, str]) -> str:
        images = offer.get("images") or []
        if images:
            return images[0]
        return "ubuntu_22_cuda_12"

    def _ensure_local_ssh_keypair(self) -> tuple[str, str]:
        env_private = os.environ.get("CORAL_SSH_PRIVATE_KEY_PATH")
        if env_private:
            private_path = Path(os.path.expanduser(env_private))
            if not private_path.exists():
                raise RuntimeError(
                    f"CORAL_SSH_PRIVATE_KEY_PATH does not exist: {private_path}"
                )
            public_path = private_path.with_suffix(private_path.suffix + ".pub")
            inline_public = os.environ.get("CORAL_SSH_PUBLIC_KEY", "").strip()
            if inline_public:
                self._ssh_private_key_path = str(private_path)
                return str(private_path), inline_public
            if not public_path.exists():
                raise RuntimeError(
                    f"Missing public key for CORAL_SSH_PRIVATE_KEY_PATH: {public_path}. "
                    "Set CORAL_SSH_PUBLIC_KEY or provide <private>.pub."
                )
            self._ssh_private_key_path = str(private_path)
            return str(private_path), public_path.read_text(encoding="utf-8").strip()

        key_dir = Path.home() / ".coral" / "ssh"
        key_dir.mkdir(parents=True, exist_ok=True)
        private_path = key_dir / "prime_no_build_ed25519"
        public_path = Path(f"{private_path}.pub")
        if not private_path.exists() or not public_path.exists():
            if not shutil.which("ssh-keygen"):
                raise RuntimeError("ssh-keygen is required for Prime no-build runtime setup")
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-N",
                    "",
                    "-C",
                    "coral-prime-no-build",
                    "-f",
                    str(private_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        private_path.chmod(0o600)
        public_key = public_path.read_text(encoding="utf-8").strip()
        if not public_key:
            raise RuntimeError(f"Prime no-build public key is empty: {public_path}")
        self._ssh_private_key_path = str(private_path)
        return str(private_path), public_key

    def _ensure_ssh_key_id(self) -> str:
        if self._ssh_key_id:
            return self._ssh_key_id
        _private_path, public_key = self._ensure_local_ssh_keypair()
        for key in self.client.list_ssh_keys():
            if str(key.get("publicKey", "")).strip() == public_key:
                key_id = key.get("id")
                if isinstance(key_id, str) and key_id:
                    if not key.get("isPrimary"):
                        self.client.set_primary_ssh_key(key_id, is_primary=True)
                    self._ssh_key_id = key_id
                    return key_id

        key_hash = hashlib.sha256(public_key.encode("utf-8")).hexdigest()[:12]
        key_name = f"coral-no-build-{key_hash}"
        created = self.client.upload_ssh_key(key_name, public_key)
        key_id = created.get("id")
        if not isinstance(key_id, str) or not key_id:
            raise RuntimeError(f"Prime SSH key upload returned unexpected response: {created}")
        self.client.set_primary_ssh_key(key_id, is_primary=True)
        self._ssh_key_id = key_id
        return key_id

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

    def _wait_for_image_ready(
        self,
        image_name: str,
        image_tag: str,
        timeout: int = 1800,
    ) -> Dict[str, str]:
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
                "Prime Intellect image build completed but template ID was not found for "
                f"{image_name}:{image_tag}"
            )
        self._store_template_id(cache_key, template_id)
        return template_id

    def _status_entries(self, status_resp: Dict[str, object]) -> list[Dict[str, object]]:
        data = status_resp.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            inner = data.get("items")
            if isinstance(inner, list):
                return [item for item in inner if isinstance(item, dict)]
        return []

    def _pod_from_response(self, response: Dict[str, object]) -> Dict[str, object]:
        if "data" in response and isinstance(response["data"], dict):
            return response["data"]
        return response

    def _pod_id_from_response(self, response: Dict[str, object]) -> str | None:
        pod = self._pod_from_response(response)
        pod_id = pod.get("id") or pod.get("podId") or pod.get("pod_id")
        if isinstance(pod_id, str):
            return pod_id
        return None

    def _wait_for_pod_active(self, pod_id: str, timeout: int = 900) -> Dict[str, object]:
        deadline = time.time() + timeout
        last_status = None
        while time.time() < deadline:
            status_resp = self.client.get_pods_status([pod_id])
            entries = self._status_entries(status_resp)
            status = None
            entry: Dict[str, object] | None = None
            if entries:
                entry = entries[0]
                status_raw = entry.get("status") or entry.get("state")
                if isinstance(status_raw, str):
                    status = status_raw.upper()
            if status and status != last_status:
                self._status(f"Prime {status.lower()}")
                last_status = status
            if status == "ACTIVE":
                return entry or {}
            if status in {"ERROR", "FAILED", "STOPPED", "TERMINATED"}:
                raise RuntimeError(f"Prime pod entered terminal status before activation: {status}")
            time.sleep(5)
        raise RuntimeError(f"Timed out waiting for Prime pod {pod_id} to become ACTIVE")

    def _get_pod_ssh_connection(self, pod_id: str, active_entry: Dict[str, object]) -> str:
        ssh = active_entry.get("sshConnection") or active_entry.get("ssh_connection")
        if isinstance(ssh, str) and ssh.strip():
            return ssh.strip()
        pod = self._pod_from_response(self.client.get_pod(pod_id))
        ssh = pod.get("sshConnection") or pod.get("ssh_connection")
        if isinstance(ssh, str) and ssh.strip():
            return ssh.strip()
        raise RuntimeError(f"Prime pod {pod_id} did not return an SSH connection")

    def _ssh_base_command(self, ssh_connection: str) -> list[str]:
        if not shutil.which("ssh"):
            raise RuntimeError("ssh is required to run Prime host runtime setup")
        ssh_parts = shlex.split(ssh_connection)
        if not ssh_parts:
            raise RuntimeError(f"Invalid Prime SSH connection string: {ssh_connection}")
        if ssh_parts[0] == "ssh":
            ssh_target = ssh_parts[1:]
        else:
            ssh_target = ssh_parts
        base = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "IdentitiesOnly=yes",
        ]
        if self._ssh_private_key_path:
            base.extend(["-i", self._ssh_private_key_path])
        return [*base, *ssh_target]

    def _wait_for_ssh_ready(self, ssh_base: list[str], timeout: int = 180) -> None:
        self._status("Waiting for SSH access")
        deadline = time.time() + timeout
        last_error = "Unknown SSH error"
        while time.time() < deadline:
            try:
                completed = subprocess.run(
                    [*ssh_base, "true"],
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                last_error = "SSH connection attempt timed out"
                time.sleep(5)
                continue
            if completed.returncode == 0:
                return
            stderr = (completed.stderr or "").strip()
            stdout = (completed.stdout or "").strip()
            last_error = stderr or stdout or f"SSH exited with code {completed.returncode}"
            time.sleep(5)
        raise RuntimeError(
            f"Prime SSH access was not ready within {timeout}s: {last_error}"
        )

    def _upload_bundle_over_ssh(self, ssh_base: list[str], bundle_path: str) -> None:
        local_path = os.path.expanduser(bundle_path)
        if not os.path.exists(local_path):
            raise RuntimeError(f"Local bundle path does not exist: {local_path}")
        command = [*ssh_base, "bash", "-lc", "cat >/tmp/coral_bundle.tar.gz"]
        with open(local_path, "rb") as handle:
            completed = subprocess.run(
                command,
                input=handle.read(),
                capture_output=True,
                timeout=300,
                check=False,
            )
        if completed.returncode != 0:
            stderr = completed.stderr.decode("utf-8", errors="replace").strip()
            stdout = completed.stdout.decode("utf-8", errors="replace").strip()
            message = stderr or stdout or "Unknown SSH upload error"
            raise RuntimeError(f"Prime bundle upload over SSH failed: {message}")

    def _run_host_runner_over_ssh(
        self,
        ssh_base: list[str],
        call_id: str,
    ) -> tuple[bool, bytes]:
        callspec_b64 = ""
        bundle_path = ""
        setup_b64 = ""
        user_env_b64 = ""
        if self._host_callspec_b64:
            callspec_b64 = self._host_callspec_b64.get(call_id, "")
        if self._host_bundle_paths:
            bundle_path = self._host_bundle_paths.get(call_id, "")
        if self._host_setup_b64:
            setup_b64 = self._host_setup_b64.get(call_id, "")
        if self._host_user_env_b64:
            user_env_b64 = self._host_user_env_b64.get(call_id, "")
        if not callspec_b64 or not bundle_path:
            raise RuntimeError(f"Missing host execution payload for call {call_id}")

        self._upload_bundle_over_ssh(ssh_base, bundle_path)
        remote_script = (
            "set -euo pipefail\n"
            f"export CORAL_CALLSPEC_B64={shlex.quote(callspec_b64)}\n"
            f"export CORAL_RUNTIME_SETUP_B64={shlex.quote(setup_b64)}\n"
            f"export CORAL_USER_ENV_B64={shlex.quote(user_env_b64)}\n"
            "cat >/tmp/coral_host_runner.py <<'PY'\n"
            f"{SSH_HOST_RUNNER_SCRIPT}\n"
            "PY\n"
            "PYTHON_BIN=$(command -v python3 || command -v python)\n"
            "if [ -z \"$PYTHON_BIN\" ]; then\n"
            "  echo 'python is not available on the Prime host image' >&2\n"
            "  exit 127\n"
            "fi\n"
            "RUNTIME_PYTHON=\"$PYTHON_BIN\"\n"
            "if [ ! -x /tmp/coral_runtime_venv/bin/python ]; then\n"
            "  if ! \"$PYTHON_BIN\" -m venv /tmp/coral_runtime_venv "
            ">/tmp/coral_venv.log 2>&1; then\n"
            "    if command -v apt-get >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then\n"
            "      export DEBIAN_FRONTEND=noninteractive\n"
            "      sudo -n apt-get update >/tmp/coral_venv_apt.log 2>&1 || true\n"
            "      sudo -n apt-get install -y python3-venv >/tmp/coral_venv_apt.log 2>&1 || true\n"
            "      \"$PYTHON_BIN\" -m venv /tmp/coral_runtime_venv "
            ">/tmp/coral_venv.log 2>&1 || true\n"
            "    fi\n"
            "  fi\n"
            "fi\n"
            "if [ -x /tmp/coral_runtime_venv/bin/python ]; then\n"
            "  RUNTIME_PYTHON=/tmp/coral_runtime_venv/bin/python\n"
            "fi\n"
            "\"$RUNTIME_PYTHON\" /tmp/coral_host_runner.py\n"
        )
        command = [*ssh_base, "bash", "-lc", remote_script]
        self._status("Running host runtime setup")
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=3600,
            check=False,
        )
        result_payload: bytes | None = None
        error_payload: bytes | None = None
        for line in completed.stdout.splitlines():
            if line.startswith(SSH_RESULT_MARKER):
                encoded = line[len(SSH_RESULT_MARKER) :].strip()
                result_payload = base64.b64decode(encoded.encode("utf-8"))
            elif line.startswith(SSH_ERROR_MARKER):
                encoded = line[len(SSH_ERROR_MARKER) :].strip()
                error_payload = base64.b64decode(encoded.encode("utf-8"))

        if result_payload is not None:
            return True, result_payload
        if error_payload is not None:
            return False, error_payload
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout).strip()
            if message:
                return False, message.encode("utf-8")
            return False, b"Prime host runtime setup failed with no output"
        return False, b"Prime host runtime setup did not produce a Coral payload"

    def _wait_host_setup(self, handle: RunHandle) -> tuple[bool, bytes]:
        active = self._wait_for_pod_active(handle.provider_ref)
        ssh_connection = self._get_pod_ssh_connection(handle.provider_ref, active)
        ssh_base = self._ssh_base_command(ssh_connection)
        self._wait_for_ssh_ready(ssh_base)
        return self._run_host_runner_over_ssh(ssh_base, handle.call_id)

    def _result_ref(self, call_id: str) -> str | None:
        if self._result_refs:
            ref = self._result_refs.get(call_id)
            if ref:
                return ref
        if hasattr(self.artifact_store, "gcs"):
            return self.artifact_store.gcs.result_uri(call_id)
        return None

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
            "PYTHONUNBUFFERED": "1",
        }
        if bundle.uri:
            env_vars["CORAL_BUNDLE_URI"] = bundle.uri
        if call_spec.result_ref:
            env_vars["CORAL_RESULT_URI"] = call_spec.result_ref
        env_vars.update(env)

        image_build_disabled = (
            env_vars.get(CORAL_IMAGE_BUILD_DISABLED_ENV) == "1"
            or image.metadata.get(CORAL_IMAGE_BUILD_DISABLED_METADATA) == "1"
        )
        if image_build_disabled and env_vars.get("CORAL_DETACHED") == "1":
            raise RuntimeError("Prime no-image-build runs do not support detached mode")

        offer = self._select_offer()
        custom_template_id = None
        ssh_key_id: str | None = None
        if image_build_disabled:
            ssh_key_id = self._ensure_ssh_key_id()
            pod_image = self._default_offer_image(offer)
        else:
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
                "sshKeyId": ssh_key_id,
                "envVars": (
                    None
                    if image_build_disabled
                    else [{"key": key, "value": value} for key, value in env_vars.items()]
                ),
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
        pod_id = self._pod_id_from_response(response)
        if not pod_id:
            raise RuntimeError(f"Unexpected Prime Intellect response: {response}")
        if image_build_disabled:
            self._store_run_mode(call_spec.call_id, "host_setup")
            internal_env = {
                "CORAL_CALLSPEC_B64",
                "CORAL_RUNTIME_SETUP_B64",
                "CORAL_BUNDLE_URI",
                "CORAL_RESULT_URI",
                "CORAL_IMAGE_BUILD_DISABLED",
                "CORAL_DETACHED",
            }
            user_env = {
                key: value
                for key, value in env_vars.items()
                if key not in internal_env and not key.startswith("CORAL_")
            }
            user_env_b64 = base64.b64encode(
                json.dumps(user_env, sort_keys=True).encode("utf-8")
            ).decode("utf-8")
            self._store_host_execution(
                call_id=call_spec.call_id,
                callspec_b64=call_spec_b64,
                bundle_path=bundle.uri,
                setup_b64=env_vars.get("CORAL_RUNTIME_SETUP_B64", ""),
                user_env_b64=user_env_b64,
            )
        else:
            self._store_run_mode(call_spec.call_id, "image_build")
        if call_spec.result_ref:
            self._store_result_ref(call_spec.call_id, call_spec.result_ref)
        return RunHandle(
            run_id=call_spec.log_labels.get("coral_run_id", ""),
            call_id=call_spec.call_id,
            provider_ref=pod_id,
        )

    def wait(self, handle: RunHandle) -> RunResult:
        self._status("Container running")
        if self._run_mode(handle.call_id) == "host_setup":
            try:
                success, output = self._wait_host_setup(handle)
            except Exception as exc:
                success = False
                output = str(exc).encode("utf-8")
            if success:
                output = base64.b64encode(output)
            return RunResult(call_id=handle.call_id, success=success, output=output)

        last_status = None
        while True:
            status_resp = self.client.get_pods_status([handle.provider_ref])
            entries = self._status_entries(status_resp)
            status = None
            if entries:
                status_raw = entries[0].get("status") or entries[0].get("state")
                if isinstance(status_raw, str):
                    status = status_raw.upper()
            if status and status != last_status:
                self._status(f"Prime {status.lower()}")
                last_status = status
            if status in {"SUCCEEDED", "FAILED", "STOPPED"}:
                break
            time.sleep(5)
        result_ref = self._result_ref(handle.call_id)
        if result_ref is None:
            raise RuntimeError("Missing result reference for Prime Intellect run")
        output = self.artifact_store.get_result(result_ref)
        success = status == "SUCCEEDED"
        return RunResult(call_id=handle.call_id, success=success, output=output)

    def cancel(self, handle: RunHandle) -> None:
        self.client.delete_pod(handle.provider_ref)

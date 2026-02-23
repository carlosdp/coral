from __future__ import annotations

import base64
import json
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests

from coral.image import build_plan, build_plan_hash
from coral.providers.base import ImageRef
from coral.spec import ImageSpec

DOCKER_HUB_API_URL = "https://hub.docker.com/v2"
DEFAULT_DOCKER_REPOSITORY = "coral"


@dataclass
class DockerHubImageBuilder:
    repository: str = DEFAULT_DOCKER_REPOSITORY
    docker_executable: str = "docker"

    def _require_docker_cli(self) -> str:
        docker = shutil.which(self.docker_executable)
        if not docker:
            raise RuntimeError(
                "Docker CLI is required for Coral image builds. Install Docker and retry."
            )
        return docker

    def _dockerhub_servers(self) -> list[str]:
        return [
            "https://index.docker.io/v1/",
            "https://index.docker.io/v1/access-token",
            "https://index.docker.io/v1/refresh-token",
            "docker.io",
            "registry-1.docker.io",
        ]

    def _username_from_credential_helper(self, helper_name: str, server: str) -> str:
        helper_bin = shutil.which(f"docker-credential-{helper_name}")
        if not helper_bin:
            return ""
        completed = subprocess.run(
            [helper_bin, "get"],
            input=f"{server}\n",
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return ""
        try:
            payload = json.loads(completed.stdout.strip() or "{}")
        except json.JSONDecodeError:
            return ""
        return str(payload.get("Username") or "").strip()

    def _docker_config_username(self) -> str:
        config_path = Path.home() / ".docker" / "config.json"
        if not config_path.exists():
            return ""
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""

        auths = data.get("auths")
        if isinstance(auths, dict):
            for server in self._dockerhub_servers():
                entry = auths.get(server)
                if not isinstance(entry, dict):
                    continue
                encoded_auth = str(entry.get("auth") or "").strip()
                if not encoded_auth:
                    continue
                try:
                    decoded = base64.b64decode(encoded_auth).decode("utf-8")
                except Exception:
                    continue
                username = decoded.split(":", 1)[0].strip()
                if username:
                    return username

        cred_helpers = data.get("credHelpers")
        creds_store = str(data.get("credsStore") or "").strip()
        helper_candidates = []
        if isinstance(cred_helpers, dict):
            helper_candidates.extend(
                str(value).strip() for value in cred_helpers.values() if str(value).strip()
            )
        if creds_store:
            helper_candidates.append(creds_store)

        # Keep insertion order while deduplicating helper candidates.
        seen_helpers: dict[str, None] = {}
        for helper_name in helper_candidates:
            seen_helpers.setdefault(helper_name, None)
        for helper_name in seen_helpers:
            for server in self._dockerhub_servers():
                username = self._username_from_credential_helper(helper_name, server)
                if username:
                    return username
        return ""

    def _docker_username(self) -> str:
        docker = self._require_docker_cli()
        completed = subprocess.run(
            [docker, "info", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout).strip()
            if message:
                raise RuntimeError(f"Failed to read Docker login status: {message}")
            raise RuntimeError("Failed to read Docker login status")

        try:
            payload = json.loads(completed.stdout.strip() or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Could not parse Docker info output while checking login status."
            ) from exc

        username = str(payload.get("Username") or "").strip()
        if not username:
            username = self._docker_config_username()
        if not username:
            raise RuntimeError(
                "Docker is not logged in. Run `docker login` before building Coral images."
            )
        return username

    def _image_uri(self, username: str, image_hash: str) -> str:
        return f"docker.io/{username}/{self.repository}:{image_hash}"

    def _docker_hub_tag_url(self, username: str, tag: str) -> str:
        return (
            f"{DOCKER_HUB_API_URL}/namespaces/{username}/repositories/"
            f"{self.repository}/tags/{tag}"
        )

    def _lookup_public_tag(self, username: str, image_hash: str) -> tuple[bool, str]:
        resp = requests.get(
            self._docker_hub_tag_url(username, image_hash),
            timeout=30,
        )
        if resp.status_code == 404:
            return False, ""
        if not resp.ok:
            body = resp.text.strip()
            if len(body) > 500:
                body = body[:500] + "...(truncated)"
            raise RuntimeError(
                "Docker Hub tag lookup failed "
                f"({resp.status_code} {resp.reason}) for {username}/{self.repository}:{image_hash}"
                + (f": {body}" if body else "")
            )
        data = resp.json()
        images = data.get("images") if isinstance(data, dict) else []
        if isinstance(images, list):
            for item in images:
                if not isinstance(item, dict):
                    continue
                digest = str(item.get("digest") or "").strip()
                if digest:
                    return True, digest
        return True, ""

    def _stage_context(self, plan: dict, copy_sources: Iterable[Path]) -> Path:
        context_dir = Path(tempfile.mkdtemp(prefix="coral-build-"))
        runtime_src = Path(__file__).resolve().parents[1] / "coral_runtime"
        runtime_dest = context_dir / "runtime" / "coral_runtime"
        runtime_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(runtime_src, runtime_dest)
        coral_src = Path(__file__).resolve().parents[1] / "coral"
        coral_dest = context_dir / "runtime" / "coral"
        shutil.copytree(coral_src, coral_dest)

        copy_root = context_dir / "copy_src"
        copy_root.mkdir(parents=True, exist_ok=True)
        for src in copy_sources:
            src = Path(src)
            target = copy_root / src.name
            if src.is_dir():
                shutil.copytree(src, target)
            else:
                shutil.copy2(src, target)

        dockerfile = context_dir / "Dockerfile"
        dockerfile.write_text(self._dockerfile(plan, has_copy=bool(copy_sources)))
        return context_dir

    def _dockerfile(self, plan: dict, has_copy: bool) -> str:
        apt = plan["apt_packages"]
        pip = list(dict.fromkeys(plan["pip_packages"] + plan["runtime_requirements"]))
        env_lines = [f"ENV {k}={v}" for k, v in plan["env"].items()]
        lines = [
            f"FROM {plan['base_image']}",
            "ENV PYTHONUNBUFFERED=1",
            "ENV PIP_BREAK_SYSTEM_PACKAGES=1",
            *env_lines,
            f"WORKDIR {plan['workdir']}",
            "COPY runtime/ /opt/coral/runtime/",
            "ENV PYTHONPATH=/opt/coral/runtime",
        ]
        if apt:
            lines.append(
                "RUN apt-get update && apt-get install -y "
                + " ".join(apt)
                + " && rm -rf /var/lib/apt/lists/*"
            )
        if pip:
            lines.append("RUN python -m pip install --no-cache-dir " + " ".join(pip))
        if has_copy:
            lines.append("COPY copy_src/ /opt/coral/src/")
            lines.append("ENV PYTHONPATH=/opt/coral/src:$PYTHONPATH")
        lines.append('ENTRYPOINT ["python", "-m", "coral_runtime.entrypoint"]')
        return "\n".join(lines) + "\n"

    def _build_and_push(self, image_uri: str, context_dir: Path) -> None:
        docker = self._require_docker_cli()
        subprocess.run(
            [docker, "build", "-t", image_uri, "."],
            cwd=context_dir,
            check=True,
        )
        subprocess.run([docker, "push", image_uri], check=True)

    def _inspect_digest(self, image_uri: str) -> str:
        docker = self._require_docker_cli()
        completed = subprocess.run(
            [docker, "image", "inspect", image_uri, "--format", "{{index .RepoDigests 0}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return ""
        repo_digest = completed.stdout.strip()
        if "@" in repo_digest:
            return repo_digest.split("@", 1)[1].strip()
        return ""

    def resolve_image(self, spec: ImageSpec, copy_sources: Iterable[str] | None = None) -> ImageRef:
        image_hash = build_plan_hash(spec)
        username = self._docker_username()
        image_uri = self._image_uri(username, image_hash)
        metadata = {
            "hash": image_hash,
            "docker_user": username,
            "docker_repo": self.repository,
        }

        exists, digest = self._lookup_public_tag(username, image_hash)
        if exists:
            return ImageRef(uri=image_uri, digest=digest, metadata=metadata)

        plan = build_plan(spec)
        copy_paths = [Path(p) for p in (copy_sources or [])]
        context_dir = self._stage_context(plan, copy_paths)
        self._build_and_push(image_uri=image_uri, context_dir=context_dir)

        digest = self._inspect_digest(image_uri)
        # Docker Hub can be eventually consistent immediately after push.
        for _ in range(10):
            exists, remote_digest = self._lookup_public_tag(username, image_hash)
            if exists:
                if remote_digest:
                    digest = remote_digest
                return ImageRef(uri=image_uri, digest=digest, metadata=metadata)
            time.sleep(2)

        raise RuntimeError(
            f"Image {image_uri} was pushed but is not publicly visible on Docker Hub. "
            "Make the repository public and retry."
        )


# Backward-compatible name used by provider modules.
CloudBuildImageBuilder = DockerHubImageBuilder

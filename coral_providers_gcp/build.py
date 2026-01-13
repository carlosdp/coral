from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from google.cloud import artifactregistry_v1, build_v1, storage

from coral.image import build_plan, build_plan_hash
from coral.providers.base import ImageRef
from coral.spec import ImageSpec


@dataclass
class CloudBuildImageBuilder:
    project: str
    region: str
    artifact_repo: str
    gcs_bucket: str

    def _artifact_client(self) -> artifactregistry_v1.ArtifactRegistryClient:
        return artifactregistry_v1.ArtifactRegistryClient()

    def _build_client(self) -> build_v1.CloudBuildClient:
        return build_v1.CloudBuildClient()

    def _storage_client(self) -> storage.Client:
        return storage.Client(project=self.project)

    def _image_uri(self, image_hash: str) -> str:
        return (
            f"{self.region}-docker.pkg.dev/{self.project}/"
            f"{self.artifact_repo}/coral:{image_hash}"
        )

    def _image_digest(self, image_hash: str) -> str | None:
        client = self._artifact_client()
        parent = (
            f"projects/{self.project}/locations/{self.region}/"
            f"repositories/{self.artifact_repo}"
        )
        for image in client.list_docker_images(parent=parent):
            for tag in image.tags:
                if tag.endswith(f":{image_hash}"):
                    return image.name
        return None

    def _stage_context(self, plan: dict, copy_sources: Iterable[Path]) -> Path:
        context_dir = Path(tempfile.mkdtemp(prefix="coral-build-"))
        runtime_src = Path(__file__).resolve().parents[1] / "coral_runtime"
        runtime_dest = context_dir / "runtime" / "coral_runtime"
        runtime_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(runtime_src, runtime_dest)

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
        pip = plan["pip_packages"] + plan["runtime_requirements"]
        env_lines = [f"ENV {k}={v}" for k, v in plan["env"].items()]
        lines = [
            f"FROM {plan['base_image']}",
            "ENV PYTHONUNBUFFERED=1",
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
        lines.append("ENTRYPOINT [\"python\", \"-m\", \"coral_runtime.entrypoint\"]")
        return "\n".join(lines) + "\n"

    def resolve_image(self, spec: ImageSpec, copy_sources: Iterable[str] | None = None) -> ImageRef:
        image_hash = build_plan_hash(spec)
        image_uri = self._image_uri(image_hash)

        digest = self._image_digest(image_hash)
        if digest:
            return ImageRef(uri=image_uri, digest=digest, metadata={"hash": image_hash})

        plan = build_plan(spec)
        copy_sources = [Path(p) for p in (copy_sources or [])]
        context_dir = self._stage_context(plan, copy_sources)

        archive_base = Path(tempfile.mkdtemp(prefix="coral-build-")) / "context"
        archive_path = shutil.make_archive(str(archive_base), "gztar", root_dir=context_dir)

        storage_client = self._storage_client()
        bucket = storage_client.bucket(self.gcs_bucket)
        blob_name = f"coral/builds/{image_hash}.tar.gz"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(archive_path))

        source = build_v1.StorageSource(bucket=self.gcs_bucket, object_=blob_name)
        build = build_v1.Build(
            steps=[
                build_v1.BuildStep(
                    name="gcr.io/cloud-builders/docker",
                    args=["build", "-t", image_uri, "."],
                )
            ],
            images=[image_uri],
            source=build_v1.Source(storage_source=source),
        )
        client = self._build_client()
        operation = client.create_build(project_id=self.project, build=build)
        operation.result()

        digest = self._image_digest(image_hash) or ""
        return ImageRef(uri=image_uri, digest=digest, metadata={"hash": image_hash})

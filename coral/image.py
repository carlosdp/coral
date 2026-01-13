from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Dict, List

from coral.spec import ImageSpec, LocalSource


class Image:
    def __init__(self, spec: ImageSpec):
        self._spec = spec

    @staticmethod
    def python(base: str) -> "Image":
        python_version = "3.11"
        if base.startswith("python:"):
            python_version = base.split(":", 1)[1].split("-")[0]
        return Image(ImageSpec(base_image=base, python_version=python_version))

    def apt_install(self, *packages: str) -> "Image":
        return Image(
            replace(self._spec, apt_packages=self._spec.apt_packages + list(packages))
        )

    def pip_install(self, *packages: str) -> "Image":
        return Image(
            replace(self._spec, pip_packages=self._spec.pip_packages + list(packages))
        )

    def env(self, values: Dict[str, str]) -> "Image":
        merged = dict(self._spec.env)
        merged.update(values)
        return Image(replace(self._spec, env=merged))

    def workdir(self, path: str) -> "Image":
        return Image(replace(self._spec, workdir=path))

    def add_local_python_source(
        self,
        module: str,
        mode: str = "sync",
        ignore: List[str] | None = None,
    ) -> "Image":
        ignore = ignore or []
        sources = self._spec.local_sources + [
            LocalSource(name=module, path="", mode=mode, ignore=ignore)
        ]
        return Image(replace(self._spec, local_sources=sources))

    @property
    def spec(self) -> ImageSpec:
        return self._spec


def build_plan(spec: ImageSpec) -> Dict[str, object]:
    plan = {
        "base_image": spec.base_image,
        "python_version": spec.python_version,
        "apt_packages": spec.apt_packages,
        "pip_packages": spec.pip_packages,
        "env": spec.env,
        "workdir": spec.workdir,
        "local_sources": [
            {"name": src.name, "mode": src.mode, "ignore": src.ignore} for src in spec.local_sources
        ],
        "runtime_requirements": ["cloudpickle", "google-cloud-storage", "requests"],
    }
    return plan


def build_plan_hash(spec: ImageSpec) -> str:
    plan = build_plan(spec)
    payload = json.dumps(plan, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

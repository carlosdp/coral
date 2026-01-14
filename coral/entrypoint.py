from __future__ import annotations

import importlib.util
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from coral import __version__
from coral.app import App
from coral.errors import CoralError
from coral.image import build_plan_hash
from coral.packaging import BundleResult, create_bundle
from coral.providers.base import BundleRef, ImageRef, RunHandle, RunResult
from coral.serialization import SERIALIZATION_VERSION, dumps
from coral.spec import CallSpec, FunctionSpec, ImageSpec

CACHE_DIR = Path.home() / ".coral" / "cache"
BUNDLE_INDEX = CACHE_DIR / "bundles.json"
IMAGE_INDEX = CACHE_DIR / "images.json"


@dataclass
class RunSession:
    provider: object
    app: App
    detached: bool = False
    env: Dict[str, str] | None = None
    verbose: bool = False

    def __post_init__(self):
        self.run_id = uuid.uuid4().hex
        self._bundle_ref: BundleRef | None = None
        self._image_ref: ImageRef | None = None
        self._bundle_result: BundleResult | None = None

    def __enter__(self):
        self.app._set_session(self)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.app._clear_session()

    def _load_index(self, path: Path) -> Dict[str, dict]:
        if not path.exists():
            return {}
        return json.loads(path.read_text())

    def _save_index(self, path: Path, data: Dict[str, dict]) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    def _resolve_local_sources(self, image: ImageSpec) -> Tuple[List[Path], List[Path], List[str]]:
        sync_sources: List[Path] = []
        copy_sources: List[Path] = []
        sync_ignores: List[str] = []
        for src in image.local_sources:
            spec = importlib.util.find_spec(src.name)
            if spec is None:
                raise CoralError(f"Could not resolve local source module '{src.name}'")
            if spec.submodule_search_locations:
                root = Path(list(spec.submodule_search_locations)[0]).resolve()
            elif spec.origin:
                root = Path(spec.origin).resolve()
            else:
                raise CoralError(f"Could not determine source path for '{src.name}'")
            if src.mode == "copy":
                copy_sources.append(root)
            else:
                sync_sources.append(root)
                sync_ignores.extend(src.ignore)
        return sync_sources, copy_sources, sync_ignores

    def _app_source_roots(self, app: App) -> List[Path]:
        roots: List[Path] = []
        if not app.include_source:
            return roots
        for handle in app._functions.values():
            source_file = Path(handle.spec.source_file)
            if source_file.exists():
                roots.append(source_file.parent.resolve())
        return roots

    def _bundle(self, image: ImageSpec) -> BundleRef:
        if self._bundle_ref is not None:
            return self._bundle_ref

        sync_sources, _copy_sources, sync_ignores = self._resolve_local_sources(image)
        roots = self._app_source_roots(self.app) + sync_sources
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Bundling sources:[/info] {', '.join(str(r) for r in roots)}"
            )
        bundle_cache = self._load_index(BUNDLE_INDEX)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        bundle_path = CACHE_DIR / "bundle.tar.gz"
        bundle_result = create_bundle(roots, bundle_path, __version__, extra_ignores=sync_ignores)
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Bundle hash:[/info] {bundle_result.hash}"
            )
        if bundle_result.hash in bundle_cache:
            cached = bundle_cache[bundle_result.hash]
            self._bundle_ref = BundleRef(uri=cached["uri"], hash=bundle_result.hash)
            self._bundle_result = bundle_result
            return self._bundle_ref

        artifact_store = self.provider.get_artifacts()
        bundle_ref = artifact_store.put_bundle(bundle_result.path, bundle_result.hash)
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Uploaded bundle:[/info] {bundle_ref.uri}"
            )
        bundle_cache[bundle_result.hash] = {"uri": bundle_ref.uri}
        self._save_index(BUNDLE_INDEX, bundle_cache)
        self._bundle_ref = bundle_ref
        self._bundle_result = bundle_result
        return bundle_ref

    def _image(self, image: ImageSpec) -> ImageRef:
        if self._image_ref is not None:
            return self._image_ref
        image_hash = build_plan_hash(image)
        if self.verbose:
            from coral.logging import get_console

            get_console().print(f"[info]Image hash:[/info] {image_hash}")
        image_cache = self._load_index(IMAGE_INDEX)
        if image_hash in image_cache:
            cached = image_cache[image_hash]
            self._image_ref = ImageRef(
                uri=cached["uri"],
                digest=cached["digest"],
                metadata=cached.get("metadata", {}),
            )
            if self.verbose:
                from coral.logging import get_console

                get_console().print(
                    f"[info]Using cached image:[/info] {self._image_ref.uri}"
                )
            return self._image_ref

        _sync_sources, copy_sources, _sync_ignores = self._resolve_local_sources(image)
        builder = self.provider.get_builder()
        image_ref = builder.resolve_image(image, copy_sources=copy_sources)
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Built image:[/info] {image_ref.uri}"
            )
        image_cache[image_hash] = {
            "uri": image_ref.uri,
            "digest": image_ref.digest,
            "metadata": image_ref.metadata,
        }
        self._save_index(IMAGE_INDEX, image_cache)
        self._image_ref = image_ref
        return image_ref

    def prepare(self):
        image = self.app.image
        if self.verbose:
            from coral.logging import get_console

            get_console().print("[info]Preparing image and bundle...[/info]")
        self._image(image)
        self._bundle(image)

    def submit(self, spec: FunctionSpec, args: tuple, kwargs: dict) -> RunHandle:
        image = spec.image or self.app.image
        image_ref = self._image(image)
        bundle_ref = self._bundle(image)
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Submitting call:[/info] {spec.module}:{spec.qualname}"
            )

        call_id = uuid.uuid4().hex
        result_uri = self.provider.get_artifacts().result_uri(call_id)
        call_spec = CallSpec(
            call_id=call_id,
            module=spec.module,
            qualname=spec.qualname,
            args_b64=dumps(args),
            kwargs_b64=dumps(kwargs),
            serialization=SERIALIZATION_VERSION,
            result_ref=result_uri,
            stdout_mode="stream",
            log_labels={
                "coral_run_id": self.run_id,
                "coral_app": self.app.name,
                "coral_call_id": call_id,
            },
        )
        env = dict(self.env or {})
        labels = {
            "coral_run_id": self.run_id,
            "coral_app": self.app.name,
            "coral_call_id": call_id,
        }
        handle = self.provider.get_executor().submit(
            call_spec,
            image_ref,
            bundle_ref,
            spec.resources,
            env,
            labels,
        )
        return handle

    def wait(self, handle: RunHandle) -> RunResult:
        result = self.provider.get_executor().wait(handle)
        if not self.detached:
            self.provider.get_cleanup().cleanup(handle, detached=self.detached)
        return result

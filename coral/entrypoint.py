from __future__ import annotations

import importlib.util
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from coral import __version__
from coral.app import App
from coral.errors import CoralError
from coral.image import build_plan_hash
from coral.packaging import BundleResult, create_bundle
from coral.providers.base import BundleRef, ImageRef, RunHandle, RunResult
from coral.runtime_setup import (
    CORAL_IMAGE_BUILD_DISABLED_ENV,
    CORAL_IMAGE_BUILD_DISABLED_METADATA,
    CORAL_RUNTIME_SETUP_B64_ENV,
    encode_runtime_setup_payload,
)
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
    no_cache: bool = False
    status_cb: Callable[[str], None] | None = None

    def __post_init__(self):
        self.run_id = uuid.uuid4().hex
        self._bundle_refs: Dict[str, BundleRef] = {}
        self._bundle_results: Dict[str, BundleResult] = {}
        self._image_refs: Dict[str, ImageRef] = {}

    def __enter__(self):
        self.app._set_session(self)
        if hasattr(self.provider, "set_status_callback"):
            self.provider.set_status_callback(self._status)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.app._clear_session()

    def _status(self, message: str) -> None:
        if self.status_cb:
            self.status_cb(message)

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

    def _bundle(
        self,
        image: ImageSpec,
        include_copy_sources: bool = False,
        upload: bool = True,
        extra_roots: List[Path] | None = None,
    ) -> BundleRef:
        mode = "copy" if include_copy_sources else "sync"
        storage_mode = "upload" if upload else "local"
        extra_root_key = ",".join(
            sorted(str(root.resolve()) for root in (extra_roots or []))
        )
        cache_key = f"{build_plan_hash(image)}:{mode}:{storage_mode}:{extra_root_key}"
        if cache_key in self._bundle_refs:
            return self._bundle_refs[cache_key]

        sync_sources, copy_sources, sync_ignores = self._resolve_local_sources(image)
        roots = self._app_source_roots(self.app) + sync_sources
        if include_copy_sources:
            roots += copy_sources
        if extra_roots:
            roots += [root.resolve() for root in extra_roots]
        self._status("Uploading files")
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
        if not upload:
            local_ref = BundleRef(uri=bundle_result.path, hash=bundle_result.hash)
            self._bundle_refs[cache_key] = local_ref
            self._bundle_results[cache_key] = bundle_result
            self._status("Prepared local bundle")
            return local_ref
        if not self.no_cache and bundle_result.hash in bundle_cache:
            cached = bundle_cache[bundle_result.hash]
            self._status("Using cached bundle")
            self._bundle_refs[cache_key] = BundleRef(uri=cached["uri"], hash=bundle_result.hash)
            self._bundle_results[cache_key] = bundle_result
            return self._bundle_refs[cache_key]

        artifact_store = self.provider.get_artifacts()
        bundle_ref = artifact_store.put_bundle(bundle_result.path, bundle_result.hash)
        self._status("Uploaded files")
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Uploaded bundle:[/info] {bundle_ref.uri}"
            )
        bundle_cache[bundle_result.hash] = {"uri": bundle_ref.uri}
        self._save_index(BUNDLE_INDEX, bundle_cache)
        self._bundle_refs[cache_key] = bundle_ref
        self._bundle_results[cache_key] = bundle_result
        return bundle_ref

    def _image(self, image: ImageSpec) -> ImageRef:
        image_hash = build_plan_hash(image)
        if image_hash in self._image_refs:
            return self._image_refs[image_hash]
        self._status("Resolving image")
        if self.verbose:
            from coral.logging import get_console

            get_console().print(f"[info]Image hash:[/info] {image_hash}")

        _sync_sources, copy_sources, _sync_ignores = self._resolve_local_sources(image)
        builder = self.provider.get_builder()
        image_ref = builder.resolve_image(image, copy_sources=copy_sources)
        if getattr(self.provider, "name", "") == "prime":
            ensure_template = getattr(self.provider, "ensure_custom_template", None)
            if callable(ensure_template):
                self._status("Syncing Prime template")
                template_id = ensure_template(image_ref)
                metadata = dict(image_ref.metadata)
                metadata["prime_custom_template_id"] = template_id
                image_ref = ImageRef(uri=image_ref.uri, digest=image_ref.digest, metadata=metadata)

        self._status("Image ready")
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Resolved image:[/info] {image_ref.uri}"
            )
            template_id = image_ref.metadata.get("prime_custom_template_id")
            if template_id:
                get_console().print(
                    f"[info]Prime custom template:[/info] {template_id}"
                )
        image_cache = self._load_index(IMAGE_INDEX)
        image_cache[image_hash] = {
            "uri": image_ref.uri,
            "digest": image_ref.digest,
            "metadata": image_ref.metadata,
        }
        self._save_index(IMAGE_INDEX, image_cache)
        self._image_refs[image_hash] = image_ref
        return image_ref

    def _default_runtime_image(self) -> ImageRef:
        return ImageRef(
            uri="",
            digest="",
            metadata={CORAL_IMAGE_BUILD_DISABLED_METADATA: "1"},
        )

    def prepare(self):
        image = self.app.image
        self._status("Preparing image and bundle")
        if self.verbose:
            from coral.logging import get_console

            get_console().print("[info]Preparing image and bundle...[/info]")
        self._image(image)
        upload_bundle = getattr(self.provider, "name", "") != "prime"
        self._bundle(image, upload=upload_bundle)

    def prepare_image(self) -> ImageRef:
        image = self.app.image
        self._status("Preparing image")
        if self.verbose:
            from coral.logging import get_console

            get_console().print("[info]Preparing image...[/info]")
        return self._image(image)

    def submit(self, spec: FunctionSpec, args: tuple, kwargs: dict) -> RunHandle:
        image = spec.image or self.app.image
        provider_name = getattr(self.provider, "name", "")
        prime_no_build = provider_name == "prime" and not spec.build_image
        if self.detached and prime_no_build:
            raise CoralError(
                "Prime detached runs are not supported when build_image is disabled."
            )
        image_ref = self._image(image) if spec.build_image else self._default_runtime_image()
        extra_bundle_roots: List[Path] = []
        if prime_no_build:
            # Prime no-build execution imports app modules directly on the host.
            # Include the Coral SDK source so `import coral` resolves consistently.
            extra_bundle_roots.append(Path(__file__).resolve().parent)
        bundle_ref = self._bundle(
            image,
            include_copy_sources=not spec.build_image,
            upload=provider_name != "prime" and not prime_no_build,
            extra_roots=extra_bundle_roots,
        )
        self._status("Spawning container")
        if self.verbose:
            from coral.logging import get_console

            get_console().print(
                f"[info]Submitting call:[/info] {spec.module}:{spec.qualname}"
            )

        call_id = uuid.uuid4().hex
        result_uri = ""
        if provider_name != "prime" and not prime_no_build:
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
        if not spec.build_image:
            runtime_env = dict(image.env)
            runtime_env.update(env)
            env = runtime_env
            env[CORAL_IMAGE_BUILD_DISABLED_ENV] = "1"
            env[CORAL_RUNTIME_SETUP_B64_ENV] = encode_runtime_setup_payload(image)
        if self.verbose:
            env["CORAL_VERBOSE"] = "1"
        if self.detached:
            env["CORAL_DETACHED"] = "1"
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
        self._status("Container running")
        result = self.provider.get_executor().wait(handle)
        if not self.detached:
            self.provider.get_cleanup().cleanup(handle, detached=self.detached)
        self._status("Completed")
        return result

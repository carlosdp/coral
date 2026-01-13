from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from coral.errors import CoralError
from coral.serialization import loads
from coral.spec import AppSpec, FunctionSpec, ImageSpec, ResourceSpec

_REGISTERED_APPS = []


def get_registered_apps():
    return list(_REGISTERED_APPS)


@dataclass
class FunctionHandle:
    name: str
    spec: FunctionSpec
    app: "App"

    def remote(self, *args, **kwargs):
        session = self.app._require_session()
        handle = session.submit(self.spec, args, kwargs)
        result = session.wait(handle)
        if result.success:
            return loads(result.output.decode("utf-8"))
        raise CoralError(result.output.decode("utf-8"))

    def spawn(self, *args, **kwargs):
        session = self.app._require_session()
        return session.submit(self.spec, args, kwargs)


class App:
    def __init__(
        self,
        name: str,
        image: Optional[ImageSpec] = None,
        include_source: bool = True,
    ):
        from coral.image import Image

        image_spec = image.spec if isinstance(image, Image) else image
        self._spec = AppSpec(
            name=name,
            image=image_spec or ImageSpec(base_image="python:3.11-slim"),
            include_source=include_source,
        )
        self._functions: Dict[str, FunctionHandle] = {}
        self._local_entrypoints: Dict[str, Callable] = {}
        self._session = None
        _REGISTERED_APPS.append(self)

    @property
    def name(self) -> str:
        return self._spec.name

    @property
    def image(self) -> ImageSpec:
        return self._spec.image

    @property
    def include_source(self) -> bool:
        return self._spec.include_source

    def _require_session(self):
        if self._session is None:
            raise CoralError("No active RunSession. Use coral.run or app.run to create one.")
        return self._session

    def function(
        self,
        *,
        cpu: int = 1,
        memory: str = "2Gi",
        gpu: Optional[str] = None,
        timeout: int = 3600,
        retries: int = 0,
        image: Optional[ImageSpec] = None,
    ):
        def decorator(fn: Callable):
            module = fn.__module__
            qualname = fn.__qualname__
            source_file = inspect.getsourcefile(fn) or ""
            resources = ResourceSpec(
                cpu=cpu,
                memory=memory,
                gpu=gpu,
                timeout=timeout,
                retries=retries,
            )
            spec = FunctionSpec(
                name=fn.__name__,
                module=module,
                qualname=qualname,
                source_file=source_file,
                resources=resources,
                image=image,
            )
            handle = FunctionHandle(name=fn.__name__, spec=spec, app=self)
            self._functions[fn.__name__] = handle
            return handle

        return decorator

    def local_entrypoint(self):
        def decorator(fn: Callable):
            self._local_entrypoints[fn.__name__] = fn
            return fn

        return decorator

    def _set_session(self, session):
        self._session = session

    def _clear_session(self):
        self._session = None

    def get_function(self, name: str) -> FunctionHandle:
        if name not in self._functions:
            raise CoralError(f"Function '{name}' not found in app '{self.name}'")
        return self._functions[name]

    def get_entrypoint(self, name: str) -> Callable:
        if name not in self._local_entrypoints:
            raise CoralError(f"Entrypoint '{name}' not found in app '{self.name}'")
        return self._local_entrypoints[name]

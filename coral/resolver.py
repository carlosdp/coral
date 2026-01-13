from __future__ import annotations

import importlib
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import List, Optional, Tuple

from coral.app import get_registered_apps
from coral.errors import ResolverError


@dataclass(frozen=True)
class ResolvedTarget:
    module: ModuleType
    target: Optional[str]


@dataclass(frozen=True)
class ResolvedApp:
    app: object
    local_entrypoints: List[str]
    functions: List[str]


def parse_func_ref(ref: str) -> Tuple[str, Optional[str], bool]:
    if "::" in ref:
        path_or_module, target = ref.split("::", 1)
    else:
        path_or_module, target = ref, None
    is_module = False
    if path_or_module.endswith(".py") or Path(path_or_module).exists():
        return path_or_module, target, False
    is_module = True
    return path_or_module, target, is_module


def load_module(path_or_module: str, is_module: bool) -> ModuleType:
    if is_module:
        return importlib.import_module(path_or_module)
    path = Path(path_or_module).resolve()
    if not path.exists():
        raise ResolverError(f"File not found: {path}")
    sys.path.insert(0, str(path.parent))
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ResolverError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def resolve(ref: str) -> ResolvedTarget:
    path_or_module, target, is_module = parse_func_ref(ref)
    module = load_module(path_or_module, is_module)
    return ResolvedTarget(module=module, target=target)


def discover_apps() -> List[ResolvedApp]:
    apps = []
    for app in get_registered_apps():
        entrypoints = list(app._local_entrypoints.keys())
        functions = list(app._functions.keys())
        apps.append(ResolvedApp(app=app, local_entrypoints=entrypoints, functions=functions))
    return apps

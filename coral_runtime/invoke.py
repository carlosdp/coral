from __future__ import annotations

import importlib
import traceback

from coral_runtime.serialization import loads


def invoke(module: str, qualname: str, args_b64: str, kwargs_b64: str) -> tuple[bool, bytes]:
    try:
        mod = importlib.import_module(module)
        obj = mod
        for part in qualname.split("."):
            obj = getattr(obj, part)
        args = loads(args_b64)
        kwargs = loads(kwargs_b64)
        result = obj(*args, **kwargs)
        return True, result
    except Exception:
        return False, traceback.format_exc().encode("utf-8")

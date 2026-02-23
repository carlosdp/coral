from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from coral.providers.base import BundleRef


@dataclass
class PrimeArtifactStore:
    root: Path = Path.home() / ".coral" / "cache" / "prime"

    def _bundle_path(self, bundle_hash: str) -> Path:
        return self.root / "bundles" / f"{bundle_hash}.tar.gz"

    def _result_path(self, call_id: str) -> Path:
        return self.root / "results" / f"{call_id}.bin"

    def put_bundle(self, bundle_path: str, bundle_hash: str) -> BundleRef:
        dst = self._bundle_path(bundle_hash)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copyfile(bundle_path, dst)
        return BundleRef(uri=str(dst), hash=bundle_hash)

    def result_uri(self, call_id: str) -> str:
        path = self._result_path(call_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)

    def get_result(self, result_ref: str) -> bytes:
        return Path(result_ref).read_bytes()

    def signed_url(self, uri: str, ttl_seconds: int, method: str = "GET") -> Optional[str]:
        return uri

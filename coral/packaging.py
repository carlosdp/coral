from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import pathspec

from coral.errors import PackagingError

DEFAULT_IGNORES = [
    ".git",
    ".venv",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    ".pytest_cache",
    "build",
    "dist",
]


@dataclass(frozen=True)
class BundleResult:
    path: str
    hash: str
    manifest: dict


def _load_ignore_patterns(root: Path, extra: Iterable[str] | None = None) -> List[str]:
    patterns = list(DEFAULT_IGNORES)
    if extra:
        patterns.extend(extra)
    for name in [".gitignore", ".coralignore"]:
        ignore_path = root / name
        if ignore_path.exists():
            patterns.extend(ignore_path.read_text().splitlines())
    return [p for p in patterns if p and not p.strip().startswith("#")]


def _spec_for_root(root: Path, extra: Iterable[str] | None = None) -> pathspec.PathSpec:
    patterns = _load_ignore_patterns(root, extra)
    return pathspec.PathSpec.from_lines("gitwildmatch", patterns)


def _iter_files(root: Path, spec: pathspec.PathSpec) -> Iterable[Tuple[Path, str]]:
    if root.is_file():
        rel = root.name
        if not spec.match_file(rel):
            yield root, rel
        return
    base = root
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(base)
        dirnames[:] = [
            d
            for d in dirnames
            if not spec.match_file(str(rel_dir / d))
        ]
        for filename in filenames:
            rel_path = rel_dir / filename
            if spec.match_file(str(rel_path)):
                continue
            yield Path(dirpath) / filename, str(rel_path)


def create_bundle(
    roots: Iterable[Path],
    output_path: Path,
    version: str,
    extra_ignores: Iterable[str] | None = None,
) -> BundleResult:
    roots = [root.resolve() for root in roots]
    if not roots:
        raise PackagingError("No source roots to bundle")

    manifest = {
        "version": version,
        "python": f"{os.sys.version_info.major}.{os.sys.version_info.minor}",
        "roots": [str(r) for r in roots],
        "ignore": DEFAULT_IGNORES + list(extra_ignores or []),
    }
    manifest_json = json.dumps(manifest, sort_keys=True).encode("utf-8")

    file_entries: List[Tuple[str, Path]] = []
    for root in roots:
        spec = _spec_for_root(root, extra=extra_ignores)
        for file_path, rel in _iter_files(root, spec):
            tar_path = str(Path(root.name) / rel) if root.is_dir() else str(Path(root.name))
            file_entries.append((tar_path, file_path))
    file_entries.sort(key=lambda item: item[0])

    with tarfile.open(output_path, "w:gz") as tar:
        for tar_path, file_path in file_entries:
            data = file_path.read_bytes()
            info = tarfile.TarInfo(name=tar_path)
            info.size = len(data)
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(data))

        manifest_info = tarfile.TarInfo(name="coral_manifest.json")
        manifest_info.size = len(manifest_json)
        manifest_info.mtime = 0
        manifest_info.uid = 0
        manifest_info.gid = 0
        manifest_info.uname = "root"
        manifest_info.gname = "root"
        manifest_info.mode = 0o644
        tar.addfile(manifest_info, io.BytesIO(manifest_json))

    tar_bytes = output_path.read_bytes()
    bundle_hash = hashlib.sha256(tar_bytes + manifest_json).hexdigest()
    return BundleResult(path=str(output_path), hash=bundle_hash, manifest=manifest)

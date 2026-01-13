from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path

from coral.packaging import create_bundle
from coral.version import __version__


def test_sync_bundle_respects_coralignore() -> None:
    root = Path(tempfile.mkdtemp())
    pkg = root / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("value = 42\n")
    (pkg / "ignore.me").write_text("ignored\n")
    (pkg / ".coralignore").write_text("ignore.me\n")

    bundle_path = root / "bundle.tar.gz"
    result = create_bundle([pkg], bundle_path, __version__)

    with tarfile.open(bundle_path, "r:gz") as tar:
        names = tar.getnames()

    assert result.hash
    assert "pkg/__init__.py" in names
    assert "pkg/ignore.me" not in names

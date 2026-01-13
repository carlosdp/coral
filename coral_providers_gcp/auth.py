from __future__ import annotations

from google.auth import default


def get_credentials():
    credentials, _project = default()
    return credentials

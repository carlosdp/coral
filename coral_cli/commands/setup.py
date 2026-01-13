from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import typer
from google.auth.transport.requests import Request

from coral.config import get_profile, load_config, write_config
from coral.errors import ConfigError
from coral.logging import get_console

app = typer.Typer(help="Authenticate and set up provider credentials")

DEVICE_ENDPOINT = "https://oauth2.googleapis.com/device/code"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


@dataclass
class DeviceCodeResponse:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str | None
    expires_in: int
    interval: int


def _get_client_config(profile_data: Dict[str, Any]) -> tuple[str, str | None]:
    client_id = (
        profile_data.get("oauth_client_id")
        or os.environ.get("CORAL_GCP_OAUTH_CLIENT_ID")
    )
    client_secret = (
        profile_data.get("oauth_client_secret")
        or os.environ.get("CORAL_GCP_OAUTH_CLIENT_SECRET")
    )
    if not client_id:
        raise ConfigError(
            "Missing OAuth client_id. Set profile.gcp.oauth_client_id or "
            "CORAL_GCP_OAUTH_CLIENT_ID."
        )
    return client_id, client_secret


def _fetch_device_code(client_id: str, scope: str) -> DeviceCodeResponse:
    resp = requests.post(
        DEVICE_ENDPOINT,
        data={"client_id": client_id, "scope": scope},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    return DeviceCodeResponse(
        device_code=payload["device_code"],
        user_code=payload["user_code"],
        verification_uri=payload.get("verification_uri") or payload.get("verification_url"),
        verification_uri_complete=payload.get("verification_uri_complete"),
        expires_in=payload.get("expires_in", 900),
        interval=payload.get("interval", 5),
    )


def _poll_token(
    client_id: str,
    client_secret: str | None,
    device_code: str,
    interval: int,
    expires_in: int,
) -> Dict[str, Any]:
    deadline = time.time() + expires_in
    while time.time() < deadline:
        data = {
            "client_id": client_id,
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }
        if client_secret:
            data["client_secret"] = client_secret
        resp = requests.post(TOKEN_ENDPOINT, data=data, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        payload = resp.json()
        error = payload.get("error")
        if error == "authorization_pending":
            time.sleep(interval)
            continue
        if error == "slow_down":
            interval += 5
            time.sleep(interval)
            continue
        if error in {"access_denied", "expired_token"}:
            raise RuntimeError(f"OAuth device flow failed: {error}")
        raise RuntimeError(f"OAuth device flow error: {payload}")
    raise RuntimeError("OAuth device flow timed out")


def _store_credentials(
    client_id: str,
    client_secret: str | None,
    refresh_token: str,
    credentials_path: Path,
) -> None:
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "type": "authorized_user",
        "client_id": client_id,
        "client_secret": client_secret or "",
        "refresh_token": refresh_token,
    }
    credentials_path.write_text(  # type: ignore[call-arg]
        __import__("json").dumps(data, indent=2)
    )


def _credentials_valid() -> bool:
    try:
        creds, _ = __import__("google.auth").default()
        if creds.valid:
            return True
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            return True
        return False
    except Exception:
        return False


def _update_profile_credentials(profile: str, credentials_path: Path) -> None:
    data = load_config()
    profiles = data.setdefault("profile", {})
    profile_data = profiles.setdefault(profile, {})
    provider = profile_data.get("provider", "gcp")
    profile_data["provider"] = provider
    provider_data = profile_data.setdefault(provider, {})
    provider_data["credentials_path"] = str(credentials_path)
    write_config(data)


@app.command()
def main(
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
    scope: str = typer.Option(DEFAULT_SCOPE, "--scope", help="OAuth scope"),
):
    console = get_console()
    if _credentials_valid():
        console.print("[success]GCP credentials already valid.[/success]")
        return

    profile_obj = get_profile(profile)
    if profile_obj.provider != "gcp":
        raise ConfigError("coral setup currently supports only GCP profiles")

    client_id, client_secret = _get_client_config(profile_obj.data)
    device = _fetch_device_code(client_id, scope)

    console.print("[info]Open this URL to authorize:[/info]")
    if device.verification_uri_complete:
        console.print(device.verification_uri_complete)
    else:
        console.print(device.verification_uri)
        console.print(f"[info]Enter code:[/info] {device.user_code}")

    token_payload = _poll_token(
        client_id=client_id,
        client_secret=client_secret,
        device_code=device.device_code,
        interval=device.interval,
        expires_in=device.expires_in,
    )

    refresh_token = token_payload.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("No refresh token returned; cannot persist credentials")

    credentials_path = Path.home() / ".coral" / "credentials.json"
    _store_credentials(client_id, client_secret, refresh_token, credentials_path)
    _update_profile_credentials(profile_obj.name, credentials_path)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)
    console.print(f"[success]Saved credentials to[/success] {credentials_path}")
    console.print("[success]Updated config with credentials_path.[/success]")

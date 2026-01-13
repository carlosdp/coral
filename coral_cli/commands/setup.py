from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional

import typer
from google.auth.transport.requests import Request

from coral.config import get_profile
from coral.errors import ConfigError
from coral.logging import get_console

app = typer.Typer(help="Authenticate and set up provider credentials")


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


def _run_gcloud_adc_login() -> None:
    subprocess.run(
        ["gcloud", "auth", "application-default", "login"],
        check=True,
    )


def _adc_file_exists() -> bool:
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path:
        return Path(env_path).expanduser().exists()
    default_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    return default_path.exists()


@app.command()
def main(
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
):
    console = get_console()
    profile_obj = get_profile(profile)
    if profile_obj.provider != "gcp":
        raise ConfigError("coral setup currently supports only GCP profiles")

    if _credentials_valid():
        console.print("[success]GCP credentials already valid.[/success]")
        return

    console.print("[info]Launching gcloud Application Default Credentials login...[/info]")
    _run_gcloud_adc_login()

    if _credentials_valid():
        console.print("[success]GCP credentials are now valid.[/success]")
        return
    if _adc_file_exists():
        console.print(
            "[warn]ADC file created, but credential validation failed. "
            "Proceeding; try again if API calls fail.[/warn]"
        )
        return

    raise RuntimeError(
        "ADC login completed but credentials are still invalid. "
        "Check gcloud auth configuration."
    )

from __future__ import annotations

from typing import Optional

import typer

from coral.config import get_profile
from coral.logging import get_console
from coral.providers import registry
from coral.providers.base import RunHandle

app = typer.Typer(help="Stream logs for a run")


@app.callback(invoke_without_command=True)
def main(
    run_id: str = typer.Argument(..., help="Run ID"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider name"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
):
    console = get_console()
    profile_data = get_profile(profile)
    provider_name = provider or profile_data.provider
    provider_obj = registry.load(provider_name)
    if hasattr(provider_obj, "configure"):
        provider_obj.configure(profile_data)

    handle = RunHandle(run_id=run_id, call_id="", provider_ref=run_id)
    for line in provider_obj.get_log_streamer().stream(handle):
        console.print(line)

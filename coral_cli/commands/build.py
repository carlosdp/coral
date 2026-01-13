from __future__ import annotations

from typing import Optional

import typer

from coral.config import get_profile
from coral.entrypoint import RunSession
from coral.errors import CoralError
from coral.logging import get_console
from coral.providers import registry
from coral.resolver import discover_apps, load_module, parse_func_ref

app = typer.Typer(help="Build images and bundles")


def _select_app():
    apps = discover_apps()
    if not apps:
        raise CoralError("No coral.App instances found in the module")
    if len(apps) > 1:
        raise CoralError("Multiple apps found. Please import a single app per module for now.")
    return apps[0].app


@app.command()
def main(
    ref: str = typer.Argument(..., help="Function reference"),
    module: bool = typer.Option(False, "-m", help="Treat ref as module path"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider name"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
):
    console = get_console()
    profile_data = get_profile(profile)
    provider_name = provider or profile_data.provider
    provider_obj = registry.load(provider_name)
    if hasattr(provider_obj, "configure"):
        provider_obj.configure(profile_data)

    if module:
        if "::" in ref:
            module_path, _target = ref.split("::", 1)
        else:
            module_path = ref
        load_module(module_path, True)
    else:
        path_or_module, _target, is_module = parse_func_ref(ref)
        load_module(path_or_module, is_module)

    app_obj = _select_app()
    with RunSession(provider=provider_obj, app=app_obj, detached=True) as session:
        session.prepare()
    console.print("[success]Build complete[/success]")

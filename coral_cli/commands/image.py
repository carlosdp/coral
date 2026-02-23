from __future__ import annotations

import os
from typing import Optional

import typer

from coral.config import Profile, get_profile, load_config
from coral.entrypoint import RunSession
from coral.errors import CoralError
from coral.logging import get_console
from coral.providers import registry
from coral.resolver import discover_apps, load_module, parse_func_ref

app = typer.Typer(help="Build/deploy images for a script")


def _select_app():
    apps = discover_apps()
    if not apps:
        raise CoralError("No coral.App instances found in the module")
    if len(apps) > 1:
        raise CoralError("Multiple apps found. Please import a single app per module for now.")
    return apps[0].app


def _selected_profile(profile: Optional[str], provider: Optional[str]) -> Profile:
    selected = get_profile(profile)
    if not provider or provider == selected.provider:
        return selected

    config = load_config()
    profile_name = profile or os.environ.get("CORAL_PROFILE", "default")
    profile_block = config.get("profile", {}).get(profile_name, {})
    provider_block = profile_block.get(provider)
    if provider_block is None:
        raise CoralError(
            f"Profile '{profile_name}' does not define provider section '{provider}'."
        )
    return Profile(name=profile_name, provider=provider, data=provider_block)


@app.callback(invoke_without_command=True)
def main(
    ref: str = typer.Argument(..., help="Function reference"),
    module: bool = typer.Option(False, "-m", help="Treat ref as module path"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider name"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
):
    console = get_console()
    profile_data = _selected_profile(profile, provider)
    provider_name = profile_data.provider
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
        image_ref = session.prepare_image()

    console.print(f"[success]Image ready:[/success] {image_ref.uri}")
    template_id = image_ref.metadata.get("prime_custom_template_id")
    if template_id:
        console.print(f"[success]Prime template ready:[/success] {template_id}")

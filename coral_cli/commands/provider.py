from __future__ import annotations

import typer

from coral.logging import get_console
from coral.providers import registry

app = typer.Typer(help="Provider information")


@app.command("list")
def list_providers():
    console = get_console()
    providers = registry.available_providers()
    for name in sorted(providers.keys()):
        console.print(f"- {name}")


@app.command("info")
def provider_info(name: str = typer.Argument(..., help="Provider name")):
    console = get_console()
    provider = registry.load(name)
    console.print(f"Provider: {provider.name}")

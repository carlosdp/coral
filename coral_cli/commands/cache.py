from __future__ import annotations

import shutil
from pathlib import Path

import typer

from coral.logging import get_console

app = typer.Typer(help="Manage Coral caches")

CACHE_DIR = Path.home() / ".coral" / "cache"


@app.command("clear")
def clear_cache() -> None:
    console = get_console()
    if not CACHE_DIR.exists():
        console.print("[info]No cache directory found.[/info]")
        return
    shutil.rmtree(CACHE_DIR)
    console.print(f"[success]Cleared cache at[/success] {CACHE_DIR}")

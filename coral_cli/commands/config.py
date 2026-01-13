from __future__ import annotations

import typer

from coral.config import CONFIG_PATH, save_config
from coral.logging import get_console

app = typer.Typer(help="Manage Coral configuration")


@app.command("init")
def init_config():
    console = get_console()
    save_config({})
    console.print(f"[success]Wrote config template to[/success] {CONFIG_PATH}")


@app.command("set")
def set_config(key: str = typer.Argument(...), value: str = typer.Argument(...)):
    console = get_console()
    if not CONFIG_PATH.exists():
        console.print("[error]Config file not found. Run 'coral config init' first.[/error]")
        raise typer.Exit(1)

    text = CONFIG_PATH.read_text()
    text += f"\n# Added by coral config set\n{key} = \"{value}\"\n"
    CONFIG_PATH.write_text(text)
    console.print(f"[success]Updated[/success] {CONFIG_PATH}")

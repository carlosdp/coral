from rich.console import Console
from rich.theme import Theme

DEFAULT_THEME = Theme(
    {
        "info": "bold cyan",
        "warn": "bold yellow",
        "error": "bold red",
        "success": "bold green",
    }
)


_console = Console(theme=DEFAULT_THEME)


def get_console() -> Console:
    return _console

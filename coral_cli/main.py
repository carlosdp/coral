import typer

from coral_cli.commands import build, config, logs, provider, run, setup, stop

app = typer.Typer(help="Coral SDK CLI")
app.add_typer(run.app, name="run")
app.add_typer(build.app, name="build")
app.add_typer(logs.app, name="logs")
app.add_typer(stop.app, name="stop")
app.add_typer(provider.app, name="providers")
app.add_typer(config.app, name="config")
app.add_typer(setup.app, name="setup")

if __name__ == "__main__":
    app()

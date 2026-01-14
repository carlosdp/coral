from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from coral.app import App
from coral.config import get_profile
from coral.entrypoint import RunSession
from coral.errors import CoralError
from coral.logging import get_console
from coral.providers import registry
from coral.resolver import discover_apps, load_module, parse_func_ref
from coral.spec import FunctionSpec, ResourceSpec

app = typer.Typer(help="Run Coral apps and functions")


def _select_app() -> App:
    apps = discover_apps()
    if not apps:
        raise CoralError("No coral.App instances found in the module")
    if len(apps) > 1:
        raise CoralError("Multiple apps found. Please import a single app per module for now.")
    return apps[0].app


def _parse_env(values: List[str]) -> dict:
    env = {}
    for item in values:
        if "=" not in item:
            raise CoralError(f"Invalid env format: {item}")
        key, value = item.split("=", 1)
        env[key] = value
    return env


@app.callback(invoke_without_command=True)
def main(
    ref: str = typer.Argument(..., help="Function reference, e.g. path/to/file.py::func"),
    args: List[str] = typer.Argument([], help="Arguments passed to the function"),
    module: bool = typer.Option(False, "-m", help="Treat ref as module path"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider name"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
    detach: bool = typer.Option(False, "--detach", help="Do not wait for completion"),
    write_result: Optional[Path] = typer.Option(
        None,
        "--write-result",
        help="Write result bytes to file",
    ),
    env: List[str] = typer.Option([], "--env", help="Extra env vars KEY=VALUE"),
    gpu: Optional[str] = typer.Option(None, "--gpu", help="Override GPU spec (e.g. A100:1)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    console = get_console()
    profile_data = get_profile(profile)
    provider_name = provider or profile_data.provider
    provider_obj = registry.load(provider_name)
    if hasattr(provider_obj, "configure"):
        provider_obj.configure(profile_data)

    if module:
        if "::" in ref:
            module_path, target = ref.split("::", 1)
        else:
            module_path, target = ref, None
        load_module(module_path, True)
        target_name = target
    else:
        path_or_module, target_name, is_module = parse_func_ref(ref)
        load_module(path_or_module, is_module)

    app_obj = _select_app()
    env_vars = _parse_env(env)

    with RunSession(
        provider=provider_obj,
        app=app_obj,
        detached=detach,
        env=env_vars,
        verbose=verbose,
    ) as session:
        if target_name:
            if target_name in app_obj._local_entrypoints:
                console.print(f"[info]Running local entrypoint {target_name}[/info]")
                app_obj.get_entrypoint(target_name)(*args)
                return
            if target_name in app_obj._functions:
                handle = app_obj.get_function(target_name)
                if gpu:
                    handle.spec = FunctionSpec(
                        name=handle.spec.name,
                        module=handle.spec.module,
                        qualname=handle.spec.qualname,
                        source_file=handle.spec.source_file,
                        resources=ResourceSpec(
                            cpu=handle.spec.resources.cpu,
                            memory=handle.spec.resources.memory,
                            gpu=gpu,
                            timeout=handle.spec.resources.timeout,
                            retries=handle.spec.resources.retries,
                        ),
                        image=handle.spec.image,
                    )
                if detach:
                    run_handle = handle.spawn(*args)
                    console.print(f"[success]Run submitted:[/success] {run_handle.run_id}")
                    return
                if write_result:
                    run_handle = session.submit(handle.spec, tuple(args), {})
                    result = session.wait(run_handle)
                    if write_result:
                        write_result.write_bytes(result.output)
                    console.print(f"[success]Run finished:[/success] {result.success}")
                    return
                handle.remote(*args)
                console.print("[success]Run finished:[/success] success")
                return
            raise CoralError(f"Target '{target_name}' not found in app")

        if len(app_obj._local_entrypoints) == 1:
            entrypoint = list(app_obj._local_entrypoints.values())[0]
            console.print(f"[info]Running default local entrypoint {entrypoint.__name__}[/info]")
            entrypoint(*args)
            return

        raise CoralError("Multiple entrypoints found; please specify ::entrypoint")

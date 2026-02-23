# Coral

Coral is a Python SDK + CLI for running Python functions remotely on cloud providers. This repository contains the core SDK, runtime, CLI, and the initial GCP provider implementation.

## Quickstart (local dev)

```bash
uv venv
uv pip install -e .
coral --help
```

## Usage example

```bash
coral run examples/simple.py::main
```

## Build/deploy image only

```bash
coral image examples/simple.py::main
```

This resolves a hash-based image tag from the image spec, reuses it if already public on Docker
Hub for your logged-in Docker user, or builds + pushes it otherwise.

## Prime Intellect provider (optional)

```bash
coral setup --provider prime
coral run --provider prime examples/simple.py::main
```

Prime Intellect config lives under `[profile.<name>.prime]` in `~/.coral/config.toml` and uses
GCP for image builds/artifacts while running containers on Prime.

If gcloud prompts for reauthentication during setup, choose "no" when asked to provision
resources and enter existing GCP resource names instead.

If you disable image building on a function (`@app.function(build_image=False)`), Coral skips
provider image builds and runs image setup steps at runtime instead.

For Prime, GPU type/count are configured per function using `gpu="GPU_TYPE:COUNT"`
(for example `gpu="RTX4090_24GB:1"`), not in profile-level config.

```python
import coral

image = (
  coral.Image.python("python:3.11-slim")
  .apt_install("git")
  .pip_install("requests")
  .env({"EXAMPLE_ENV": "1"})
)

app = coral.App(name="simple", image=image)

@app.function(cpu=1, memory="1Gi", timeout=300, build_image=False)
def process(text: str) -> dict:
    tokens = text.split()
    return {"words": len(tokens), "upper": text.upper()}

@app.local_entrypoint()
def main():
    result = process.remote("hello coral")
    print("result:", result)
```

## Repo layout

- `coral/` core provider-agnostic SDK
- `coral_cli/` CLI entrypoints
- `coral_runtime/` runtime package used inside containers
- `coral_providers_gcp/` GCP provider implementation

## License

Apache-2.0

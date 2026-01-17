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

## Prime Intellect provider (optional)

```bash
coral setup --provider prime
coral run --provider prime examples/simple.py::main
```

Prime Intellect config lives under `[profile.<name>.prime]` in `~/.coral/config.toml` and uses
GCP for image builds/artifacts while running containers on Prime.

If gcloud prompts for reauthentication during setup, choose "no" when asked to provision
resources and enter existing GCP resource names instead.

```python
import coral

image = (
  coral.Image.python("python:3.11-slim")
  .apt_install("git")
  .pip_install("requests")
  .env({"EXAMPLE_ENV": "1"})
)

app = coral.App(name="simple", image=image)

@app.function(cpu=1, memory="1Gi", timeout=300)
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

# Coral

Coral is a Python SDK + CLI for running Python functions remotely on cloud providers. This repository contains the core SDK, runtime, CLI, and the initial GCP provider implementation.

## Quickstart (local dev)

```bash
uv venv
uv pip install -e .
coral --help
```

## Repo layout

- `coral/` core provider-agnostic SDK
- `coral_cli/` CLI entrypoints
- `coral_runtime/` runtime package used inside containers
- `coral_providers_gcp/` GCP provider implementation

## License

Apache-2.0

from __future__ import annotations

import os
import random
import shutil
import string
import subprocess
from pathlib import Path
from typing import Any, Optional

import typer
from google.auth import default as google_auth_default
from google.auth.transport.requests import Request
from google.cloud import resourcemanager_v3

from coral.config import load_config, write_config
from coral.errors import ConfigError
from coral.logging import get_console

app = typer.Typer(help="Authenticate and set up provider credentials")


def _credentials_valid() -> bool:
    try:
        creds, _ = __import__("google.auth").default()
        if creds.valid:
            return True
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            return True
        return False
    except Exception:
        return False


def _run_gcloud_adc_login() -> None:
    subprocess.run(
        ["gcloud", "auth", "application-default", "login"],
        check=True,
    )


def _adc_file_exists() -> bool:
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path:
        return Path(env_path).expanduser().exists()
    default_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    return default_path.exists()


def _require_gcloud() -> None:
    if not shutil.which("gcloud"):
        raise RuntimeError("gcloud CLI not found. Install gcloud to use coral setup.")


def _list_projects() -> list[dict[str, Any]]:
    creds, _ = google_auth_default()
    client = resourcemanager_v3.ProjectsClient(credentials=creds)
    projects = []
    for project in client.search_projects():
        projects.append(
            {
                "projectId": project.project_id,
                "name": project.display_name,
                "state": project.state.name,
            }
        )
    return projects


def _select_project(console) -> str:
    try:
        projects = _list_projects()
    except Exception as exc:
        console.print(
            "[warn]Project listing failed via Resource Manager API. "
            "Please enter a project ID manually.[/warn]"
        )
        console.print(f"[warn]{exc}[/warn]")
        return typer.prompt("GCP project ID")
    if not projects:
        return typer.prompt("GCP project ID")
    console.print("[info]Available GCP projects:[/info]")
    for idx, proj in enumerate(projects, start=1):
        console.print(f"{idx}) {proj.get('projectId')} - {proj.get('name')}")
    choice = typer.prompt("Select a project number", default=1)
    try:
        choice_idx = int(choice)
    except ValueError as exc:
        raise ConfigError("Invalid selection") from exc
    if choice_idx < 1 or choice_idx > len(projects):
        raise ConfigError("Selection out of range")
    return projects[choice_idx - 1]["projectId"]


def _select_region(prompt_label: str = "GCP region") -> str:
    return typer.prompt(prompt_label, default="us-central1")


def _enable_services(
    project: str,
    *,
    include_image_build: bool = True,
    include_batch: bool = True,
) -> None:
    services = [
        "logging.googleapis.com",
        "storage.googleapis.com",
    ]
    if include_batch:
        services.append("batch.googleapis.com")
    if include_image_build:
        services.extend(
            [
                "cloudbuild.googleapis.com",
                "artifactregistry.googleapis.com",
            ]
        )
    subprocess.run(
        ["gcloud", "services", "enable", *services, "--project", project],
        check=True,
    )


def _random_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def _create_bucket(project: str, region: str) -> str:
    base_name = f"coral-artifacts-{project}"
    bucket_name = base_name
    exists = subprocess.run(
        ["gcloud", "storage", "buckets", "describe", f"gs://{bucket_name}"],
        capture_output=True,
        text=True,
    )
    if exists.returncode == 0:
        return bucket_name
    for _ in range(3):
        try:
            subprocess.run(
                [
                    "gcloud",
                    "storage",
                    "buckets",
                    "create",
                    f"gs://{bucket_name}",
                    "--project",
                    project,
                    "--location",
                    region,
                    "--uniform-bucket-level-access",
                ],
                check=True,
            )
            return bucket_name
        except subprocess.CalledProcessError:
            bucket_name = f"{base_name}-{_random_suffix()}"
    raise RuntimeError("Failed to create GCS bucket")


def _create_artifact_repo(project: str, region: str) -> str:
    repo = "coral"
    result = subprocess.run(
        [
            "gcloud",
            "artifacts",
            "repositories",
            "describe",
            repo,
            "--location",
            region,
            "--project",
            project,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        subprocess.run(
            [
                "gcloud",
                "artifacts",
                "repositories",
                "create",
                repo,
                "--repository-format=docker",
                "--location",
                region,
                "--project",
                project,
            ],
            check=True,
        )
    return repo


def _create_service_account(project: str) -> str:
    name = "coral-runner"
    email = f"{name}@{project}.iam.gserviceaccount.com"
    result = subprocess.run(
        [
            "gcloud",
            "iam",
            "service-accounts",
            "describe",
            email,
            "--project",
            project,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        subprocess.run(
            [
                "gcloud",
                "iam",
                "service-accounts",
                "create",
                name,
                "--project",
                project,
                "--display-name",
                "Coral runner",
            ],
            check=True,
        )
    return email


def _active_gcloud_account() -> Optional[str]:
    result = subprocess.run(
        ["gcloud", "config", "get-value", "account"],
        capture_output=True,
        text=True,
        check=False,
    )
    account = result.stdout.strip()
    return account or None


def _grant_service_account_impersonation(service_account: str, member: str) -> None:
    subprocess.run(
        [
            "gcloud",
            "iam",
            "service-accounts",
            "add-iam-policy-binding",
            service_account,
            "--member",
            member,
            "--role",
            "roles/iam.serviceAccountTokenCreator",
        ],
        check=True,
    )


def _bind_roles(
    project: str,
    service_account: str,
    *,
    include_image_build: bool = True,
    include_batch: bool = True,
) -> None:
    roles = [
        "roles/storage.admin",
        "roles/logging.viewer",
        "roles/logging.logWriter",
        "roles/monitoring.metricWriter",
    ]
    if include_batch:
        roles.extend(
            [
                "roles/batch.admin",
                "roles/batch.agentReporter",
            ]
        )
    if include_image_build:
        roles.extend(
            [
                "roles/artifactregistry.admin",
                "roles/cloudbuild.builds.editor",
            ]
        )
    for role in roles:
        subprocess.run(
            [
                "gcloud",
                "projects",
                "add-iam-policy-binding",
                project,
                "--member",
                f"serviceAccount:{service_account}",
                "--role",
                role,
            ],
            check=True,
        )


@app.callback(invoke_without_command=True)
def main(
    profile: Optional[str] = typer.Option(None, "--profile", help="Config profile"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider (gcp|prime)"),
):
    console = get_console()
    chosen_provider = provider or typer.prompt("Provider", default="gcp")
    if chosen_provider not in {"gcp", "prime"}:
        raise ConfigError("Provider must be 'gcp' or 'prime'")

    project = ""
    region = ""
    bucket = ""
    repo = ""
    service_account = ""
    prime_image_build = False

    if chosen_provider == "gcp":
        _require_gcloud()
        if not _credentials_valid():
            console.print("[info]Launching gcloud Application Default Credentials login...[/info]")
            _run_gcloud_adc_login()
            if not _credentials_valid() and not _adc_file_exists():
                raise RuntimeError(
                    "ADC login completed but credentials are still invalid. "
                    "Check gcloud auth configuration."
                )
        console.print("[success]GCP credentials ready.[/success]")
        project = _select_project(console)
        region = _select_region()
        console.print(f"[info]Using project {project} in region {region}[/info]")
        provision = typer.confirm("Provision GCP resources with gcloud?", default=True)
        if provision:
            console.print("[info]Enabling required services...[/info]")
            _enable_services(project, include_image_build=True, include_batch=True)
            console.print("[info]Creating resources...[/info]")
            bucket = _create_bucket(project, region)
            repo = _create_artifact_repo(project, region)
            service_account = _create_service_account(project)
            _bind_roles(
                project,
                service_account,
                include_image_build=True,
                include_batch=True,
            )
        else:
            console.print("[info]Skipping gcloud provisioning.[/info]")
            bucket = typer.prompt("GCS bucket name")
            repo = typer.prompt("Artifact Registry repo name", default="coral")
            service_account = typer.prompt("Service account email")
    else:
        prime_image_build = typer.confirm(
            "Configure Google-backed image building for Prime?",
            default=False,
        )
        use_gcloud = typer.confirm(
            "Use gcloud to configure Prime artifact resources?",
            default=True,
        )
        if use_gcloud:
            _require_gcloud()
            if not _credentials_valid():
                console.print(
                    "[info]Launching gcloud Application Default Credentials login...[/info]"
                )
                _run_gcloud_adc_login()
                if not _credentials_valid() and not _adc_file_exists():
                    raise RuntimeError(
                        "ADC login completed but credentials are still invalid. "
                        "Check gcloud auth configuration."
                    )
            console.print("[success]GCP credentials ready.[/success]")
            project = _select_project(console)
            region = _select_region()
            console.print(f"[info]Using project {project} in region {region}[/info]")
            provision = typer.confirm("Provision GCP resources with gcloud?", default=True)
            if provision:
                console.print("[info]Enabling required services...[/info]")
                _enable_services(
                    project,
                    include_image_build=prime_image_build,
                    include_batch=False,
                )
                console.print("[info]Creating resources...[/info]")
                bucket = _create_bucket(project, region)
                repo = _create_artifact_repo(project, region) if prime_image_build else ""
                service_account = _create_service_account(project)
                _bind_roles(
                    project,
                    service_account,
                    include_image_build=prime_image_build,
                    include_batch=False,
                )
                account = _active_gcloud_account()
                if account:
                    member_prefix = (
                        "serviceAccount" if account.endswith("gserviceaccount.com") else "user"
                    )
                    _grant_service_account_impersonation(
                        service_account,
                        f"{member_prefix}:{account}",
                    )
            else:
                console.print("[info]Skipping gcloud provisioning.[/info]")
                bucket = typer.prompt("GCS bucket name")
                service_account = typer.prompt("Service account email", default="")
                if prime_image_build:
                    repo = typer.prompt("Artifact Registry repo name", default="coral")
        else:
            console.print("[info]Skipping gcloud auth/provisioning.[/info]")
            project = typer.prompt("GCP project ID for artifacts")
            bucket = typer.prompt("GCS bucket name for artifacts")
            service_account = typer.prompt("Service account email (optional)", default="")
            if prime_image_build:
                region = _select_region("GCP region for image builds")
                repo = typer.prompt("Artifact Registry repo name", default="coral")
            else:
                region = typer.prompt("GCP region (optional)", default="")

    profile_name = profile or "default"
    data = load_config()
    profiles = data.setdefault("profile", {})
    profile_data = profiles.setdefault(profile_name, {})
    profile_data["provider"] = chosen_provider

    if chosen_provider == "gcp":
        gcp_data = profile_data.setdefault("gcp", {})
        gcp_data.update(
            {
                "project": project,
                "region": region,
                "artifact_repo": repo,
                "gcs_bucket": bucket,
                "execution": "batch",
                "service_account": service_account,
                "machine_type": "e2-medium",
            }
        )
    else:
        api_key = typer.prompt("Prime Intellect API key", hide_input=True)
        team_id = typer.prompt("Prime Intellect team ID (optional)", default="")
        registry_credentials_id = typer.prompt(
            "Prime registry credentials ID (optional)",
            default="",
        )
        gpu_type = typer.prompt("GPU type", default="CPU_NODE")
        gpu_count = int(typer.prompt("GPU count", default="1"))
        regions_str = typer.prompt("Prime regions (comma-separated)", default="united_states")
        regions = [item.strip() for item in regions_str.split(",") if item.strip()]

        prime_data = profile_data.setdefault("prime", {})
        prime_data.update(
            {
                "api_key": api_key,
                "gcp_project": project,
                "gcs_bucket": bucket,
                "gpu_type": gpu_type,
                "gpu_count": gpu_count,
                "regions": regions,
            }
        )
        if team_id:
            prime_data["team_id"] = team_id
        if region:
            prime_data["gcp_region"] = region
        if repo:
            prime_data["artifact_repo"] = repo
        if service_account:
            prime_data["service_account"] = service_account
        if registry_credentials_id:
            prime_data["registry_credentials_id"] = registry_credentials_id

    write_config(data)
    console.print("[success]Wrote config to ~/.coral/config.toml[/success]")

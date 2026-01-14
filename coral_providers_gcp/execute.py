from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from typing import Dict

from google.cloud import batch_v1
from google.protobuf import duration_pb2

from coral.providers.base import RunHandle, RunResult
from coral.spec import CallSpec, ResourceSpec

GPU_TYPE_MAP = {
    "A100": "nvidia-tesla-a100",
    "T4": "nvidia-tesla-t4",
    "L4": "nvidia-l4",
}

GPU_MACHINE_MAP = {
    "A100:1": "a2-highgpu-1g",
    "A100:2": "a2-highgpu-2g",
    "A100:4": "a2-highgpu-4g",
    "A100:8": "a2-highgpu-8g",
    "T4:1": "n1-standard-8",
    "L4:1": "g2-standard-8",
}


@dataclass
class BatchExecutor:
    project: str
    region: str
    artifact_store: object
    machine_type: str | None = None
    service_account: str | None = None

    def _client(self) -> batch_v1.BatchServiceClient:
        return batch_v1.BatchServiceClient()

    def _job_parent(self) -> str:
        return f"projects/{self.project}/locations/{self.region}"

    def _job_id(self, run_id: str, call_id: str) -> str:
        base = f"coral-{run_id}-{call_id}"
        return base[:63]

    def _parse_memory(self, memory: str) -> int:
        value = memory.lower()
        if value.endswith("gi"):
            return int(float(value[:-2]) * 1024)
        if value.endswith("mi"):
            return int(float(value[:-2]))
        return int(float(value))

    def _parse_gpu(self, gpu: str | None) -> tuple[str, int] | None:
        if not gpu:
            return None
        if ":" in gpu:
            name, count = gpu.split(":", 1)
            return name, int(count)
        return gpu, 1

    def submit(
        self,
        call_spec: CallSpec,
        image,
        bundle,
        resources: ResourceSpec,
        env: Dict[str, str],
        labels: Dict[str, str],
    ) -> RunHandle:
        call_spec_b64 = base64.b64encode(call_spec.to_json().encode("utf-8")).decode("utf-8")
        env_vars = {
            "CORAL_CALLSPEC_B64": call_spec_b64,
            "CORAL_BUNDLE_GCS_URI": bundle.uri,
            "CORAL_RESULT_GCS_URI": call_spec.result_ref,
            "PYTHONUNBUFFERED": "1",
        }
        env_vars.update(env)

        runnable = batch_v1.Runnable(
            container=batch_v1.Runnable.Container(image_uri=image.uri),
        )
        task_spec = batch_v1.TaskSpec(
            runnables=[runnable],
            compute_resource=batch_v1.ComputeResource(
                cpu_milli=resources.cpu * 1000,
                memory_mib=self._parse_memory(resources.memory),
            ),
            environment=batch_v1.Environment(variables=env_vars),
            max_retry_count=resources.retries,
            max_run_duration=duration_pb2.Duration(seconds=resources.timeout),
        )
        task_group = batch_v1.TaskGroup(task_spec=task_spec, task_count=1)

        allocation_policy = batch_v1.AllocationPolicy()
        gpu_spec = self._parse_gpu(resources.gpu)
        if gpu_spec or self.machine_type:
            instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
                machine_type=self.machine_type or GPU_MACHINE_MAP.get(resources.gpu or "", ""),
            )
            if gpu_spec:
                gpu_name, gpu_count = gpu_spec
                accelerator = batch_v1.AllocationPolicy.Accelerator(
                    type_=GPU_TYPE_MAP.get(gpu_name, gpu_name), count=gpu_count
                )
                instance_policy.accelerators = [accelerator]
            allocation_policy.instances = [
                batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=instance_policy)
            ]
        if self.service_account:
            allocation_policy.service_account = batch_v1.ServiceAccount(
                email=self.service_account
            )

        job = batch_v1.Job(
            task_groups=[task_group],
            allocation_policy=allocation_policy,
            labels=labels,
            logs_policy=batch_v1.LogsPolicy(destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING),
        )
        job_id = self._job_id(
            call_spec.log_labels.get("coral_run_id", "run"),
            call_spec.call_id,
        )
        client = self._client()
        client.create_job(parent=self._job_parent(), job=job, job_id=job_id)
        return RunHandle(
            run_id=call_spec.log_labels.get("coral_run_id", ""),
            call_id=call_spec.call_id,
            provider_ref=job_id,
        )

    def wait(self, handle: RunHandle) -> RunResult:
        client = self._client()
        name = f"{self._job_parent()}/jobs/{handle.provider_ref}"
        verbose = bool(os.environ.get("CORAL_VERBOSE"))
        last_state = None
        while True:
            job = client.get_job(name=name)
            state = job.status.state
            if verbose and state != last_state:
                print(f"[coral] Batch job state: {state.name} ({job.name})")
                last_state = state
            if state in (batch_v1.JobStatus.State.SUCCEEDED, batch_v1.JobStatus.State.FAILED):
                break
            time.sleep(5)
        output = self.artifact_store.get_result(self.artifact_store.result_uri(handle.call_id))
        success = state == batch_v1.JobStatus.State.SUCCEEDED
        return RunResult(call_id=handle.call_id, success=success, output=output)

    def cancel(self, handle: RunHandle) -> None:
        client = self._client()
        if handle.provider_ref and handle.provider_ref != handle.run_id:
            name = f"{self._job_parent()}/jobs/{handle.provider_ref}"
            client.delete_job(name=name)
            return
        parent = self._job_parent()
        for job in client.list_jobs(parent=parent):
            if job.labels.get("coral_run_id") == handle.run_id:
                client.delete_job(name=job.name)


@dataclass
class GKEExecutor:
    project: str
    region: str

    def submit(self, *args, **kwargs):
        raise NotImplementedError("GKE executor not implemented yet. Use execution=batch.")

    def wait(self, handle: RunHandle) -> RunResult:
        raise NotImplementedError("GKE executor not implemented yet. Use execution=batch.")

    def cancel(self, handle: RunHandle) -> None:
        raise NotImplementedError("GKE executor not implemented yet. Use execution=batch.")

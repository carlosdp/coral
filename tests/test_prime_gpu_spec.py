from __future__ import annotations

import pytest

from coral.spec import ResourceSpec
from coral_providers_primeintellect.api import PrimeClient
from coral_providers_primeintellect.execute import PrimeExecutor


def _executor() -> PrimeExecutor:
    return PrimeExecutor(
        client=PrimeClient(api_key="test"),
        project="test",
        artifact_store=object(),
        regions=["united_states"],
        gpu_type="CPU_NODE",
        gpu_count=1,
    )


def test_prime_gpu_defaults_to_executor_values() -> None:
    gpu_type, gpu_count = _executor()._requested_gpu(ResourceSpec())
    assert gpu_type == "CPU_NODE"
    assert gpu_count == 1


def test_prime_gpu_parses_type_and_count_from_resource_spec() -> None:
    gpu_type, gpu_count = _executor()._requested_gpu(ResourceSpec(gpu="RTX4090_24GB:1"))
    assert gpu_type == "RTX4090_24GB"
    assert gpu_count == 1


def test_prime_gpu_rejects_invalid_count() -> None:
    with pytest.raises(RuntimeError, match="Invalid Prime GPU count"):
        _executor()._requested_gpu(ResourceSpec(gpu="RTX4090_24GB:not-a-number"))

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable

from coral.providers.base import RunHandle
from coral_providers_primeintellect.api import PrimeClient


@dataclass
class PrimeLogStreamer:
    client: PrimeClient

    def stream(self, handle: RunHandle) -> Iterable[str]:
        seen = set()
        while True:
            text = self.client.get_pod_logs(handle.provider_ref, tail=200)
            for line in text.splitlines():
                if line in seen:
                    continue
                seen.add(line)
                yield line
            time.sleep(2)

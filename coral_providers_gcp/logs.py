from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable

from google.cloud import logging_v2

from coral.providers.base import RunHandle


@dataclass
class GCPLogStreamer:
    project: str

    def _client(self) -> logging_v2.Client:
        return logging_v2.Client(project=self.project)

    def stream(self, handle: RunHandle) -> Iterable[str]:
        client = self._client()
        seen = set()
        while True:
            filter_expr = (
                "resource.type=\"batch_job\" "
                f"labels.coral_run_id=\"{handle.run_id}\""
            )
            entries = list(client.list_entries(filter_=filter_expr, order_by="timestamp asc"))
            for entry in entries:
                entry_id = f"{entry.timestamp}-{entry.insert_id}"
                if entry_id in seen:
                    continue
                seen.add(entry_id)
                ts = entry.timestamp.isoformat() if entry.timestamp else ""
                yield f"{ts} {entry.payload}"
            time.sleep(2)

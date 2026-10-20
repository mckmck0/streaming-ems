"""Record live sessions (raw responses, canonical events, manifest) and replay them.

A recording lives in ``data/recordings/<name>/``: ``raw/<source>/<n>.<ext>`` with a ``.meta.json``
(URL without secrets, status, content type, fetch time), ``events.jsonl`` in emission order and
``manifest.json``. Replayed events keep their ids: only ``source_system`` (``replay:<orig>``) and
``ingested_at`` change, and the identity rule hashes the original source.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from ems.contracts.envelope import EnvelopeBase, parse_event, to_json_bytes
from ems.contracts.ids import REPLAY_PREFIX
from ems.ingest.live.http import Fetched, redact

Event = Any


class Recorder:
    def __init__(self, root: Path, secrets: list[str] | None = None) -> None:
        self.root = root
        self.secrets = [s for s in (secrets or []) if s]
        (root / "raw").mkdir(parents=True, exist_ok=True)
        self._events = (root / "events.jsonl").open("ab")
        self._seq: Counter[str] = Counter()
        self.counts: Counter[str] = Counter()

    def raw(self, fetched: Fetched) -> None:
        self._seq[fetched.source] += 1
        folder = self.root / "raw" / fetched.source
        folder.mkdir(parents=True, exist_ok=True)
        ext = "xml" if "xml" in fetched.content_type else "json"
        base = folder / f"{self._seq[fetched.source]:05d}"
        body = redact(fetched.text(), self.secrets)
        base.with_suffix(f".{ext}").write_text(body)
        meta = {
            "source": fetched.source,
            "url": redact(fetched.url, self.secrets),
            "status": fetched.status,
            "content_type": fetched.content_type,
            "fetched_at": fetched.fetched_at.isoformat(),
        }
        base.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    def event(self, event: EnvelopeBase) -> None:
        self._events.write(to_json_bytes(event) + b"\n")
        self._events.flush()
        self.counts[event.stream.value] += 1

    def close(self, manifest: dict[str, Any]) -> Path:
        self._events.close()
        out = self.root / "manifest.json"
        body = {**manifest, "counts": dict(self.counts)}
        out.write_text(
            redact(json.dumps(body, indent=2, sort_keys=True, default=str), self.secrets) + "\n"
        )
        return out


class RecordingSink:
    """Sink that only records (``ems-gen record`` needs no Kafka)."""

    def __init__(self, recorder: Recorder) -> None:
        from ems.ingest.kafka.sink import SinkStats

        self.recorder = recorder
        self.stats = SinkStats()

    def publish(self, event: EnvelopeBase) -> bool:
        self.recorder.event(event)
        self.stats.produced += 1
        self.stats.delivered += 1
        return True

    def poll(self, timeout: float = 0) -> None:
        return None

    def close(self, timeout_s: float = 30.0) -> Any:
        return self.stats


class TeeSink:
    """Publish to a sink and record every event."""

    def __init__(self, inner: Any, recorder: Recorder) -> None:
        self.inner = inner
        self.recorder = recorder
        self.stats = inner.stats

    def publish(self, event: EnvelopeBase) -> bool:
        self.recorder.event(event)
        return bool(self.inner.publish(event))

    def poll(self, timeout: float = 0) -> None:
        self.inner.poll(timeout)

    def close(self, timeout_s: float = 30.0) -> Any:
        return self.inner.close(timeout_s)


def read_events(root: Path) -> Iterator[Event]:
    with (root / "events.jsonl").open("rb") as f:
        for line in f:
            if line.strip():
                yield parse_event(line)


def as_replay(event: Event, ingested_at: datetime) -> Event:
    """The recorded event as a replay: same id and payload, marked ``replay:<orig>``."""
    source = event.source_system
    if not source.startswith(REPLAY_PREFIX):
        source = REPLAY_PREFIX + source
    return event.model_copy(update={"source_system": source, "ingested_at": ingested_at})

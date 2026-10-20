"""Identity rules: deterministic event ids and Polish metering-point (PPE) codes."""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime

from ems.contracts.time import fmt_utc

# Fixed namespace for every EMS event id. Never change it within major version 1.
EMS_NAMESPACE = uuid.UUID("6f1c3c84-7d0a-5b9e-9a52-2f0d1a6c4e31")

ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
LOCATION_PATTERN = re.compile(r"^PL-LOC-\d{6}$")
REPLAY_PREFIX = "replay:"


def origin_source(source_system: str) -> str:
    """Source system without the ``replay:`` prefix, so replays keep their original id."""
    return source_system.removeprefix(REPLAY_PREFIX)


def event_id(
    *,
    stream: str,
    subject: str,
    interval_start: datetime,
    issued_at: datetime | None,
    source_system: str,
    run_id: str | None,
    major: int,
) -> uuid.UUID:
    """UUIDv5 over the canonical identity string (``ingested_at`` never enters it).

    ``subject`` is the location id, or the market key without run prefix (``PL:RCE``) for prices.
    """
    name = "|".join(
        [
            stream,
            subject,
            fmt_utc(interval_start),
            fmt_utc(issued_at) if issued_at is not None else "",
            origin_source(source_system),
            run_id or "",
            str(major),
        ]
    )
    return uuid.uuid5(EMS_NAMESPACE, name)


def gs1_check_digit(body: str) -> int:
    """GS1 mod-10 check digit: weights 3,1,3,... from the rightmost body digit."""
    if not body.isdigit():
        raise ValueError("GS1 body must be digits")
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10


def make_gsrn(operator: str, serial: int) -> str:
    """18-digit GSRN for a Polish PPE: ``590`` + operator (4) + serial (10) + check digit."""
    if len(operator) != 4 or not operator.isdigit():
        raise ValueError("operator prefix must be 4 digits")
    if not 0 <= serial < 10**10:
        raise ValueError("serial out of range")
    body = f"590{operator}{serial:010d}"
    return body + str(gs1_check_digit(body))


def is_valid_gsrn(code: str) -> bool:
    return (
        len(code) == 18
        and code.isdigit()
        and code.startswith("590")
        and int(code[-1]) == gs1_check_digit(code[:-1])
    )


def stable_hash64(*parts: str | int) -> int:
    """Stable 64-bit hash (blake2b), independent of PYTHONHASHSEED."""
    text = "|".join(str(p) for p in parts).encode()
    return int.from_bytes(hashlib.blake2b(text, digest_size=8).digest(), "big")


def location_seed(global_seed: int, location_id: str) -> int:
    """Per-location seed: any single location is reproducible on its own (63-bit, JSON-safe)."""
    return stable_hash64(global_seed, location_id) & ((1 << 63) - 1)

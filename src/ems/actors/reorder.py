"""The one hold-back buffer every receiver uses (R7, spec 011).

Restate keeps the order of one Kafka partition per key, but promises nothing about the order of
messages between two objects across separate invocations (V5). So every message between actors
carries a number, its step ``t`` or a stream's sequence number ``seq``, and the receiver applies
them strictly in that order: an early message is held, a stale one (already applied) is dropped.
Both are counted, outside the compared state, because they depend on arrival order only.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Inbox[M]:
    expected: int | None = None  # the next number to apply; None until the start is known
    held: tuple[tuple[int, M], ...] = ()  # early messages, sorted by number
    reordered: int = field(default=0, compare=False)  # messages that arrived early
    stale: int = field(default=0, compare=False)  # messages dropped as already applied


def in_order[M](inbox: Inbox[M], number: int, message: M) -> tuple[Inbox[M], tuple[M, ...]]:
    """Add one message; return the inbox and the messages now ready, in order."""
    if inbox.expected is not None and number < inbox.expected:
        return replace(inbox, stale=inbox.stale + 1), ()
    if any(n == number for n, _ in inbox.held):
        return replace(inbox, stale=inbox.stale + 1), ()
    held = tuple(sorted((*inbox.held, (number, message)), key=lambda item: item[0]))
    early = inbox.expected is None or number > inbox.expected
    return _release(replace(inbox, held=held, reordered=inbox.reordered + int(early)))


def begin[M](inbox: Inbox[M], expected: int) -> tuple[Inbox[M], tuple[M, ...]]:
    """Set the first number to apply (once the start is known) and release what is ready."""
    stale = sum(1 for n, _ in inbox.held if n < expected)
    held = tuple(item for item in inbox.held if item[0] >= expected)
    return _release(replace(inbox, expected=expected, held=held, stale=inbox.stale + stale))


def _release[M](inbox: Inbox[M]) -> tuple[Inbox[M], tuple[M, ...]]:
    if inbox.expected is None:
        return inbox, ()
    ready: list[M] = []
    expected, held = inbox.expected, inbox.held
    while held and held[0][0] == expected:
        ready.append(held[0][1])
        held = held[1:]
        expected += 1
    return replace(inbox, expected=expected, held=held), tuple(ready)

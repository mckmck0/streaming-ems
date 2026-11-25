"""The timing log: one line per handler commit, for the systems metrics (spec 012, AC-012-19, -20).

A side channel, outside state, messages and the journal: the handlers' transitions never see it, and
it makes no call on the Restate context, so it cannot change what a retried invocation writes. It is
on only when ``EMS_TIMING_LOG`` names a file. Each line is a JSON object:

    {"ns": <wall time>, "svc": ..., "h": ..., "key": ..., "inv": <invocation id>,
     "in_t": <t of the handled message, or null>, "sent": [[<service>, <t>], ...]}

The record is built on the event loop and written by a ``QueueListener`` thread, so no file I/O runs
on the loop (P5). The listener is stopped, and every queued line written, at a normal exit and on
SIGTERM: the hook stops it and then calls the handler the process had before. A ``kill -9`` loses
the lines still queued; the metrics count their steps as missing. This is the only actor module that
reads the wall clock.
"""

from __future__ import annotations

import atexit
import json
import logging
import logging.handlers
import os
import queue
import signal
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

LOGGER = "ems.timing"

_log = logging.getLogger(LOGGER)
_log.propagate = False
_listener: logging.handlers.QueueListener | None = None
_on = False
_previous: Any = None
_hooked = False


def start(path: Path | None = None) -> None:
    """Turn the log on, written to ``path`` (default: the setting ``EMS_TIMING_LOG``); without
    either it stays off. Starting again replaces the previous listener.
    """
    global _listener, _on
    if path is None:
        from ems.settings import EmsSettings

        path = EmsSettings().timing_log
    if path is None:
        return
    stop()
    path.parent.mkdir(parents=True, exist_ok=True)
    q: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _listener = logging.handlers.QueueListener(q, handler)
    _listener.start()
    _log.addHandler(logging.handlers.QueueHandler(q))
    _log.setLevel(logging.INFO)
    _on = True
    _hook()


def stop() -> None:
    """Write every queued line and stop the listener; harmless when the log is off."""
    global _listener, _on
    _on = False
    if _listener is not None:
        listener, _listener = _listener, None
        listener.stop()
        for h in listener.handlers:
            h.close()
    for h in [h for h in _log.handlers if isinstance(h, logging.handlers.QueueHandler)]:
        _log.removeHandler(h)


def mark(
    names: tuple[str, str],
    key: str,
    inv: str,
    in_t: int | None,
    sends: Sequence[Any],
) -> None:
    """One line for a handler commit; ``sends`` are the ``shell.Send`` values it sent."""
    if not _on:
        return
    record = {
        "ns": time.time_ns(),
        "svc": names[0],
        "h": names[1],
        "key": key,
        "inv": inv,
        "in_t": in_t,
        "sent": [[s.service, s.message.t] for s in sends],
    }
    _log.info(json.dumps(record, separators=(",", ":")))


def _hook() -> None:
    """Stop the listener at exit and on SIGTERM, then pass SIGTERM on to the previous handler."""
    global _hooked, _previous
    if not _hooked:
        atexit.register(stop)
        _hooked = True
    try:
        current = signal.getsignal(signal.SIGTERM)
        if current is not _on_sigterm:
            _previous = current
            signal.signal(signal.SIGTERM, _on_sigterm)
    except ValueError:  # not the main thread: atexit still covers a normal exit
        pass


def _on_sigterm(signum: int, frame: Any) -> None:
    stop()
    previous = _previous
    if callable(previous):
        previous(signum, frame)
    elif previous != signal.SIG_IGN:  # the default action: end the process by SIGTERM
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)

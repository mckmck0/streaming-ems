"""Spec 012, T20 (AC-012-19, -20): the timing log, ``ems.actors.timing``.

- One JSON object per line, keys in order: ``ns`` (``time.time_ns()`` at the mark), ``svc``, ``h``,
  ``key``, ``inv``, ``in_t`` (the handled message's ``t``, or null) and ``sent`` (``[service, t]``
  of each message sent, in send order).
- With the setting unset, importing the app leaves the log off: ``mark`` returns at once, without
  reading the clock, and no handler is attached.
- ``EMS_TIMING_LOG`` turns it on when the app is imported.
- The file is written on the queue listener's thread, never on the caller's (the event loop).
- The listener is stopped, and every queued line written, at a normal exit (``atexit``) and on
  SIGTERM, by a hook that then calls the handler the process had before (coverage's, or the
  default action that ends the process).

The in-process tests start and stop the log through ``tests.network.timing_on``, which also removes
what ``start`` installed. The others run a fresh interpreter, so the module's state is its own. Not
here: which objects are marked (``shell`` needs a Restate context; integration tier,
``tests/integration/test_timing_log.py``).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from ems.actors.messages import PriceUpdate
from ems.actors.shell import Send
from ems.assembler.join import Step
from tests.network import REPO, timing_on

KEYS = ["ns", "svc", "h", "key", "inv", "in_t", "sent"]
NS = 1_793_000_000_123_456_789
N = 25


def _step(t: int) -> Step:
    return Step(t, 0.1, 0.4, 0.5, False, False, False, None, ())


def _lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_ac_012_19_one_json_line_per_mark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "timing.jsonl"
    monkeypatch.setattr(time, "time_ns", lambda: NS)
    sends = (
        Send("NoBattery", "on_step", "r1:PL-LOC-000001", _step(1_992_760)),
        Send("Ems", "on_step", "r1:PL-LOC-000001", _step(1_992_760)),
        Send("Household", "on_prices", "r1:PL-LOC-000002", PriceUpdate(1_992_800, 3, False, ())),
    )
    with timing_on(path) as timing:
        timing.mark(("Household", "on_measurement"), "r1:PL-LOC-000001", "inv_1a", None, sends)
        timing.mark(("Ems", "on_ack"), "r1:PL-LOC-000001", "inv_2b", 1_992_759, ())
    text = path.read_text()
    assert text.endswith("\n") and len(text.splitlines()) == 2
    assert [list(line) for line in _lines(path)] == [KEYS, KEYS]
    assert _lines(path) == [
        {
            "ns": NS,
            "svc": "Household",
            "h": "on_measurement",
            "key": "r1:PL-LOC-000001",
            "inv": "inv_1a",
            "in_t": None,
            "sent": [["NoBattery", 1_992_760], ["Ems", 1_992_760], ["Household", 1_992_800]],
        },
        {
            "ns": NS,
            "svc": "Ems",
            "h": "on_ack",
            "key": "r1:PL-LOC-000001",
            "inv": "inv_2b",
            "in_t": 1_992_759,
            "sent": [],
        },
    ]


def test_ac_012_19_the_file_is_written_on_the_listener_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writers: list[int] = []
    emit = logging.FileHandler.emit

    def recorded(self: logging.FileHandler, record: logging.LogRecord) -> None:
        if type(self) is logging.FileHandler:  # pytest's own log handlers subclass it
            writers.append(threading.get_ident())
        emit(self, record)

    monkeypatch.setattr(logging.FileHandler, "emit", recorded)
    path = tmp_path / "timing.jsonl"
    with timing_on(path) as timing:
        for i in range(N):
            timing.mark(("Tariff", "on_price"), "r1:PL:RCE", f"inv_{i}", None, ())
    assert len(writers) == N == len(_lines(path))
    assert threading.get_ident() not in writers


def test_ac_012_19_marks_keep_their_order(tmp_path: Path) -> None:
    path = tmp_path / "timing.jsonl"
    with timing_on(path) as timing:
        for i in range(N):
            timing.mark(("Meter", "on_flows"), "r1:PL-LOC-000001", f"inv_{i}", i, ())
    lines = _lines(path)
    assert [line["inv"] for line in lines] == [f"inv_{i}" for i in range(N)]
    assert [line["ns"] for line in lines] == sorted(line["ns"] for line in lines)


def _python(
    code: str, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """``code`` in a fresh interpreter, from the repository, without ``EMS_TIMING_LOG`` unless
    ``env`` sets it."""
    base = {k: v for k, v in os.environ.items() if k != "EMS_TIMING_LOG"}
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=REPO,
        env={**base, **(env or {})},
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


OFF = """
import json, logging, time
import ems.actors.app  # starts the timing log from the settings
from ems.actors import timing

def clock():
    raise AssertionError("the wall clock was read")

time.time_ns = clock
time.time = clock
timing.mark(("Tariff", "subscribe"), "r1:PL:RCE", "inv_1", 1, ())
print(json.dumps({"handlers": len(logging.getLogger("ems.timing").handlers)}))
"""


def test_ac_012_19_with_the_setting_unset_mark_returns_at_once() -> None:
    proc = _python(OFF)
    assert proc.returncode == 0, proc.stderr[-4000:]
    assert json.loads(proc.stdout.splitlines()[-1]) == {"handlers": 0}


ON = """
import sys
import ems.actors.app  # starts the timing log from EMS_TIMING_LOG
from ems.actors import timing

for i in range(int(sys.argv[1])):
    timing.mark(("Tariff", "subscribe"), "r1:PL:RCE", f"inv_{i}", 1, ())
timing.stop()
"""


def test_ac_012_19_the_setting_turns_the_log_on_when_the_app_is_imported(tmp_path: Path) -> None:
    path = tmp_path / "timing.jsonl"
    proc = _python(ON, str(N), env={"EMS_TIMING_LOG": str(path)})
    assert proc.returncode == 0, proc.stderr[-4000:]
    assert [line["inv"] for line in _lines(path)] == [f"inv_{i}" for i in range(N)]


AT_EXIT = """
import sys
from pathlib import Path
from ems.actors import timing

timing.start(Path(sys.argv[1]))
for i in range(int(sys.argv[2])):
    timing.mark(("Tariff", "on_price"), "r1:PL:RCE", f"inv_{i}", None, ())
"""  # no stop(): the interpreter exits normally


def test_ac_012_19_a_normal_exit_writes_every_queued_line(tmp_path: Path) -> None:
    path = tmp_path / "timing.jsonl"
    proc = _python(AT_EXIT, str(path), str(N))
    assert proc.returncode == 0, proc.stderr[-4000:]
    assert len(_lines(path)) == N


CHAINED = """
import os, signal, sys, time
from pathlib import Path

log, marker = Path(sys.argv[1]), Path(sys.argv[2])


def previous(signum, frame):  # the handler the process had (an ASGI server's, coverage's)
    marker.write_text("called")
    os._exit(7)


signal.signal(signal.SIGTERM, previous)
from ems.actors import timing

timing.start(log)
for i in range(int(sys.argv[3])):
    timing.mark(("Tariff", "on_price"), "r1:PL:RCE", f"inv_{i}", None, ())
os.kill(os.getpid(), signal.SIGTERM)
time.sleep(30)
"""


def test_ac_012_19_sigterm_writes_every_line_then_calls_the_previous_handler(
    tmp_path: Path,
) -> None:
    path, marker = tmp_path / "timing.jsonl", tmp_path / "previous"
    proc = _python(CHAINED, str(path), str(marker), str(N))
    assert proc.returncode == 7, proc.stderr[-4000:]  # the previous handler ran, and it exits
    assert marker.read_text() == "called"
    assert len(_lines(path)) == N  # written before it


DEFAULT = """
import os, signal, sys, time
from pathlib import Path
from ems.actors import timing

timing.start(Path(sys.argv[1]))
for i in range(int(sys.argv[2])):
    timing.mark(("Tariff", "on_price"), "r1:PL:RCE", f"inv_{i}", None, ())
os.kill(os.getpid(), signal.SIGTERM)
time.sleep(30)
print("still running")
"""


def test_ac_012_19_sigterm_without_a_python_handler_still_ends_the_process(tmp_path: Path) -> None:
    """Under the unit tier's coverage the previous handler is coverage's, which saves and ends
    the process with SIGTERM; without coverage it is the default action. Either way: every line is
    written and the process ends by SIGTERM."""
    path = tmp_path / "timing.jsonl"
    proc = _python(DEFAULT, str(path), str(N))
    assert proc.returncode == -signal.SIGTERM, (proc.returncode, proc.stderr[-4000:])
    assert "still running" not in proc.stdout
    assert len(_lines(path)) == N

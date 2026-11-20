"""E2E fixtures: replays through Kafka and Restate compared with `==` against the offline side (spec
010 on).
"""

from __future__ import annotations

from tests.stack_fixtures import bootstrap, restate_session, run_id, stack_env

__all__ = ["bootstrap", "restate_session", "run_id", "stack_env"]

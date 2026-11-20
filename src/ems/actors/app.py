"""The SDK service registered with Restate (spec 010): `make dev` serves it with hypercorn on
127.0.0.1:9080.
"""

from __future__ import annotations

import restate

from ems.actors.probe import probe
from ems.actors.statecost import state_cost_eager, state_cost_lazy

app = restate.app(services=[probe, state_cost_eager, state_cost_lazy])

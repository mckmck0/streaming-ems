"""Deployment #2 (port 9081): a NEW strategy shipped while the pipeline keeps running.

    restate deployments register http://127.0.0.1:9081
    POST /Registry/fleet/configure {"strategies": [..., "SelfConsumptionReserve"]}

No restart of the server or of deployment #1 is needed; Household routes by name.
"""
import restate

from ems_spike.restate_app import strategy_object

app = restate.app(services=[strategy_object("SelfConsumptionReserve")])

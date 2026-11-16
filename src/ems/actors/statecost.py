"""Measurement-only objects for the cost of shipping object state (V7; spec 010, AC-010-20).

`StateCostEager` keeps the default (all state sent with every invocation); `StateCostLazy` sets
``state_preload=False`` (each key fetched when read; the SDK marks the older ``enable_lazy_state``
deprecated). A one-way ``pad`` fills the key's state to a given size; one-way ``touch`` calls read
and write one small key, so their duration shows what the padding costs. Stage 3 removes both
objects together with `Probe`.
"""

from __future__ import annotations

from restate import ObjectContext, ObjectSharedContext, VirtualObject

state_cost_eager = VirtualObject("StateCostEager")
state_cost_lazy = VirtualObject("StateCostLazy", state_preload=False)


def _add_handlers(obj: VirtualObject) -> None:
    @obj.handler()
    async def pad(ctx: ObjectContext, size: int) -> None:
        ctx.set("blob", "x" * max(size, 0))

    @obj.handler()
    async def touch(ctx: ObjectContext) -> None:
        ctx.set("c", (await ctx.get("c") or 0) + 1)

    @obj.handler(kind="shared")
    async def count(ctx: ObjectSharedContext) -> int:
        return await ctx.get("c") or 0


_add_handlers(state_cost_eager)
_add_handlers(state_cost_lazy)

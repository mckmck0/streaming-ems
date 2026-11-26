"""AC-012-39: the running Restate server uses the memory pools compose gives it.

`restate-server --dump-config` in the stack's restate container prints, as TOML, the configuration
the server resolved from its defaults and its `RESTATE_*` environment. Compose sets four pools that
together stay below the container's `mem_limit` (plan §9, F2); the dump must show each of them,
which also shows that the `"512 MiB"` format reads for the three variables added beside RocksDB.
Without them the server keeps its defaults: invoker 1.5 GiB, query engine 1.0 GiB, record cache
250.0 MiB. The container must have been recreated after the compose change (`make up`).
"""

from __future__ import annotations

import re
import tomllib
from typing import Any

from ruamel.yaml import YAML

from tests.integration.conftest import REPO, compose

# The compose variable, the key's path in the dump, and the value the dump prints.
POOLS: dict[str, tuple[tuple[str, ...], str]] = {
    "RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE": (("rocksdb-total-memory-size",), "512.0 MiB"),
    "RESTATE_WORKER__INVOKER__MEMORY_LIMIT": (("worker", "invoker", "memory-limit"), "512.0 MiB"),
    "RESTATE_ADMIN__QUERY_ENGINE__MEMORY_SIZE": (
        ("admin", "query-engine", "memory-size"),
        "384.0 MiB",
    ),
    "RESTATE_BIFROST__RECORD_CACHE_MEMORY_SIZE": (
        ("bifrost", "record-cache-memory-size"),
        "128.0 MiB",
    ),
}
_SIZE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]iB)\s*$")


def _dump_config() -> dict[str, Any]:
    out = compose("exec", "-T", "restate", "restate-server", "--dump-config", timeout=60)
    return tomllib.loads(out.stdout)


def _at(config: dict[str, Any], path: tuple[str, ...]) -> Any:
    node: Any = config
    for part in path:
        node = node.get(part) if isinstance(node, dict) else None
    return node


def _as_dumped(text: str) -> str:
    """A compose size (``512 MiB``) as the dump prints it (``512.0 MiB``)."""
    m = _SIZE.match(text)
    assert m is not None, f"not a size in MiB or GiB: {text!r}"
    return f"{float(m.group(1)):.1f} {m.group(2)}"


def test_ac_012_39_restate_runs_with_the_memory_pools_from_compose() -> None:
    config = _dump_config()
    shown = {name: _at(config, path) for name, (path, _) in POOLS.items()}
    assert shown == {name: value for name, (_, value) in POOLS.items()}

    compose_file = REPO / "infra" / "docker-compose.yml"
    env = YAML(typ="safe").load(compose_file.read_text())["services"]["restate"]["environment"]
    assert {name: _as_dumped(str(env[name])) for name in POOLS if name in env} == shown

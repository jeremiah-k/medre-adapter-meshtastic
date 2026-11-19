"""Startup-info facts from state the Meshtastic adapter already holds.

Core's runtime duck-types an optional ``startup_info()`` callable on a
built adapter and renders the returned scalar facts at the adapter's
ready transition (see the adapter-startup-info specification in the
core distribution).  This module shapes the facts this adapter holds at
that transition — the session's own node address, once the radio's
``myInfo`` has been gathered, and the configured radio endpoint — into
the plain mapping the contract expects.

Pure read: the provider performs no device or radio I/O, acquires no
resources, and mutates no session state, so a synchronous callable is
safe to run on any worker thread.  Facts that are not held yet are
omitted rather than probed.  This module imports nothing from
``medre.core``: the returned mapping is plain JSON-like data, validated
and redacted by core at the render boundary.
"""

from __future__ import annotations

from typing import Any

from medre.config.adapters.meshtastic import MeshtasticConfig


def _endpoint_facts(config: MeshtasticConfig) -> list[dict[str, Any]]:
    """Endpoint facts for the configured connection type, in display order."""
    facts: list[dict[str, Any]] = [
        {"name": "connection_type", "value": config.connection_type, "label": "link"}
    ]
    if config.connection_type == "tcp":
        if config.host:
            facts.append({"name": "host", "value": config.host, "label": "host"})
        if config.port is not None:
            facts.append({"name": "port", "value": config.port, "label": "port"})
    elif config.connection_type == "serial" and config.serial_port:
        facts.append(
            {"name": "serial_port", "value": config.serial_port, "label": "serial"}
        )
    elif config.connection_type == "ble" and config.ble_address:
        facts.append(
            {"name": "ble_address", "value": config.ble_address, "label": "ble"}
        )
    return facts


def build_startup_info(
    config: MeshtasticConfig, *, node_id: str | None = None
) -> dict[str, Any]:
    """Build the startup-info mapping from held state.

    ``node_id`` is the session's own node address (``"!"`` plus lowercase
    hex of the radio's ``my_node_num``) when the connected radio's
    ``myInfo`` has already been gathered; it is omitted when unavailable.
    The node identity forms the first row and the configured endpoint
    details the second, per the contract's row guidance.
    """
    rows: list[list[dict[str, Any]]] = []
    if node_id:
        rows.append([{"name": "node_id", "value": node_id}])
    rows.append(_endpoint_facts(config))
    return {"rows": rows}

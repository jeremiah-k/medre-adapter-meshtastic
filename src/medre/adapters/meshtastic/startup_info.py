"""Startup-info facts from state the Meshtastic adapter already holds.

Core's runtime duck-types an optional ``startup_info()`` callable on a
built adapter and renders the returned scalar facts at the adapter's
ready transition (see the adapter-startup-info specification in the
core distribution).  This module shapes the facts this adapter holds at
that transition — the session's own node address, the connected radio's
device release facts (firmware, board) once the radio has delivered its
handshake payload, the radio's LoRa link settings held from the same
initial config exchange, and the configured radio endpoint — into the
plain mapping the contract expects.

Pure read: the provider performs no device or radio I/O, acquires no
resources, and mutates no session state, so a synchronous callable is
safe to run on any worker thread.  Facts that are not held yet are
omitted rather than probed.  This module imports nothing from
``medre.core``: the returned mapping is plain JSON-like data, validated
and redacted by core at the render boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from medre.config.adapters.meshtastic import MeshtasticConfig


@dataclass(frozen=True, slots=True)
class LoRaRadioFacts:
    """The connected radio's LoRa link settings, held from connect flow.

    The session captures these scalars from the radio's ``LoRaConfig``
    (delivered in the initial config exchange, same as ``myInfo``) at
    client activation; every field not meaningfully set is ``None`` so
    the provider omits it instead of burning a fact row.  Enum fields
    hold their protobuf enum names (e.g. ``"US"``, ``"LONG_FAST"``);
    ``coding_rate`` is the raw denominator (0 means "derive from the
    preset"); ``frequency_override`` is the override frequency in MHz.
    """

    region: str | None = None
    preset: str | None = None
    coding_rate: int | None = None
    hop_limit: int | None = None
    frequency_override: float | None = None
    frequency_slot: int | None = None


def _lora_facts_row(facts: LoRaRadioFacts) -> list[dict[str, Any]] | None:
    """The labeled LoRa row for the held settings, or ``None`` if none held.

    Only settings that distinguish this radio's link render: unset
    overrides (zero means "derive from the preset / region plan") are
    omitted, and the preset appears only when the radio is actually
    using one (``use_preset``).  The region always renders when held —
    including an unset region, which is itself the operationally
    relevant fact that the radio cannot transmit.
    """
    row: list[dict[str, Any]] = []
    if facts.region:
        row.append({"name": "region", "value": facts.region, "label": "region"})
    if facts.preset:
        row.append({"name": "preset", "value": facts.preset, "label": "preset"})
    if facts.coding_rate:
        # Denominator of the coding rate (4/5 ... 4/8); render the ratio.
        if 4 <= facts.coding_rate <= 8:
            value: str | int = f"4/{facts.coding_rate}"
        else:
            value = facts.coding_rate
        row.append({"name": "coding_rate", "value": value, "label": "coding rate"})
    if facts.hop_limit:
        row.append(
            {"name": "hop_limit", "value": facts.hop_limit, "label": "hop limit"}
        )
    if facts.frequency_override:
        row.append(
            {
                "name": "frequency_override",
                "value": f"{facts.frequency_override:g} MHz",
                "label": "frequency override",
            }
        )
    if facts.frequency_slot:
        row.append(
            {
                "name": "frequency_slot",
                "value": facts.frequency_slot,
                "label": "frequency slot",
            }
        )
    return row or None


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
    config: MeshtasticConfig,
    *,
    node_id: str | None = None,
    firmware_version: str | None = None,
    board_model: str | None = None,
    lora_facts: LoRaRadioFacts | None = None,
) -> dict[str, Any]:
    """Build the startup-info mapping from held state.

    ``node_id`` is the session's own node address (``"!"`` plus lowercase
    hex of the radio's ``my_node_num``) when the connected radio's
    ``myInfo`` has already been gathered; it is omitted when unavailable.
    ``firmware_version`` and ``board_model`` are the connected radio's
    device release facts, held from the ``DeviceMetadata`` the radio
    delivers during the same initial config exchange as ``myInfo``;
    each is omitted when not held.  ``lora_facts`` holds the radio's
    LoRa link settings from that same config exchange; the row is
    omitted when no setting is meaningfully set.  The node identity
    forms the first row, the configured endpoint the second with the
    device release facts trailing it, and the LoRa link settings the
    third (core renders at most ten rows and caps the facts per row),
    per the contract's row guidance.
    """
    rows: list[list[dict[str, Any]]] = []
    if node_id:
        rows.append([{"name": "node_id", "value": node_id}])
    endpoint_facts = _endpoint_facts(config)
    if firmware_version:
        endpoint_facts.append(
            {
                "name": "firmware_version",
                "value": firmware_version,
                "label": "firmware",
            }
        )
    if board_model:
        endpoint_facts.append(
            {"name": "board_model", "value": board_model, "label": "board"}
        )
    rows.append(endpoint_facts)
    lora_row = _lora_facts_row(lora_facts) if lora_facts is not None else None
    if lora_row is not None:
        rows.append(lora_row)
    return {"rows": rows}

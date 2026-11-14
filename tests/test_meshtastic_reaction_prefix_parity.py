"""Described reactions reuse the ordinary-message relay prefix verbatim.

MMRelay — the design reference — composes the matrix → mesh reaction
description with the very same prefix builder used for ordinary messages
(``mmrelay/matrix/events.py`` calls ``get_meshtastic_prefix`` for both),
so the truncated display name in a reaction prefix renders identically to
the truncated display name on an ordinary message, whitespace included.
MEDRE matches that: the reaction path applies no space stripping before
``{nameN}`` truncation.

The regression this pins: for a sender whose display name contains a
space (e.g. ``Tad Chadwick``), the reaction prefix used to strip spaces
before truncation (``TadCh[M]: reacted …``) while ordinary messages kept
them (``Tad C[M]: hello``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.events import (
    CanonicalEvent,
    EventMetadata,
    EventRelation,
    NativeMetadata,
    NativeRef,
)
from medre.core.rendering.renderer import RenderingContext, RenderingResult

MATRIX_ADAPTER = "matrix-home"
MESH_ADAPTER = "mesh-lf"
ROOM_ID = "!room:example.org"
SENDER_MXID = "@tad:example.org"
DISPLAY_NAME = "Tad Chadwick"
REACTION_KEY = "👍"

# The automatic matrix prefix: first five code points of the display
# name, the Matrix marker, and the template's trailing ": " — identical
# on the message path and the reaction path.
EXPECTED_PREFIX = "Tad C[M]: "


@pytest.fixture(autouse=True)
def _deterministic_matrix_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Project sender labels from the matrix namespace without the matrix
    adapter distribution.

    The parity under test is the meshtastic renderer's composition, not
    cross-adapter dispatch: fake-mode CI runs without the matrix adapter
    projector installed, so the suite-standard monkeypatch (see
    test_meshtastic_default_prefix.py) supplies the display name the real
    matrix projector would.
    """

    def _project(
        native_data: dict[str, Any],
        *,
        source_adapter: str = "",
        source_transport_id: str | None = None,
        platform_hint: str | None = None,
    ) -> dict[str, str | None]:
        matrix = native_data.get("matrix") or {}
        sender = matrix.get("sender") or source_transport_id
        short = None
        if sender:
            short = sender[1:].split(":", 1)[0] or None
        return {
            "source_sender_label": matrix.get("sender_display_name"),
            "source_sender_short_label": short,
            "source_sender_id": sender,
            "source_platform": "matrix",
        }

    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields", _project
    )


def _native(**overrides: Any) -> dict[str, Any]:
    """Real matrix codec namespace shape for sender attribution."""
    matrix = {
        "schema_version": 1,
        "sender": SENDER_MXID,
        "sender_display_name": DISPLAY_NAME,
    }
    matrix.update(overrides)
    return {"matrix": matrix}


def _ordinary_message(display_name: str | None = None) -> CanonicalEvent:
    native = _native(**({"sender_display_name": display_name} if display_name else {}))
    return CanonicalEvent(
        event_id="evt-msg-1",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter=MATRIX_ADAPTER,
        source_transport_id=SENDER_MXID,
        source_channel_id=ROOM_ID,
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"body": "hello", "msgtype": "m.text"},
        metadata=EventMetadata(native=NativeMetadata(data=native)),
    )


def _described_reaction(display_name: str | None = None) -> CanonicalEvent:
    """Matrix ``m.reaction`` whose target never resolves (honest preview)."""
    rel = EventRelation(
        relation_type="reaction",
        target_event_id=None,
        target_native_ref=NativeRef(
            adapter=MATRIX_ADAPTER,
            native_channel_id=ROOM_ID,
            native_message_id="$abc123",
        ),
        key=REACTION_KEY,
        fallback_text=None,
    )
    native = _native(**({"sender_display_name": display_name} if display_name else {}))
    return CanonicalEvent(
        event_id="evt-reaction-1",
        event_kind="message.reacted",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter=MATRIX_ADAPTER,
        source_transport_id=SENDER_MXID,
        source_channel_id=ROOM_ID,
        parent_event_id=None,
        lineage=(),
        relations=(rel,),
        payload={"body": "", "msgtype": "m.text", "key": REACTION_KEY},
        metadata=EventMetadata(native=NativeMetadata(data=native)),
    )


def _renderer() -> MeshtasticRenderer:
    return MeshtasticRenderer(
        configs={MESH_ADAPTER: MeshtasticConfig(adapter_id=MESH_ADAPTER)}
    )


async def _render(
    event: CanonicalEvent, *, strategy: str = "direct"
) -> RenderingResult:
    return await _renderer().render(
        event,
        RenderingContext(target_adapter=MESH_ADAPTER, delivery_strategy=strategy),
    )


async def test_reaction_prefix_matches_message_prefix_for_spaced_sender() -> None:
    """Same sender: message and described reaction share one prefix shape."""
    message = await _render(_ordinary_message())
    reaction = await _render(_described_reaction())
    assert str(message.payload["text"]) == "Tad C[M]: hello"
    assert str(reaction.payload["text"]) == "Tad C[M]: reacted 👍 to an earlier message"
    assert message.metadata["relay_prefix_rendered"] == EXPECTED_PREFIX
    assert reaction.metadata["relay_prefix_rendered"] == EXPECTED_PREFIX


async def test_reaction_prefix_matches_message_prefix_in_fallback_mode() -> None:
    """fallback_text composes the reaction prefix the same way."""
    message = await _render(_ordinary_message(), strategy="fallback_text")
    reaction = await _render(_described_reaction(), strategy="fallback_text")
    assert str(message.payload["text"]) == "Tad C[M]: hello"
    assert str(reaction.payload["text"]) == "Tad C[M]: reacted 👍 to an earlier message"


@pytest.mark.parametrize(
    "display_name,expected_prefix",
    [
        # Truncation lands inside the name's inner whitespace run; both
        # paths must keep it verbatim (MMRelay truncates by plain slicing).
        ("Bob  Smith", "Bob  [M]: "),
        # Single-space name truncating exactly at the boundary.
        ("Ann O'Neill", "Ann O[M]: "),
    ],
)
async def test_whitespace_at_truncation_boundary_is_kept_on_both_paths(
    display_name: str, expected_prefix: str
) -> None:
    """Whitespace kept by ``{sender5}`` never differs message vs reaction."""
    message = await _render(_ordinary_message(display_name))
    reaction = await _render(_described_reaction(display_name))
    assert message.metadata["relay_prefix_rendered"] == expected_prefix
    assert reaction.metadata["relay_prefix_rendered"] == expected_prefix
    assert str(message.payload["text"]).startswith(expected_prefix)
    assert str(reaction.payload["text"]).startswith(expected_prefix)

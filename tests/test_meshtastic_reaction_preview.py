"""Cross-platform (matrix → mesh) described reaction preview behavior.

The mesh-side described reaction carries the attribution prefix, the exact
reaction key, and a bounded single-line preview of the reacted-to text.
When the target text is genuinely unresolvable — the matrix reaction's
native ref does not resolve to a stored canonical event, or the target's
quote read is denied so enrichment only sees the metadata-only envelope —
the preview degrades to the honest wording ``an earlier message`` and the
literal empty-quote shape ``to ""`` never reaches the wire.

Mesh-origin (native tapback) reactions are covered by
test_meshtastic_cross_radio_reactions.py and
test_meshtastic_packet_classifier.py; this file covers described delivery
toward the mesh only.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.events import CanonicalEvent, EventMetadata, EventRelation, NativeRef
from medre.core.events.metadata import NativeMetadata
from medre.core.planning.relation_enricher import RelationEnricher
from medre.core.planning.relation_resolution import RelationResolver
from medre.core.rendering.renderer import RenderingContext, RenderingResult

MATRIX_ADAPTER = "matrix-home"
MESH_ADAPTER = "mesh-lf"
ROOM_ID = "!room:example.org"
TARGET_EVENT_ID = "evt-target-1"
TARGET_BODY = "deploy the sensor array at dawn"
REACTION_KEY = "👍️"  # multi-codepoint: U+1F44D + U+FE0F

EMPTY_QUOTE_SHAPE = 'to ""'


# ---------------------------------------------------------------------------
# Canonical event helpers (matrix codec shapes)
# ---------------------------------------------------------------------------


def _target_event(body: str = TARGET_BODY) -> CanonicalEvent:
    """An observed matrix message, as the codec produces it."""
    return CanonicalEvent(
        event_id=TARGET_EVENT_ID,
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter=MATRIX_ADAPTER,
        source_transport_id="@alice:example.org",
        source_channel_id=ROOM_ID,
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"body": body, "msgtype": "m.text"},
        metadata=EventMetadata(),
    )


def _matrix_reaction_event(
    *,
    sender: str = "@bob:example.org",
    display_name: str | None = None,
    target_event_id: str | None = None,
    key: str = REACTION_KEY,
) -> CanonicalEvent:
    """A matrix ``m.reaction`` as the codec builds it.

    Spec-minimal reaction content carries no body field, so the payload body
    is empty and cannot serve as a preview source.  The relation targets the
    observed message through a matrix-native ref; *target_event_id* stands
    for whatever core's RelationResolver resolved at ingress.
    """
    native: dict[str, Any] = {"sender": sender}
    if display_name is not None:
        native["sender_display_name"] = display_name
    rel = EventRelation(
        relation_type="reaction",
        target_event_id=target_event_id,
        target_native_ref=NativeRef(
            adapter=MATRIX_ADAPTER,
            native_channel_id=ROOM_ID,
            native_message_id="$abc123",
        ),
        key=key,
        fallback_text=None,
    )
    return CanonicalEvent(
        event_id="evt-reaction-1",
        event_kind="message.reacted",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter=MATRIX_ADAPTER,
        source_transport_id=sender,
        source_channel_id=ROOM_ID,
        parent_event_id=None,
        lineage=(),
        relations=(rel,),
        payload={"body": "", "msgtype": "m.text", "key": key},
        metadata=EventMetadata(native=NativeMetadata(data=native)),
    )


def _make_renderer(
    *, radio_relay_prefix: str | None = None, max_text_bytes: int = 227
) -> MeshtasticRenderer:
    kwargs: dict[str, Any] = {
        "adapter_id": MESH_ADAPTER,
        "max_text_bytes": max_text_bytes,
    }
    if radio_relay_prefix is not None:
        kwargs["radio_relay_prefix"] = radio_relay_prefix
    config = MeshtasticConfig(**kwargs)
    return MeshtasticRenderer(configs={MESH_ADAPTER: config})


async def _render(
    event: CanonicalEvent,
    renderer: MeshtasticRenderer | None = None,
    *,
    strategy: str = "direct",
) -> RenderingResult:
    return await (renderer or _make_renderer()).render(
        event,
        RenderingContext(target_adapter=MESH_ADAPTER, delivery_strategy=strategy),
    )


# ---------------------------------------------------------------------------
# Storage double mirroring real backend read semantics
# ---------------------------------------------------------------------------


class _QuoteRead:
    def __init__(self, status: str, event: CanonicalEvent | None) -> None:
        self.status = status
        self.event = event


class _QuoteStorage:
    """Storage double for relation resolution and enrichment.

    ``resolve_native_ref`` maps the matrix-native triple to the canonical
    target ID only when the target was observed.  ``read_reply_context``
    denies when *quote_denied* — real callers then fall back to
    ``get_metadata``, which projects the stored envelope with separated
    body content (no ``body``/``text`` strings).
    """

    def __init__(self, *, observed: bool, quote_denied: bool) -> None:
        self._observed = observed
        self._quote_denied = quote_denied
        self._target = _target_event()

    def store_target(self, body: str) -> None:
        self._target = _target_event(body)

    async def resolve_native_ref(
        self, adapter: str, native_channel_id: str, native_message_id: str
    ) -> str | None:
        return self._target.event_id if self._observed else None

    async def read_reply_context(self, event_id: str) -> _QuoteRead:
        if self._observed and not self._quote_denied:
            return _QuoteRead("retained", self._target)
        return _QuoteRead("denied", None)

    async def get_metadata(self, event_id: str) -> CanonicalEvent | None:
        from msgspec.structs import replace

        envelope = {
            k: v
            for k, v in self._target.payload.items()
            if k not in ("body", "text") or not isinstance(v, str)
        }
        return replace(self._target, payload=envelope)


async def _resolve_and_enrich(
    event: CanonicalEvent, storage: _QuoteStorage
) -> CanonicalEvent:
    """Run the pipeline-owned relation stages before per-target rendering."""
    event = await RelationResolver(storage).resolve_event_relations(event)
    return await RelationEnricher(storage).enrich_for_target(
        event, target_adapter=MESH_ADAPTER
    )


# ===================================================================
# Resolvable target — populated bounded one-line preview
# ===================================================================


class TestResolvableTargetPreview:
    """Matrix-sourced reaction with a resolvable, readable target."""

    async def test_resolvable_target_renders_populated_preview(self) -> None:
        storage = _QuoteStorage(observed=True, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        rel = event.relations[0]
        assert rel.metadata is not None
        assert rel.metadata["original_text"] == TARGET_BODY
        result = await _render(event)
        text = str(result.payload["text"])
        # The full short target is quoted verbatim after the attribution
        # prefix (the automatic matrix marker prefix is assertion-agnostic).
        assert text.endswith(f'reacted {REACTION_KEY} to "{TARGET_BODY}"')
        assert EMPTY_QUOTE_SHAPE not in text

    async def test_preview_is_bounded_and_single_line(self) -> None:
        long_body = "alpha bravo\ncharlie delta   " + "x" * 300
        storage = _QuoteStorage(observed=True, quote_denied=False)
        storage.store_target(long_body)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        result = await _render(event)
        text = str(result.payload["text"])
        assert "\n" not in text
        quoted = text.rsplit('to "', 1)[1]
        # 40-char preview plus ellipsis, inside the closing quote.
        assert len(quoted) == 44  # 43 chars + closing quote
        assert quoted.endswith('..."')


# ===================================================================
# Unresolvable target — honest degradation, never empty quotes
# ===================================================================


class TestUnresolvableTargetDegradation:
    """Absent target text degrades to ``an earlier message``."""

    @pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
    async def test_unresolvable_target_degrades_to_earlier_message(
        self, strategy: str
    ) -> None:
        storage = _QuoteStorage(observed=False, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        assert event.relations[0].target_event_id is None
        result = await _render(event, strategy=strategy)
        text = str(result.payload["text"])
        assert text.endswith("to an earlier message")
        assert EMPTY_QUOTE_SHAPE not in text

    @pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
    async def test_denied_quote_read_degrades_to_earlier_message(
        self, strategy: str
    ) -> None:
        """Resolved target whose quote read is denied still degrades honestly."""
        storage = _QuoteStorage(observed=True, quote_denied=True)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        assert event.relations[0].target_event_id == TARGET_EVENT_ID
        rel_meta = event.relations[0].metadata or {}
        assert "original_text" not in rel_meta
        result = await _render(event, strategy=strategy)
        text = str(result.payload["text"])
        assert text.endswith("to an earlier message")
        assert EMPTY_QUOTE_SHAPE not in text

    @pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
    @pytest.mark.parametrize("body", ["👍️", "not the target text"])
    async def test_reaction_body_does_not_forge_a_target_preview(
        self, strategy: str, body: str
    ) -> None:
        """The reaction's own body never becomes the reacted-to quote."""
        from msgspec.structs import replace

        storage = _QuoteStorage(observed=False, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        event = replace(event, payload={**event.payload, "body": body})
        result = await _render(event, strategy=strategy)
        text = str(result.payload["text"])
        assert text.endswith("to an earlier message")
        assert f'to "{body}"' not in text

    async def test_degradation_holds_with_compact_prefix(self) -> None:
        storage = _QuoteStorage(observed=False, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        renderer = _make_renderer(radio_relay_prefix="{sender_short}: ")
        result = await _render(event, renderer)
        text = str(result.payload["text"])
        assert "reacted 👍️ to an earlier message" in text
        assert EMPTY_QUOTE_SHAPE not in text


# ===================================================================
# Same key, same target, three source users — three distinct messages
# ===================================================================


class TestPerUserDistinctMessages:
    """Same-key reactions from different source users never collapse."""

    async def test_three_matrix_users_render_three_distinct_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _project(
            native_data: dict[str, Any],
            *,
            source_adapter: str = "",
            source_transport_id: str | None = None,
            platform_hint: str | None = None,
        ) -> dict[str, str | None]:
            return {
                "source_sender_label": native_data.get("sender_display_name"),
                "source_sender_short_label": native_data.get("sender_display_name"),
                "source_sender_id": source_transport_id,
                "source_platform": "matrix",
            }

        monkeypatch.setattr(
            "medre.adapters.meshtastic.renderer.project_source_fields", _project
        )
        users = [
            ("@alice:example.org", "Alice A"),
            ("@bobby:example.org", "Bobby B"),
            ("@tad:example.org", "Tad C"),
        ]
        texts = []
        for sender, display in users:
            event = _matrix_reaction_event(
                sender=sender, display_name=display, target_event_id=TARGET_EVENT_ID
            )
            result = await _render(event)
            texts.append(str(result.payload["text"]))
        assert len(set(texts)) == 3
        for text in texts:
            assert f"reacted {REACTION_KEY} to" in text
            assert EMPTY_QUOTE_SHAPE not in text


# ===================================================================
# Budget respect — described body obeys the radio truncation machinery
# ===================================================================


class TestBudgetRespect:
    """The complete described body passes the UTF-8 byte budget."""

    async def test_long_target_within_budget_with_truncation_evidence(self) -> None:
        storage = _QuoteStorage(observed=True, quote_denied=False)
        storage.store_target("y" * 400)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        renderer = _make_renderer(max_text_bytes=60)
        result = await _render(event, renderer)
        text = str(result.payload["text"])
        assert len(text.encode("utf-8")) <= 60
        assert result.metadata["truncated"] is True
        assert result.metadata["rendered_text_bytes"] <= 60
        assert (
            result.metadata["original_text_bytes"]
            > result.metadata["rendered_text_bytes"]
        )
        assert EMPTY_QUOTE_SHAPE not in text

    async def test_degradation_wording_also_respects_budget(self) -> None:
        storage = _QuoteStorage(observed=False, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        renderer = _make_renderer(max_text_bytes=20)
        result = await _render(event, renderer)
        text = str(result.payload["text"])
        assert len(text.encode("utf-8")) <= 20
        assert result.metadata["truncated"] is True
        assert EMPTY_QUOTE_SHAPE not in text


# ===================================================================
# Exact key verbatim
# ===================================================================


class TestExactKeyVerbatim:
    """The explicit reaction key renders exactly once, unmodified."""

    async def test_multi_codepoint_key_rendered_verbatim(self) -> None:
        storage = _QuoteStorage(observed=True, quote_denied=False)
        event = await _resolve_and_enrich(_matrix_reaction_event(), storage)
        result = await _render(event)
        text = str(result.payload["text"])
        assert f"reacted {REACTION_KEY} to" in text
        assert text.count(REACTION_KEY) == 1

    async def test_explicit_textual_key_rendered_verbatim(self) -> None:
        storage = _QuoteStorage(observed=True, quote_denied=False)
        event = await _resolve_and_enrich(
            _matrix_reaction_event(key="acknowledged"), storage
        )
        result = await _render(event)
        assert 'reacted acknowledged to "' in str(result.payload["text"])

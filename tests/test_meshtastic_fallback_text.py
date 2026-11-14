"""Cover uncovered paths in MeshtasticRenderer._render_fallback_text and
_resolve_reply_target_marker.

Target lines (renderer.py):
  500-519  — reaction branch of _render_fallback_text
  524-544  — _resolve_reply_target_marker
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.events import (
    CanonicalEvent,
    EventMetadata,
    EventRelation,
    NativeRef,
)
from medre.core.rendering.relations import relation_target_display
from medre.core.rendering.renderer import RenderingContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_renderer(
    target_adapter: str = "mesh-1",
    *,
    relay_prefix: str = "",
    max_text_bytes: int = 227,
) -> MeshtasticRenderer:
    config = MeshtasticConfig(
        adapter_id=target_adapter,
        relay_prefix=relay_prefix,
        max_text_bytes=max_text_bytes,
    )
    return MeshtasticRenderer(configs={target_adapter: config})


def _make_event(
    *,
    source_adapter: str = "mesh-1",
    payload: dict | None = None,
    relations: tuple | None = None,
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id="evt-1",
        event_kind=(
            "message.reacted"
            if relations
            and len(relations) == 1
            and relations[0].relation_type == "reaction"
            else "message.created"
        ),
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter=source_adapter,
        source_transport_id="!node1",
        source_channel_id="0",
        parent_event_id=None,
        lineage=(),
        relations=relations or (),
        payload=payload or {"body": "hello"},
        metadata=EventMetadata(),
    )


def _make_relation(
    *,
    relation_type: str = "reaction",
    key: str | None = None,
    fallback_text: str | None = None,
    target_event_id: str | None = "evt-0",
    target_native_ref: NativeRef | None = None,
    metadata: dict | None = None,
) -> EventRelation:
    return EventRelation(
        relation_type=relation_type,
        target_event_id=target_event_id,
        target_native_ref=target_native_ref,
        key=key,
        fallback_text=fallback_text,
        metadata=metadata or {},
    )


# ===================================================================
# _render_fallback_text — reaction branch (lines 500-519)
# ===================================================================


class TestFallbackTextReaction:
    """Reaction branch of _render_fallback_text."""

    @pytest.mark.asyncio
    async def test_native_reaction_degraded_to_description(self) -> None:
        """Reaction without native delivery uses a shared description.

        Exercises lines 504-506.
        """
        renderer = _make_renderer("mesh-1")
        rel = _make_relation(key="👍")
        # source_adapter == target_adapter → _is_native_reaction is True
        event = _make_event(source_adapter="mesh-1", relations=(rel,))
        result = await renderer.render(
            event,
            RenderingContext(
                target_adapter="mesh-1",
                delivery_strategy="fallback_text",
            ),
        )
        assert result.payload["text"] == "reacted 👍 to an earlier message"

    @pytest.mark.asyncio
    async def test_native_reaction_rejects_emoji_from_payload_key(self) -> None:
        """Payload presentation cannot supply a missing structured reaction key."""
        renderer = _make_renderer("mesh-1")
        rel = _make_relation(key=None)
        event = _make_event(
            source_adapter="mesh-1",
            payload={"key": "❤️", "body": "some body"},
            relations=(rel,),
        )
        with pytest.raises(ValueError, match="explicit nonempty key"):
            await renderer.render(
                event,
                RenderingContext(
                    target_adapter="mesh-1",
                    delivery_strategy="fallback_text",
                ),
            )

    @pytest.mark.asyncio
    async def test_native_reaction_rejects_emoji_from_payload_body(self) -> None:
        """Payload presentation cannot supply a missing structured reaction key."""
        renderer = _make_renderer("mesh-1")
        rel = _make_relation(key=None)
        event = _make_event(
            source_adapter="mesh-1",
            payload={"body": "🎉"},
            relations=(rel,),
        )
        with pytest.raises(ValueError, match="explicit nonempty key"):
            await renderer.render(
                event,
                RenderingContext(
                    target_adapter="mesh-1",
                    delivery_strategy="fallback_text",
                ),
            )

    @pytest.mark.asyncio
    async def test_cross_platform_reaction_with_compact_prefix(self) -> None:
        """Cross-platform reaction with compact prefix (no trailing space).

        Exercises lines 509-519 with sep=' ' (prefix doesn't end with space).
        """
        renderer = _make_renderer(
            "mesh-1",
            relay_prefix="[{sender_short}]",
        )
        rel = _make_relation(
            key="🔥",
            fallback_text="original message text here",
        )
        # source_adapter != target_adapter → _is_native_reaction is False
        event = _make_event(
            source_adapter="matrix-src",
            relations=(rel,),
        )
        result = await renderer.render(
            event,
            RenderingContext(
                target_adapter="mesh-1",
                delivery_strategy="fallback_text",
            ),
        )
        text = result.payload["text"]
        # Prefix resolves to "[]" (sender_short empty) + sep " " + "reacted 🔥 to ..."
        assert "reacted 🔥" in text
        assert "original message text here" in text
        # Verify separator was inserted (prefix "[]" ends with "]" not space)
        assert "[] reacted" in text

    @pytest.mark.asyncio
    async def test_cross_platform_reaction_without_prefix(self) -> None:
        """Cross-platform reaction with empty compact_prefix → no prefix/sep.

        Exercises lines 516-518 with compact_prefix="" → sep="".
        """
        renderer = _make_renderer("mesh-1")  # no prefix
        rel = _make_relation(
            key="👍",
            fallback_text="hello world",
        )
        event = _make_event(
            source_adapter="matrix-src",
            relations=(rel,),
        )
        result = await renderer.render(
            event,
            RenderingContext(
                target_adapter="mesh-1",
                delivery_strategy="fallback_text",
            ),
        )
        text = result.payload["text"]
        # No prefix, so text starts directly with "reacted"
        assert text.startswith("reacted 👍")
        assert '"hello world"' in text

    @pytest.mark.asyncio
    async def test_cross_platform_reaction_prefix_with_trailing_space(self) -> None:
        """Prefix ending with a space → sep stays empty.

        Exercises lines 516-518 where compact_prefix[-1:].isspace() is True.
        """
        renderer = _make_renderer(
            "mesh-1",
            relay_prefix="{sender_short} ",
        )
        rel = _make_relation(
            key="😂",
            fallback_text="funny message",
        )
        event = _make_event(
            source_adapter="matrix-src",
            relations=(rel,),
        )
        result = await renderer.render(
            event,
            RenderingContext(
                target_adapter="mesh-1",
                delivery_strategy="fallback_text",
            ),
        )
        text = result.payload["text"]
        # Prefix ends with space so no extra separator is added
        assert "reacted 😂" in text
        assert '"funny message"' in text
        # Should not have double space
        assert "  " not in text

    @pytest.mark.asyncio
    async def test_cross_platform_reaction_original_text_from_metadata(self) -> None:
        """Cross-platform reaction uses metadata['original_text'] for preview.

        Exercises _abbreviated_original_text priority: metadata > fallback_text > payload.
        """
        renderer = _make_renderer("mesh-1")
        rel = _make_relation(
            key="❤️",
            fallback_text="fallback text",
            metadata={"original_text": "metadata original text"},
        )
        event = _make_event(
            source_adapter="matrix-src",
            relations=(rel,),
        )
        result = await renderer.render(
            event,
            RenderingContext(
                target_adapter="mesh-1",
                delivery_strategy="fallback_text",
            ),
        )
        text = result.payload["text"]
        assert '"metadata original text"' in text


# ===================================================================
# _resolve_reply_target_marker (lines 524-544)
# ===================================================================


class TestResolveReplyTargetMarker:
    """Static method _resolve_reply_target_marker edge cases."""

    def test_returns_target_event_id_when_no_native_ref(self) -> None:
        """target_native_ref is None, target_event_id is set → returns target_event_id.

        Exercises lines 542-543.
        """
        rel = EventRelation(
            relation_type="reply",
            target_event_id="evt-abc",
            target_native_ref=None,
            key=None,
            fallback_text=None,
        )
        result = relation_target_display(rel)
        assert result == "evt-abc"

    def test_returns_unknown_when_no_ref_and_no_event_id(self) -> None:
        """target_native_ref is None, target_event_id is None → unknown message.

        Exercises line 544.
        """
        rel = EventRelation(
            relation_type="reply",
            target_event_id=None,
            target_native_ref=None,
            key=None,
            fallback_text=None,
        )
        result = relation_target_display(rel)
        assert result == "unknown message"

    def test_canonical_target_marker_precedes_native_id(self) -> None:
        """A valid native reference cannot replace known canonical correlation."""
        rel = EventRelation(
            relation_type="reply",
            target_event_id="evt-fallback",
            target_native_ref=NativeRef("source", "0", "123"),
            key=None,
            fallback_text=None,
        )
        assert relation_target_display(rel) == "evt-fall…"

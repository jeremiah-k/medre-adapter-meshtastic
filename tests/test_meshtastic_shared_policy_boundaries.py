"""Meshtastic boundaries shared with the exact candidate core revision."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from medre.adapters.meshtastic.queue import MeshtasticOutboundQueue, QueueDeliveryResult
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.events import CanonicalEvent, EventMetadata, EventRelation, NativeRef
from medre.core.events.canonical import NativeMessageRef
from medre.core.planning.delivery_plan import DeliveryStrategyMethod
from medre.core.planning.relation_enricher import RelationEnricher
from medre.core.rendering.renderer import RenderingContext

TARGET = "mesh-1"


def _event(
    body: str,
    relations: tuple[EventRelation, ...] = (),
    *,
    kind: str = "message.created",
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id="event-1",
        event_kind=kind,
        schema_version=1,
        timestamp=datetime.now(UTC),
        source_adapter=TARGET,
        source_transport_id="!12345678",
        source_channel_id="0",
        parent_event_id=None,
        lineage=(),
        payload={"body": body},
        metadata=EventMetadata(),
        relations=relations,
    )


def _renderer() -> MeshtasticRenderer:
    return MeshtasticRenderer(
        configs={TARGET: MeshtasticConfig(TARGET, default_channel=4, relay_prefix="")}
    )


@pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
@pytest.mark.parametrize("reaction_first", [False, True])
async def test_mixed_relations_preserve_tuple_order_and_authored_body(
    strategy: DeliveryStrategyMethod, reaction_first: bool
) -> None:
    reply = EventRelation("reply", None, None, None, "first original")
    reaction = EventRelation("reaction", None, None, "👍", "second original")
    relations = (reaction, reply) if reaction_first else (reply, reaction)
    result = await _renderer().render(
        _event("Please check", relations),
        RenderingContext(target_adapter=TARGET, delivery_strategy=strategy),
    )
    descriptions = ["[replying to: first original]", 'reacted 👍 to "second original"']
    if reaction_first:
        descriptions.reverse()
    assert result.payload["text"] == " ".join([*descriptions, "Please check"])
    assert "reply_id" not in result.payload
    assert "emoji" not in result.payload


@pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
async def test_authored_reaction_context_preserves_quote_and_body(
    strategy: DeliveryStrategyMethod,
) -> None:
    reaction = EventRelation(
        "reaction", None, NativeRef(TARGET, "4", "9001"), "👍", "Supply request"
    )
    result = await _renderer().render(
        _event("Please bring water", (reaction,)),
        RenderingContext(target_adapter=TARGET, delivery_strategy=strategy),
    )
    assert result.payload["text"] == 'reacted 👍 to "Supply request" Please bring water'
    assert "reply_id" not in result.payload
    assert "emoji" not in result.payload


def _stored_ref(channel: str | None, packet_id: str = "9001") -> NativeMessageRef:
    return NativeMessageRef(
        id=f"mapping-{channel}",
        event_id="event-1",
        adapter=TARGET,
        native_channel_id=channel,
        native_message_id=packet_id,
        native_thread_id=None,
        native_relation_id=None,
        direction="outbound",
    )


async def _enrich(
    original: CanonicalEvent,
    event: CanonicalEvent,
    refs: list[NativeMessageRef],
    target_channel: str | None,
) -> CanonicalEvent:
    """Supply a deterministic stored snapshot to the real core authority."""

    async def get_target(event_id: str) -> CanonicalEvent | None:
        assert event_id == original.event_id
        return original

    async def list_refs(event_id: str) -> list[NativeMessageRef]:
        assert event_id == original.event_id
        return refs

    return await RelationEnricher(None).enrich_for_target(
        event,
        target_adapter=TARGET,
        target_channel=target_channel,
        cached_get_fn=get_target,
        cached_list_fn=list_refs,
    )


@pytest.mark.parametrize("relation_kind", ["reply", "reaction"])
@pytest.mark.parametrize("route_channel", [None, "4"])
async def test_default_channel_handoff_remains_bindable(
    relation_kind: str, route_channel: str | None
) -> None:
    renderer = _renderer()
    context = RenderingContext(
        delivery_strategy="direct", target_adapter=TARGET, target_channel=route_channel
    )
    original = _event("original")
    rendered = await renderer.render(original, context)
    assert rendered.payload["channel_index"] == 4
    queue = MeshtasticOutboundQueue(delay_between_messages=0)
    await queue.enqueue(dict(rendered.payload), 4, event_id=original.event_id)

    async def native_send(item: dict[str, Any]) -> dict[str, int]:
        assert item["channel_index"] == 4
        assert item["payload"]["channel_index"] == 4
        return {"id": 9001}

    delivered = await queue.process_one(native_send)
    assert isinstance(delivered, QueueDeliveryResult)
    assert delivered.handoff.native_channel_id == "4"
    assert delivered.handoff.native_message_id == "9001"
    # Core persists the handoff's native channel, not the omitted route channel.
    ref = _stored_ref(
        delivered.handoff.native_channel_id, delivered.handoff.native_message_id
    )
    relation = EventRelation(
        relation_kind,
        original.event_id,
        None,
        "👍" if relation_kind == "reaction" else None,
        None,
    )
    event = _event(
        "Please check",
        (relation,),
        kind="message.reacted" if relation_kind == "reaction" else "message.created",
    )
    enriched = await _enrich(original, event, [ref], route_channel)
    fact = enriched.relations[0].target_fact
    assert fact is not None and fact.status == "bound"
    assert fact.native_channel_id == "4"
    result = await renderer.render(enriched, context)
    assert result.payload["reply_id"] == 9001
    assert result.payload["text"] == (
        "👍" if relation_kind == "reaction" else "Please check"
    )
    assert (result.payload.get("emoji") == 1) is (relation_kind == "reaction")


@pytest.mark.parametrize("relation_kind", ["reply", "reaction"])
@pytest.mark.parametrize("channels", [(None,), ("5",), ("4", "5")])
async def test_omitted_route_cannot_bypass_unknown_wrong_or_ambiguous_channel(
    relation_kind: str, channels: tuple[str | None, ...]
) -> None:
    original = _event("original")
    relation = EventRelation(
        relation_kind,
        original.event_id,
        NativeRef(TARGET, "4", "9001"),
        "👍" if relation_kind == "reaction" else None,
        None,
    )
    event = _event(
        "Please check",
        (relation,),
        kind="message.reacted" if relation_kind == "reaction" else "message.created",
    )
    enriched = await _enrich(original, event, [_stored_ref(c) for c in channels], None)
    fact = enriched.relations[0].target_fact
    assert fact is not None
    assert fact.status == ("ambiguous" if len(channels) > 1 else "bound")
    result = await _renderer().render(
        enriched, RenderingContext(delivery_strategy="direct", target_adapter=TARGET)
    )
    assert result.payload["channel_index"] == 4
    assert "reply_id" not in result.payload
    assert "emoji" not in result.payload


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("message.created", "Please check"),
        ("message.edited", "[edited] Please check"),
        ("message.deleted", "[deleted]"),
        ("message.reacted", "[reacted]"),
    ],
)
async def test_bound_reply_preserves_shared_event_kind_semantics(
    kind: str, expected: str
) -> None:
    relation = EventRelation("reply", None, NativeRef(TARGET, "4", "9001"), None, None)
    result = await _renderer().render(
        _event("Please check", (relation,), kind=kind),
        RenderingContext(target_adapter=TARGET, delivery_strategy="direct"),
    )
    assert result.payload["reply_id"] == 9001
    assert result.payload["text"] == expected
    assert "emoji" not in result.payload

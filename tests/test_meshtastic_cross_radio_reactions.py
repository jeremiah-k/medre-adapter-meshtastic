"""Native tapbacks cross independent Meshtastic instances and packet namespaces."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.codec import MeshtasticCodec
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.adapters.meshtastic.session import NativeSendAttempt
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.engine.pipeline import PipelineConfig, PipelineRunner
from medre.core.events.bus import EventBus
from medre.core.planning.fallback_resolution import FallbackResolver
from medre.core.planning.relation_enricher import RelationEnricher
from medre.core.planning.relation_resolution import RelationResolver
from medre.core.rendering.renderer import RenderingContext, RenderingPipeline
from medre.core.routing import Route, Router, RouteSource, RouteTarget
from medre_meshtastic_tests.helpers.async_utils import wait_until
from medre_meshtastic_tests.helpers.meshtastic_bridge import (
    make_adapter_context,
    make_text_packet,
)


@pytest.mark.parametrize("mapping_ready", [True, False])
@pytest.mark.parametrize(
    "source,target,source_channel,target_channel",
    [
        ("FXLF", "FXLT", 0, 0),
        ("FXLT", "FXLF", 0, 0),
        ("FXLF", "FXLT", 4, 1),
        ("FXLT", "FXLF", 1, 4),
        ("FXLF", "FXLT", 3, 2),
        ("FXLT", "FXLF", 2, 3),
    ],
)
async def test_native_reaction_crosses_radios_after_durable_handoff(
    source,
    target,
    source_channel,
    target_channel,
    mapping_ready,
    temp_storage,
    monkeypatch,
):
    """Use native tapbacks only after handoff; queued fallback is never upgraded."""
    configs = {
        name: MeshtasticConfig(
            adapter_id=name,
            connection_type="fake",
            radio_relay_prefix="",
            message_delay_seconds=0,
        )
        for name in (source, target)
    }
    adapters = {name: MeshtasticAdapter(config) for name, config in configs.items()}
    rendering = RenderingPipeline()
    rendering.register(MeshtasticRenderer(configs=configs), priority=50)
    for name in configs:
        rendering.register_adapter_platform(name, "meshtastic")
    runner = PipelineRunner(
        PipelineConfig(
            storage=temp_storage,
            router=Router(
                routes=[
                    Route(
                        id="direct-radio",
                        source=RouteSource(
                            adapter=source, event_kinds=(), channel=str(source_channel)
                        ),
                        targets=[
                            RouteTarget(adapter=target, channel=str(target_channel))
                        ],
                    )
                ]
            ),
            fallback_resolver=FallbackResolver(),
            relation_resolver=RelationResolver(temp_storage),
            adapters=adapters,
            rendering_pipeline=rendering,
            event_bus=EventBus(),
        )
    )
    first_entered, release_original = asyncio.Event(), asyncio.Event()
    if mapping_ready:
        release_original.set()
    await runner.start()
    try:
        for name, adapter in adapters.items():
            await adapter.start(make_adapter_context(name, runner))
        session = adapters[target]._session
        assert session is not None
        # Fake mode intentionally owns no client and cannot drain radio work.
        # Install only a live-client ownership stub; the send boundary below
        # remains controlled and no SDK, sockets, serial or BLE are opened.
        session._client = SimpleNamespace(close=lambda: None)
        assert session.connected
        submissions = 0

        async def native_send(_payload):
            """Hold the first native result to expose the pre-mapping queue window."""
            nonlocal submissions
            submissions += 1
            if submissions == 1:
                first_entered.set()
                await release_original.wait()
            return NativeSendAttempt(
                native={"id": 9000 + submissions},
                connection_generation=session.connection_generation,
            )

        send = AsyncMock(side_effect=native_send)
        monkeypatch.setattr(type(session), "send_with_provenance", send)
        await adapters[source].simulate_inbound(
            make_text_packet(
                text="radio original", packet_id=101, channel=source_channel
            )
        )

        async def handed_off():
            return await temp_storage.resolve_native_ref(
                target, str(target_channel), "9001"
            )

        assert await wait_until(first_entered.is_set)
        if mapping_ready:
            assert await wait_until(handed_off)
        else:
            assert await handed_off() is None
        reaction = make_text_packet(text="👍", packet_id=102, channel=source_channel)
        reaction["decoded"].update(replyId=101, emoji=1)
        await adapters[source].simulate_inbound(reaction)
        release_original.set()
        assert await wait_until(lambda: send.await_count == 2)
        wire = send.await_args_list[1].args[0]
        if mapping_ready:
            assert wire["text"] == "👍"
            assert wire["reply_id"] == 9001
            assert wire["emoji"] == 1
        else:
            assert wire["text"] == "[reacted: 👍]"
            assert "reply_id" not in wire
            assert "emoji" not in wire
        assert wire["channel_index"] == target_channel

        async def reaction_handed_off():
            return await temp_storage.resolve_native_ref(
                target, str(target_channel), "9002"
            )

        assert await wait_until(reaction_handed_off)
        original_id = await handed_off()
        assert original_id is not None
        if not mapping_ready:
            # Publishing the original mapping cannot mutate queued/sent fallback.
            assert send.await_count == 2
            assert "emoji" not in send.await_args_list[1].args[0]
            later = make_text_packet(text="👍", packet_id=103, channel=source_channel)
            later["decoded"].update(replyId=101, emoji=1)
            await adapters[source].simulate_inbound(later)
            assert await wait_until(lambda: send.await_count == 3)
            later_wire = send.await_args_list[2].args[0]
            assert later_wire["reply_id"] == 9001
            assert later_wire["emoji"] == 1
            assert later_wire["text"] == "👍"
        event_id = await temp_storage.resolve_native_ref(
            source, str(source_channel), "102"
        )
        stored = await temp_storage.get(event_id)
        assert stored is not None
        assert stored.relations[0].target_event_id == original_id
        # The persisted ingress retains source evidence; destination enrichment is ephemeral.
        assert stored.relations[0].target_native_ref.adapter == source
        assert stored.relations[0].target_native_ref.native_message_id == "101"
    finally:
        release_original.set()
        for adapter in reversed(list(adapters.values())):
            await adapter.stop()
        await runner.stop()


@pytest.mark.parametrize("relation_kind", ["reaction", "reply"])
@pytest.mark.parametrize("strategy", ["direct", "fallback_text"])
async def test_unresolved_source_packet_id_is_not_reused_on_another_radio(
    relation_kind, strategy, temp_storage
):
    """A source-radio packet ID cannot bind a reaction/reply on another mesh."""
    configs = {
        name: MeshtasticConfig(
            adapter_id=name, connection_type="fake", radio_relay_prefix=""
        )
        for name in ("FXLF", "FXLT")
    }
    packet = make_text_packet(
        text="👍" if relation_kind == "reaction" else "reply", packet_id=102, channel=4
    )
    packet["decoded"]["replyId"] = 101
    if relation_kind == "reaction":
        packet["decoded"]["emoji"] = 1
    event = MeshtasticCodec("FXLF", configs["FXLF"]).decode(packet)
    enriched = await RelationEnricher(temp_storage).enrich_for_target(
        event, target_adapter="FXLT", target_channel="1"
    )
    result = await MeshtasticRenderer(configs=configs).render(
        enriched,
        RenderingContext(
            target_adapter="FXLT",
            target_platform="meshtastic",
            target_channel="1",
            delivery_strategy=strategy,
        ),
    )
    assert "reply_id" not in result.payload
    assert "emoji" not in result.payload
    if relation_kind == "reaction":
        assert result.payload["text"] == "[reacted: 👍]"


@pytest.mark.meshtastic_sdk
async def test_cross_radio_tapback_encodes_real_sdk_protobuf():
    """The rendered destination ID and emoji flag survive the actual SDK wire builder."""
    from msgspec.structs import replace
    from medre.adapters.meshtastic.session import MeshtasticSession
    from medre.core.events import NativeRef

    configs = {
        name: MeshtasticConfig(
            adapter_id=name, connection_type="fake", radio_relay_prefix=""
        )
        for name in ("FXLF", "FXLT")
    }
    packet = make_text_packet(text="👍", packet_id=102, channel=4)
    packet["decoded"].update(replyId=101, emoji=1)
    event = MeshtasticCodec("FXLF", configs["FXLF"]).decode(packet)
    relation = replace(
        event.relations[0],
        target_native_ref=NativeRef(
            adapter="FXLT", native_channel_id="1", native_message_id="9001"
        ),
    )
    event = replace(event, relations=(relation,))
    rendered = await MeshtasticRenderer(configs=configs).render(
        event,
        RenderingContext(
            target_adapter="FXLT",
            target_platform="meshtastic",
            target_channel="1",
            delivery_strategy="direct",
        ),
    )
    sent = []

    def capture(mesh_packet, **_kwargs):
        sent.append(mesh_packet)
        return mesh_packet

    session = MeshtasticSession(configs["FXLT"], "FXLT", "meshtastic")
    session._client = SimpleNamespace(_sendPacket=capture)
    await session.send(dict(rendered.payload))
    assert len(sent) == 1
    assert sent[0].channel == 1
    assert sent[0].decoded.reply_id == 9001
    assert sent[0].decoded.emoji == 1
    assert sent[0].decoded.payload.decode("utf-8") == "👍"

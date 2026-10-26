"""Planned packets retain the core claim while using Meshtastic's paced queue."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.errors import MeshtasticSendError
from medre.adapters.meshtastic.queue import MeshtasticOutboundQueue
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig, MeshtasticConfigError
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.engine.pipeline.radio_delivery import deliver_radio_plan
from medre.core.events import CanonicalEvent, DeliveryAttemptProvenance, EventMetadata
from medre.core.rendering.renderer import RenderingContext
from medre_meshtastic_tests.helpers.storage_outbox import (
    create_outbox_item_with_parent,
    make_outbox_item,
)


def config(**changes):
    """Use a small deterministic radio budget; no network connection is started."""
    return MeshtasticConfig(
        adapter_id="mt",
        max_text_bytes=40,
        max_text_parts=16,
        radio_relay_prefix="[North] ",
        message_delay_seconds=0,
        **changes,
    )


async def rendered(*, body="café α " * 12, parts=16, budget=40):
    """Exercise the actual renderer's strict canonical-text selection and prefix."""
    settings = MeshtasticConfig(
        adapter_id="mt",
        max_text_bytes=budget,
        max_text_parts=parts,
        radio_relay_prefix="[North] ",
    )
    event = CanonicalEvent(
        event_id="guarded-event",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(UTC),
        source_adapter="source",
        source_transport_id="peer",
        source_channel_id="0",
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"text": body},
        metadata=EventMetadata(),
    )
    return await MeshtasticRenderer(configs={"mt": settings}).render(
        event,
        RenderingContext(
            target_adapter="mt",
            target_channel="0",
            target_platform="meshtastic",
            delivery_strategy="direct",
        ),
    )


def connected(native, settings=None):
    """Install a controlled SDK client in the real session and adapter owners."""
    settings = config() if settings is None else settings
    adapter = MeshtasticAdapter(settings)
    session = MeshtasticSession(settings, adapter_id="mt", platform="meshtastic")
    session._started = True
    session._activate_client(SimpleNamespace(sendText=native))
    adapter._session = session
    adapter._started = True
    adapter.ctx = SimpleNamespace(report_delivery_feedback=AsyncMock(), logger=Mock())
    return adapter, session


async def attempt(storage, result):
    """Create one exact in-progress claim through production storage validation."""
    item = replace(
        make_outbox_item(
            status="in_progress",
            event_id=result.event_id,
            target_adapter="mt",
            target_channel="0",
        ),
        worker_id="owner",
    )
    item = await create_outbox_item_with_parent(storage, item)
    claim = DeliveryAttemptProvenance(
        item.event_id, item.delivery_plan_id, "mt", "0", item.outbox_id, 1, "live"
    )
    return replace(result, attempt_provenance=claim)


def observe_enqueues(adapter, monkeypatch):
    """Expose a deterministic enqueue hook without sleeps or SDK I/O."""
    queued = asyncio.Queue()
    original = adapter._queue.enqueue

    async def enqueue(*args, **kwargs):
        await original(*args, **kwargs)
        queued.put_nowait(None)

    monkeypatch.setattr(adapter._queue, "enqueue", enqueue)
    return queued


async def test_renderer_preserves_full_unicode_body_and_prefix_per_packet():
    """Long ordinary messages retain exact source text rather than clipped tails."""
    body = "α café\n" * 15
    result = await rendered(body=body)
    assert result.radio_plan is not None and not result.truncated
    assert "".join(p.body for p in result.radio_plan.parts) == body
    assert all(
        p.wire_text.startswith("[North] (") and p.wire_bytes <= 40
        for p in result.radio_plan.parts
    )
    assert result.metadata["rendered_text_bytes"] == sum(
        p.wire_bytes for p in result.radio_plan.parts
    )
    default = await rendered(body=body, parts=1)
    assert default.truncated and default.radio_plan is None
    zero = await rendered(body=body, budget=0)
    assert zero.payload["text"] == "" and zero.radio_plan is None
    with pytest.raises(ValueError, match="part limit"):
        await rendered(body=body, parts=2)


@pytest.mark.parametrize("value", [True, False, 0, 129, 2.5, "2", None])
def test_part_cap_is_a_bounded_explicit_integer(value):
    """Boolean or malformed configuration cannot enable a new wire behavior."""
    with pytest.raises(MeshtasticConfigError, match="max_text_parts"):
        MeshtasticConfig(adapter_id="mt", max_text_parts=value).validate()


async def test_complete_plan_retains_claim_and_never_reports_individual_parent_completion(
    temp_storage,
    monkeypatch,
):
    """Real queue/session sends commit before SDK use, then aggregate through core."""
    result = await attempt(temp_storage, await rendered())
    sent = []

    def native(text, *, channelIndex):
        with closing(sqlite3.connect(temp_storage._db_path)) as db:
            parts = json.loads(
                db.execute("SELECT parts_json FROM radio_attempt_progress").fetchone()[
                    0
                ]
            )
            assert parts[len(sent)]["state"] == "submitting"
            assert (
                db.execute("SELECT status FROM delivery_outbox").fetchone()[0]
                == "in_progress"
            )
        sent.append((text, channelIndex))
        return {"id": 100 + len(sent)}

    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    for _ in result.radio_plan.parts:
        await asyncio.wait_for(queued.get(), 2)
        packet = await adapter.send_one()
        await adapter._report_deferred_completion(
            packet
        )  # Must ignore a planned packet.
    handoff = await delivery
    assert sent == [(p.wire_text, 0) for p in result.radio_plan.parts]
    assert handoff.disposition == "transport_handoff"
    assert handoff.native_message_id is None
    assert handoff.confirmation_level == "local_transport"
    adapter.ctx.report_delivery_feedback.assert_not_awaited()
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert all(p["state"] == "accepted" for p in snapshot["parts"])
    assert [
        json.loads(p["handoff_id"])["native_message_id"] for p in snapshot["parts"]
    ] == [str(101 + i) for i in range(len(sent))]
    assert not adapter._guarded_sends and adapter._queue.queue_depth == 0


async def test_native_timeout_after_first_packet_never_retries_consumed_grant(
    temp_storage, monkeypatch
):
    """The session and queue retry loops cannot duplicate an uncertain fragment."""
    result = await attempt(temp_storage, await rendered())
    native = Mock(
        side_effect=[{"id": 101}, ConnectionError("native acceptance unknown")]
    )
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    for _ in range(2):
        await asyncio.wait_for(queued.get(), 2)
        await adapter.send_one()
    with pytest.raises(AdapterPermanentError, match="requires inspection"):
        await delivery
    assert native.call_count == 2
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert [p["state"] for p in snapshot["parts"][:3]] == [
        "accepted",
        "unknown",
        "unsent",
    ]
    assert adapter._queue.queue_depth == 0
    adapter.ctx.report_delivery_feedback.assert_not_awaited()


async def test_cancelled_waiter_is_removed_before_any_native_grant(
    temp_storage, monkeypatch
):
    """Cancellation of a pending packet frees capacity and cannot send on later drain."""
    result = await attempt(temp_storage, await rendered())
    native = Mock()
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    await asyncio.wait_for(queued.get(), 2)
    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    assert adapter._queue.queue_depth == 0
    assert await adapter.send_one() is None
    native.assert_not_called()
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert all(p["state"] == "unsent" for p in snapshot["parts"])


async def test_cancellation_after_grant_but_before_worker_call_blocks_native(
    temp_storage, monkeypatch
):
    """A worker queued behind other threads cannot transmit after its waiter closes."""
    result = await attempt(temp_storage, await rendered())
    native = Mock()
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    worker_ready, release_worker = asyncio.Event(), asyncio.Event()

    async def delayed_thread(fn, *args, **kwargs):
        worker_ready.set()
        await release_worker.wait()
        return fn(*args, **kwargs)

    monkeypatch.setattr(
        "medre.adapters.meshtastic.session.asyncio.to_thread", delayed_thread
    )
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    await asyncio.wait_for(queued.get(), 2)
    send = asyncio.create_task(adapter.send_one())
    await asyncio.wait_for(worker_ready.wait(), 2)
    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    release_worker.set()
    await send
    native.assert_not_called()
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert snapshot["parts"][0]["state"] == "unknown"


@pytest.mark.parametrize("change", ["stop", "client", "cancel"])
async def test_session_checks_owner_after_suspended_grant(change):
    """A consumed grant cannot migrate onto a replacement SDK or cancelled owner."""
    native = Mock()
    _, session = connected(native)
    entered, release = asyncio.Event(), asyncio.Event()
    cancelled = threading.Event()

    async def grant():
        entered.set()
        await release.wait()

    sending = asyncio.create_task(
        session.send(
            {"text": "packet", "channel_index": 0},
            before_native_send=grant,
            is_cancelled=cancelled.is_set,
        )
    )
    await asyncio.wait_for(entered.wait(), 2)
    replacement = Mock()
    if change == "stop":
        session._stop_requested = True
    elif change == "client":
        session._activate_client(SimpleNamespace(sendText=replacement))
    else:
        cancelled.set()
    release.set()
    with pytest.raises(MeshtasticSendError) as caught:
        await sending
    assert caught.value.transient is False
    native.assert_not_called()
    replacement.assert_not_called()


async def test_stop_resolves_guarded_waiters_and_retains_ordinary_pending_work(
    temp_storage, monkeypatch
):
    """Guarded packet ownership ends without emitting deferred parent feedback."""
    result = await attempt(temp_storage, await rendered())
    native = Mock()
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    await adapter._queue.enqueue({"text": "ordinary"}, 0)
    await queued.get()
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    await asyncio.wait_for(queued.get(), 2)
    await adapter.stop()
    with pytest.raises(AdapterPermanentError):
        await delivery
    assert adapter._queue.queue_depth == 1
    assert (await adapter._queue.dequeue())["payload"]["text"] == "ordinary"
    native.assert_not_called()
    adapter.ctx.report_delivery_feedback.assert_not_awaited()


async def test_concurrent_queue_consumers_share_one_send_owner():
    """Manual draining and the background drain cannot overlap or bypass pacing."""
    queue = MeshtasticOutboundQueue(delay_between_messages=0)
    for text in ("first", "second"):
        await queue.enqueue({"text": text}, 0)
    entered, release, second_ready = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def native(item):
        calls.append(item["payload"]["text"])
        if len(calls) == 1:
            entered.set()
            await release.wait()
        return {"id": len(calls)}

    first = asyncio.create_task(queue.process_one(native))
    await entered.wait()

    async def second():
        second_ready.set()
        return await queue.process_one(native)

    following = asyncio.create_task(second())
    await second_ready.wait()
    assert calls == ["first"]
    release.set()
    await asyncio.gather(first, following)
    assert calls == ["first", "second"]


async def test_complete_plan_cannot_be_sent_directly_to_adapter():
    """The renderer's first packet is not a substitute for full core dispatch."""
    adapter, _ = connected(Mock())
    with pytest.raises(AdapterPermanentError, match="core-owned packet dispatch"):
        await adapter.deliver(await rendered())


@pytest.mark.parametrize(
    "native_result", [None, {"id": True}, {"id": "bad"}, {"id": -1}, {"id": 2**32}]
)
async def test_missing_or_invalid_sdk_packet_identity_holds_remaining_parts(
    temp_storage, monkeypatch, native_result
):
    """A planned Meshtastic packet requires its actual uint32 identity, never coercion."""
    result = await attempt(temp_storage, await rendered())
    native = Mock(return_value=native_result)
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    await asyncio.wait_for(queued.get(), 2)
    await adapter.send_one()
    with pytest.raises(AdapterPermanentError):
        await delivery
    assert native.call_count == 1
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert snapshot["parts"][0]["state"] == "unknown"
    assert snapshot["parts"][1]["state"] == "unsent"


async def test_result_normalization_failure_resolves_waiter_without_resending(
    temp_storage, monkeypatch
):
    """An SDK return followed by evidence conversion failure cannot hang or falsely finish."""
    result = await attempt(temp_storage, await rendered())
    native = Mock(return_value={"id": 101})
    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)

    def broken_snapshot(value):
        raise ValueError("injected invalid SDK metadata")

    monkeypatch.setattr(
        "medre.adapters.meshtastic.queue._packet_snapshot", broken_snapshot
    )
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    await asyncio.wait_for(queued.get(), 2)
    with pytest.raises(ValueError):
        await adapter.send_one()
    with pytest.raises(AdapterPermanentError):
        await asyncio.wait_for(delivery, 2)
    assert native.call_count == 1
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert snapshot["parts"][0]["state"] == "unknown"
    assert adapter._queue.pop_cancelled_item() is None
    assert not adapter._guarded_sends


@pytest.mark.parametrize("guarded_packet", [False, True])
async def test_normalization_failure_does_not_cancel_sent_or_abandon_pending_work(
    monkeypatch, guarded_packet
):
    """A returned SDK call cannot poison shutdown's in-flight cancellation capture."""
    from medre.adapters.meshtastic.guarded import GuardedQueuedSend

    adapter, _ = connected(Mock())
    guard = GuardedQueuedSend(AsyncMock()) if guarded_packet else None
    await adapter._queue.enqueue({"text": "sent"}, 0, guarded_send=guard)
    await adapter._queue.enqueue({"text": "pending"}, 0)

    def broken_snapshot(value):
        raise ValueError("injected invalid SDK metadata")

    monkeypatch.setattr(
        "medre.adapters.meshtastic.queue._packet_snapshot", broken_snapshot
    )

    async def native(item):
        if guard is not None:
            await guard.grant()
        return {"id": 101}

    with pytest.raises(ValueError, match="invalid SDK metadata"):
        await adapter._queue.process_one(native)
    if guard is not None:
        with pytest.raises(AdapterPermanentError, match="normalization failed"):
            await guard.completion
        guard.close()
    await adapter._report_cancelled_and_drain()
    adapter.ctx.report_delivery_feedback.assert_not_awaited()
    assert adapter._queue.total_sent == 0
    assert adapter._queue.pending_count == 1
    assert (await adapter._queue.dequeue())["payload"]["text"] == "pending"


async def test_queue_capacity_failure_preserves_ordinary_work_without_a_grant(
    temp_storage,
):
    """Admission failure stays retryable and never mutates queued ordinary payloads."""
    from medre.core.contracts.adapter import AdapterSendError

    result = await attempt(temp_storage, await rendered())
    native = Mock()
    adapter, _ = connected(native, config(queue_max_size=1))
    await adapter._queue.enqueue({"text": "ordinary"}, 0)
    with pytest.raises(AdapterSendError):
        await deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    assert not adapter._guarded_sends
    assert (await adapter._queue.dequeue())["payload"]["text"] == "ordinary"
    native.assert_not_called()
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert all(part["state"] == "unsent" for part in snapshot["parts"])


async def test_background_owner_finishes_packets_without_deferred_parent_feedback(
    temp_storage,
):
    """The real background drain routes guarded completions solely to waiting core."""
    result = await attempt(temp_storage, await rendered(body="one two three " * 6))
    calls = []

    def native(text, *, channelIndex):
        calls.append(text)
        return {"id": 100 + len(calls)}

    adapter, _ = connected(native)
    drain = asyncio.create_task(adapter._process_queue())
    try:
        handoff = await asyncio.wait_for(
            deliver_radio_plan(temp_storage, result, "owner", adapter.deliver),
            3,
        )
        assert handoff.disposition == "transport_handoff"
        assert calls == [part.wire_text for part in result.radio_plan.parts]
        adapter.ctx.report_delivery_feedback.assert_not_awaited()
    finally:
        adapter._started = False
        drain.cancel()
        await drain


async def test_ordinary_packet_keeps_existing_deferred_feedback_contract(temp_storage):
    """Opting in for long text does not turn normal sends into parent-blocking packets."""
    result = await attempt(temp_storage, await rendered(body="short"))
    adapter, _ = connected(Mock(return_value={"id": 101}))
    handoff = await adapter.deliver(result)
    assert handoff.disposition == "deferred"
    sent = await adapter.send_one()
    await adapter._report_deferred_completion(sent)
    adapter.ctx.report_delivery_feedback.assert_awaited_once()
    assert not adapter._guarded_sends


def test_max_text_parts_does_not_shift_existing_positional_configuration():
    """The opt-in is appended after all established configuration fields."""
    from dataclasses import fields

    original = config()
    old_arguments = [
        getattr(original, field.name)
        for field in fields(original)
        if field.name != "max_text_parts"
    ]
    restored = MeshtasticConfig(*old_arguments)
    assert restored.max_text_parts == 1
    assert restored.max_text_bytes == original.max_text_bytes
    assert restored.detection_sensor_relay == original.detection_sensor_relay


async def test_slow_native_grant_does_not_consume_the_next_send_spacing(monkeypatch):
    """A delayed first invocation must not let its successor send in a burst."""
    queue = MeshtasticOutboundQueue(delay_between_messages=2)
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(
        "medre.adapters.meshtastic.queue.time",
        SimpleNamespace(monotonic=lambda: clock.now),
    )

    async def pause(duration):
        clock.now += duration

    monkeypatch.setattr("medre.adapters.meshtastic.queue.asyncio.sleep", pause)
    for text in ("slow", "next"):
        await queue.enqueue({"text": text}, 0)
    native_times = []

    async def native(item):
        if item["payload"]["text"] == "slow":
            clock.now += 5  # The storage grant or SDK ran longer than the pacing gap.
        native_times.append(clock.now)
        return {"id": 100 + len(native_times)}

    await queue.process_one(native)
    await queue.process_one(native)
    assert native_times[1] - native_times[0] == 2


@pytest.mark.parametrize("channel", [None, True, "0", -1])
async def test_guarded_channel_cannot_be_silently_coerced(temp_storage, channel):
    """Malformed native scope is rejected before queue mutation or grant consumption."""
    result = await attempt(temp_storage, await rendered(body="short"))
    native, grant = Mock(), AsyncMock()
    adapter, _ = connected(native)
    packet = replace(
        result,
        payload={"text": "packet", "channel_index": channel},
        before_native_send=grant,
    )
    with pytest.raises(AdapterPermanentError, match="channel"):
        await adapter.deliver(packet)
    grant.assert_not_awaited()
    native.assert_not_called()
    assert adapter._queue.queue_depth == 0


@pytest.mark.meshtastic_sdk
async def test_guarded_structured_reply_uses_real_pinned_protobuf_and_captured_client():
    """The guarded native-reply branch preserves the SDK packet and known reply ID."""
    from meshtastic.protobuf import mesh_pb2

    grant = AsyncMock()
    adapter, session = connected(Mock())
    packets = []

    def send_packet(packet, **kwargs):
        assert grant.await_count == 1
        assert isinstance(packet, mesh_pb2.MeshPacket)
        assert packet.decoded.reply_id == 55
        assert packet.decoded.payload == b"native reply"
        assert kwargs["wantAck"] is False
        packets.append(packet)
        return packet

    client = session._client
    client._sendPacket = send_packet
    client._generatePacketId = lambda: 101
    result = await session.send(
        {"text": "native reply", "reply_id": 55, "channel_index": 0},
        before_native_send=grant,
        is_cancelled=lambda: False,
    )
    assert len(packets) == 1 and result.id == 101
    assert session._client is client


async def test_native_uint32_zero_id_is_retained_without_fabrication(
    temp_storage, monkeypatch
):
    """The SDK's uint32 domain includes zero; it is distinct from a missing ID."""
    result = await attempt(temp_storage, await rendered())
    sent = []

    def native(text, **kwargs):
        sent.append(text)
        return {"id": len(sent) - 1}

    adapter, _ = connected(native)
    queued = observe_enqueues(adapter, monkeypatch)
    delivery = asyncio.create_task(
        deliver_radio_plan(temp_storage, result, "owner", adapter.deliver)
    )
    for _ in result.radio_plan.parts:
        await asyncio.wait_for(queued.get(), 2)
        await adapter.send_one()
    await delivery
    snapshot = await temp_storage.get_radio_attempt_snapshot(result.outbox_id, 1)
    assert json.loads(snapshot["parts"][0]["handoff_id"])["native_message_id"] == "0"


async def test_guarded_mapping_packet_id_zero_wins_over_secondary_id() -> None:
    """SDK packet ID zero is authoritative even when an alias differs."""
    from medre.adapters.meshtastic.guarded import GuardedQueuedSend

    queue = MeshtasticOutboundQueue(delay_between_messages=0)
    guard = GuardedQueuedSend(AsyncMock())
    await queue.enqueue({"text": "zero"}, 0, guarded_send=guard)
    packet = await queue.process_one(
        lambda _item: asyncio.sleep(0, result={"packet_id": 0, "id": 123})
    )
    assert packet is not None and packet.handoff.native_message_id == "0"
    guard.complete(packet.handoff)
    assert (await guard.completion).native_message_id == "0"
    guard.close()


def test_mapping_packet_id_precedence() -> None:
    """The queue preserves zero when the SDK reports only packet_id."""
    from medre.adapters.meshtastic.queue import _extract_packet_id

    assert _extract_packet_id({"packet_id": 0, "id": 456}) == "0"
    assert _extract_packet_id({"packet_id": 0}) == "0"
    assert _extract_packet_id({"packet_id": None, "id": 456}) == "456"

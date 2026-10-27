"""Bounded, session-fenced correlation of accepted sends to routing callbacks.

Unit coverage for :class:`MeshtasticSendCorrelation` plus adapter
integration proving the pairing is classification only: a correlated
callback never produces a core observation, and every adversarial shape
(callback before acceptance, duplicates, conflicting outcomes, reused
ids, stale generations, expiry, capacity, close) fails closed.
"""

from __future__ import annotations

import concurrent.futures
from typing import Any

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.routing_observations import (
    NativeRoutingResult,
    RejectedNativeRouting,
    normalize_native_routing_result,
)
from medre.adapters.meshtastic.send_correlation import (
    AcceptedSendFacts,
    CorrelatedRoutingResult,
    MeshtasticSendCorrelation,
    UncorrelatedRoutingResult,
    _native_id_key,
)
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_rendering_result,
)


def _facts(native_id: str = "42", generation: int = 1) -> AcceptedSendFacts:
    return AcceptedSendFacts(
        native_message_id=native_id,
        native_channel_id="0",
        connection_generation=generation,
        event_id="evt-1",
        delivery_plan_id="plan-1",
        outbox_id="obox-1",
        attempt_number=1,
    )


def _routing(
    request_id: int = 42,
    *,
    outcome: str = "ack",
    from_id: str = "!deadbeef",
) -> NativeRoutingResult:
    return NativeRoutingResult(
        outcome="ack" if outcome == "ack" else "nak",  # type: ignore[arg-type]
        request_id=request_id,
        error_reason="NONE" if outcome == "ack" else "NO_CHANNEL",
        from_id=from_id,
        to_id="!11112222",
        channel_index=0,
        packet_id=999,
        hop_start=1,
        hop_limit=1,
        rx_time=None,
        relay_node=None,
    )


class TestNativeIdKey:
    """Native id strings become integer request-id keys or nothing."""

    def test_decimal_strings_and_explicit_zero_map_to_ints(self) -> None:
        assert _native_id_key("42") == 42
        assert _native_id_key("0") == 0

    @pytest.mark.parametrize("value", ["", "  ", "abc", "4 2", "-1", "0x10"])
    def test_unmatchable_strings_are_refused(self, value: str) -> None:
        assert _native_id_key(value) is None


class TestCorrelationHappyPath:
    """An exactly recorded send correlates exactly once."""

    def test_ack_pairs_with_retained_facts(self) -> None:
        registry = MeshtasticSendCorrelation()
        assert registry.record(_facts()) is True
        result = registry.observe(_routing(), connection_generation=1)
        assert isinstance(result, CorrelatedRoutingResult)
        assert result.outcome == "ack"
        assert result.facts is not None
        assert result.facts.outbox_id == "obox-1"
        assert result.facts.attempt_number == 1
        # The pairing names its own rejection: never delivered evidence.
        assert "does not qualify" in result.source_meaning
        snapshot = registry.snapshot()
        assert snapshot["matched_acks"] == 1
        assert snapshot["active"] == 0

    def test_identical_duplicate_is_idempotent(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        first = registry.observe(_routing(), connection_generation=1)
        assert isinstance(first, CorrelatedRoutingResult)
        second = registry.observe(_routing(), connection_generation=1)
        assert isinstance(second, CorrelatedRoutingResult)
        assert second.outcome == "duplicate"
        assert second.facts is None
        assert registry.snapshot()["duplicates"] == 1

    def test_negative_result_pairs_but_never_qualifies(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        result = registry.observe(_routing(outcome="nak"), connection_generation=1)
        assert isinstance(result, CorrelatedRoutingResult)
        assert result.outcome == "nak"
        assert "does not qualify" in result.source_meaning


class TestAckSourceClassification:
    """The local implicit acknowledgment classifies as itself, not a third party."""

    def test_ack_from_the_local_node_is_local_implicit(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        result = registry.observe(
            _routing(), connection_generation=1, own_node_id="!deadbeef"
        )
        assert isinstance(result, CorrelatedRoutingResult)
        assert result.ack_source == "local_implicit"
        assert "mesh relaying" in result.source_meaning

    def test_ack_from_another_node_is_third_party(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        result = registry.observe(
            _routing(), connection_generation=1, own_node_id="!ccccdddd"
        )
        assert isinstance(result, CorrelatedRoutingResult)
        assert result.ack_source == "third_party"
        assert "unrelated node" in result.source_meaning

    def test_duplicate_carries_the_same_source_classification(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        first = registry.observe(
            _routing(), connection_generation=1, own_node_id="!deadbeef"
        )
        assert isinstance(first, CorrelatedRoutingResult)
        duplicate = registry.observe(
            _routing(), connection_generation=1, own_node_id="!deadbeef"
        )
        assert isinstance(duplicate, CorrelatedRoutingResult)
        assert duplicate.outcome == "duplicate"
        assert duplicate.ack_source == "local_implicit"
        assert "mesh relaying" in duplicate.source_meaning


class TestCorrelationFailures:
    """Every ambiguous or hostile shape is refused and counted."""

    def test_callback_before_acceptance_is_dropped_not_buffered(self) -> None:
        registry = MeshtasticSendCorrelation()
        result = registry.observe(_routing(), connection_generation=1)
        assert isinstance(result, UncorrelatedRoutingResult)
        assert "no retained acceptance" in result.reason
        # Recording afterwards must not retroactively pair the same event.
        registry.record(_facts())
        again = registry.observe(_routing(), connection_generation=1)
        assert isinstance(again, CorrelatedRoutingResult)
        assert again.outcome == "ack"
        assert registry.snapshot()["unmatched"] == 1

    def test_conflicting_outcomes_for_one_id_are_refused(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        ack = registry.observe(_routing(), connection_generation=1)
        assert isinstance(ack, CorrelatedRoutingResult)
        nak = registry.observe(_routing(outcome="nak"), connection_generation=1)
        assert isinstance(nak, UncorrelatedRoutingResult)
        assert "conflicting outcomes" in nak.reason
        assert registry.snapshot()["conflicting"] == 1

    def test_reused_native_id_becomes_permanently_ambiguous(self) -> None:
        registry = MeshtasticSendCorrelation()
        assert registry.record(_facts(native_id="42")) is True
        assert registry.record(_facts(native_id="42")) is False
        result = registry.observe(_routing(request_id=42), connection_generation=1)
        assert isinstance(result, UncorrelatedRoutingResult)
        assert "reused" in result.reason

    def test_stale_connection_generation_is_refused(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts(generation=1))
        result = registry.observe(_routing(), connection_generation=2)
        assert isinstance(result, UncorrelatedRoutingResult)
        assert "retired connection generation" in result.reason
        assert registry.snapshot()["stale_generation"] == 1

    def test_missing_generation_is_refused(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts(generation=3))
        result = registry.observe(_routing())
        assert isinstance(result, UncorrelatedRoutingResult)

    def test_expired_entry_is_gone_not_guessable(self) -> None:
        now = [100.0]
        registry = MeshtasticSendCorrelation(
            retention_seconds=10.0, clock=lambda: now[0]
        )
        registry.record(_facts())
        now[0] += 10.0 + 0.001  # one monotonic step past the retention bound
        result = registry.observe(_routing(), connection_generation=1)
        assert isinstance(result, UncorrelatedRoutingResult)
        assert "no retained acceptance" in result.reason
        assert registry.snapshot()["expired"] == 1

    def test_capacity_refuses_new_ids_without_forgetting_reuse(self) -> None:
        registry = MeshtasticSendCorrelation(capacity=2)
        registry.record(_facts(native_id="1"))
        registry.record(_facts(native_id="2"))
        assert not registry.record(_facts(native_id="3"))
        assert registry.snapshot()["active"] == 2
        oldest = registry.observe(_routing(request_id=1), connection_generation=1)
        assert isinstance(oldest, CorrelatedRoutingResult)
        newer = registry.observe(_routing(request_id=3), connection_generation=1)
        assert isinstance(newer, UncorrelatedRoutingResult)
        assert registry.snapshot()["retained_ids"] == 2
        assert registry.snapshot()["capacity_refusals"] == 1

    def test_non_numeric_native_id_is_never_guessed_into_a_key(self) -> None:
        registry = MeshtasticSendCorrelation()
        assert registry.record(_facts(native_id="opaque")) is False
        assert registry.snapshot()["unmatchable_ids"] == 1
        result = registry.observe(_routing(request_id=42))
        assert isinstance(result, UncorrelatedRoutingResult)

    def test_zero_id_send_is_recorded_but_zero_request_id_never_matches(
        self,
    ) -> None:
        registry = MeshtasticSendCorrelation()
        assert registry.record(_facts(native_id="0")) is True
        rejected = normalize_native_routing_result(
            {
                "id": 5,
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 0,
                    "routing": {"errorReason": "NONE"},
                },
            }
        )
        assert isinstance(rejected, RejectedNativeRouting)
        result = registry.observe(rejected)
        assert isinstance(result, UncorrelatedRoutingResult)
        assert registry.snapshot()["rejected_normalization"] == 1

    def test_rejected_normalization_is_counted(self) -> None:
        registry = MeshtasticSendCorrelation()
        result = registry.observe(
            RejectedNativeRouting(reason="malformed routing packet")
        )
        assert isinstance(result, UncorrelatedRoutingResult)
        assert registry.snapshot()["rejected_normalization"] == 1

    def test_clear_closes_the_registry(self) -> None:
        registry = MeshtasticSendCorrelation()
        registry.record(_facts())
        registry.clear()
        result = registry.observe(_routing())
        assert isinstance(result, UncorrelatedRoutingResult)
        assert "closed" in result.reason
        assert registry.snapshot()["closed"] is True
        assert registry.snapshot()["after_close"] == 1
        assert registry.record(_facts()) is False

    def test_concurrent_observers_never_corrupt_state(self) -> None:
        registry = MeshtasticSendCorrelation()
        for native_id in range(50):
            registry.record(_facts(native_id=str(native_id)))

        def observe_many(base: int) -> int:
            correlated = 0
            for offset in range(25):
                result = registry.observe(
                    _routing(request_id=base + offset),
                    connection_generation=1,
                )
                if isinstance(result, CorrelatedRoutingResult):
                    correlated += 1
            return correlated

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(observe_many, base) for base in (0, 25, 50, 75)]
            matched = sum(future.result() for future in futures)
        assert matched == 50  # every id correlates at most once, exactly once

    def test_validation_rejects_invalid_bounds(self) -> None:
        with pytest.raises(ValueError):
            MeshtasticSendCorrelation(capacity=0)
        with pytest.raises(ValueError):
            MeshtasticSendCorrelation(capacity=True)
        with pytest.raises(ValueError):
            MeshtasticSendCorrelation(retention_seconds=0)
        with pytest.raises(ValueError):
            MeshtasticSendCorrelation(retention_seconds="60")  # type: ignore[arg-type]


class _FakeCorrelationSession:
    """Minimal session double exposing generation and a send boundary."""

    def __init__(self, packet_id: int = 4242, generation: int = 1) -> None:
        self.connection_generation = generation
        self.connected = True
        self.node_id = "!0a0b0c0d"
        self._packet_id = packet_id

    async def send(self, packet_dict: dict[str, Any]) -> dict[str, Any]:
        return {"packet_id": self._packet_id}

    async def send_with_provenance(self, packet_dict: dict[str, Any]) -> Any:
        from medre.adapters.meshtastic.session import NativeSendAttempt

        return NativeSendAttempt(
            native={"packet_id": self._packet_id},
            connection_generation=self.connection_generation,
        )

    async def stop(self, timeout: float = 5.0) -> None:
        self.connected = False

    def diagnostics(self) -> Any:
        """Shape-compatible diagnostics for the adapter snapshot."""
        from medre.adapters.meshtastic.session import MeshtasticSessionDiagnostics

        return MeshtasticSessionDiagnostics(
            connected=self.connected,
            reconnecting=False,
            reconnect_attempts=0,
            last_packet_time=None,
            node_id=self.node_id,
            channel_count=1,
            transient_delivery_failures=0,
            permanent_delivery_failures=0,
            stale_receive_callbacks=0,
            stale_disconnect_callbacks=0,
            last_error=None,
        )


class TestAdapterIntegration:
    """The adapter records accepts and classifies callbacks; core sees none."""

    async def test_accepted_send_correlates_later_callback_without_emitting(
        self, make_adapter_context
    ) -> None:
        feedback: list[Any] = []

        async def sink(item: Any) -> None:
            feedback.append(item)

        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        adapter.ctx = make_adapter_context(config.adapter_id)
        adapter.ctx.report_delivery_feedback = sink
        session = _FakeCorrelationSession(packet_id=4242, generation=1)
        adapter._session = session  # type: ignore[assignment]
        adapter._started = True

        await adapter.deliver(make_meshtastic_rendering_result(event_id="evt-corr"))
        sent = await adapter.send_one()
        assert sent is not None and isinstance(sent.item, dict)
        # Drive the same reporting path the queue drain task uses.
        await adapter._report_deferred_completion(sent)

        ack_packet = {
            "id": 900001,
            "fromId": "!feedf00d",
            "toId": "!0a0b0c0d",
            "channel": 0,
            "decoded": {
                "portnum": "ROUTING_APP",
                "requestId": 4242,
                "routing": {"errorReason": "NONE"},
            },
        }
        adapter._on_packet(ack_packet)

        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["recorded"] == 1
        assert snapshot["matched_acks"] == 1
        assert snapshot["unmatched"] == 0
        # Classification only: the feedback sink saw exactly the deferred
        # completion and never a post-handoff observation.
        kinds = [type(item).__name__ for item in feedback]
        assert kinds == ["DeferredHandoffCompleted"]

    async def test_callback_for_unknown_id_is_dropped(
        self, make_adapter_context
    ) -> None:
        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        adapter.ctx = make_adapter_context(config.adapter_id)
        adapter._session = _FakeCorrelationSession(generation=1)  # type: ignore[assignment]
        adapter._started = True

        adapter._on_packet(
            {
                "id": 900002,
                "fromId": "!feedf00d",
                "toId": "!0a0b0c0d",
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 777777,
                    "routing": {"errorReason": "NONE"},
                },
            }
        )
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["unmatched"] == 1
        assert snapshot["recorded"] == 0

    async def test_stop_closes_correlation_and_late_callback_is_counted(
        self, make_adapter_context
    ) -> None:
        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        adapter.ctx = make_adapter_context(config.adapter_id)
        adapter._session = _FakeCorrelationSession(generation=1)  # type: ignore[assignment]
        adapter._started = True
        await adapter.stop()
        assert adapter.diagnostics()["send_correlation"]["closed"] is True

        adapter._session = _FakeCorrelationSession(generation=1)  # type: ignore[assignment]
        adapter._started = True
        adapter._on_packet(
            {
                "id": 900003,
                "fromId": "!feedf00d",
                "toId": "!0a0b0c0d",
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 4242,
                    "routing": {"errorReason": "NONE"},
                },
            }
        )
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["closed"] is True
        assert snapshot["after_close"] >= 1

    async def test_restart_installs_a_fresh_registry(
        self, make_adapter_context
    ) -> None:
        """A stop/start cycle correlates again instead of refusing as closed."""
        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        ctx = make_adapter_context(config.adapter_id)
        adapter.ctx = ctx
        await adapter.start(ctx)
        await adapter.stop()
        assert adapter.diagnostics()["send_correlation"]["closed"] is True

        await adapter.start(ctx)
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["closed"] is False
        assert snapshot["after_close"] == 0

        # A callback after the restart is classified against the fresh
        # registry, not refused with the closed-registry reason.
        adapter._on_packet(
            {
                "id": 900004,
                "fromId": "!feedf00d",
                "toId": "!0a0b0c0d",
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 777777,
                    "routing": {"errorReason": "NONE"},
                },
            }
        )
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["unmatched"] == 1
        assert snapshot["after_close"] == 0

    async def test_guarded_packet_facts_are_never_recorded(
        self, make_adapter_context
    ) -> None:
        """Synchronous planned-packet handoffs belong to core's dispatch."""
        from medre.adapters.meshtastic.guarded import GuardedQueuedSend
        from medre.adapters.meshtastic.queue import QueueDeliveryResult
        from medre.core.contracts.delivery import AdapterHandoffResult

        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        adapter.ctx = make_adapter_context(config.adapter_id)

        async def _grant() -> None:
            return None

        guarded = GuardedQueuedSend(_grant)
        result = QueueDeliveryResult(
            item={
                "payload": {"text": "hi"},
                "channel_index": 0,
                "guarded_send": guarded,
                "_connection_generation": 1,
            },
            handoff=AdapterHandoffResult(
                native_message_id="5150",
                native_channel_id="0",
                confirmation_level="local_transport",
            ),
        )
        adapter._record_send_correlation(result)
        assert adapter.diagnostics()["send_correlation"]["recorded"] == 0

    async def test_diagnostic_snapshot_shape(self, make_adapter_context) -> None:
        config = make_meshtastic_config(connection_type="fake")
        adapter = MeshtasticAdapter(config)
        adapter.ctx = make_adapter_context(config.adapter_id)
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["capacity"] == config.queue_max_size
        assert snapshot["retention_seconds"] == int(
            config.reconnect_backoff_max_seconds
        )
        assert snapshot["closed"] is False


@pytest.mark.parametrize("outcome", ["ack", "nak"])
def test_resolved_id_cannot_be_rebound_to_another_send(outcome):
    """Terminal callbacks retain the native ID fence for the adapter lifetime."""
    registry = MeshtasticSendCorrelation()
    assert registry.record(_facts())
    assert isinstance(
        registry.observe(_routing(outcome=outcome), connection_generation=1),
        CorrelatedRoutingResult,
    )
    assert not registry.record(_facts())
    assert isinstance(
        registry.observe(_routing(outcome=outcome), connection_generation=1),
        UncorrelatedRoutingResult,
    )


def test_expiry_keeps_a_bounded_reuse_fence():
    """Expiry discards facts without admitting a delayed callback under reused IDs."""
    now = [0.0]
    registry = MeshtasticSendCorrelation(
        capacity=2, retention_seconds=1, clock=lambda: now[0]
    )
    assert registry.record(_facts())
    now[0] = 2.0
    assert not registry.record(_facts())
    assert isinstance(
        registry.observe(_routing(), connection_generation=1), UncorrelatedRoutingResult
    )
    assert registry.snapshot()["retained_ids"] == 1


def test_wrong_channel_does_not_consume_the_original_acceptance():
    """A mismatched native callback cannot consume another channel's facts."""
    from dataclasses import replace

    registry = MeshtasticSendCorrelation()
    registry.record(_facts())
    assert isinstance(
        registry.observe(replace(_routing(), channel_index=1), connection_generation=1),
        UncorrelatedRoutingResult,
    )
    assert isinstance(
        registry.observe(_routing(), connection_generation=1), CorrelatedRoutingResult
    )


async def test_structured_send_uses_the_client_that_supplies_its_generation(
    monkeypatch,
):
    """A replacement session cannot borrow another client's send provenance."""
    from medre.adapters.meshtastic.session import MeshtasticSession

    session = MeshtasticSession(
        make_meshtastic_config(), adapter_id="radio", platform="meshtastic"
    )
    old_client, new_client = object(), object()
    session._client = old_client
    generation = session.connection_generation

    async def structured(*args, client=None, **kwargs):
        session._client = new_client
        assert client is old_client
        return {"id": 42}

    monkeypatch.setattr(type(session), "_send_structured", structured)
    result = await session.send_with_provenance({"text": "reply", "reply_id": 7})
    assert result.connection_generation == generation
    assert result.native == {"id": 42}

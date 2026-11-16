"""Focused tests for the Meshtastic startup packet drain window.

Verifies that relay-classified inbound packets arriving while the
content-independent startup drain window is open are dropped before codec
decode / publish — regardless of ``rxTime`` — and that the window clears
lazily after expiry, logs a single completion line, and leaves the
rxTime-based startup backlog suppression intact as defense-in-depth.

Behavioral reference: mmrelay's startup drain window
(``STARTUP_PACKET_DRAIN_SECS``).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.async_utils import wait_until
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_text_packet,
)

from medre.adapters.meshtastic import adapter as adapter_module
from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.errors import MeshtasticConnectionError
from medre.adapters.meshtastic.session import MeshtasticSession

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(adapter_id: str = "mesh-drain") -> AdapterContext:
    """Build a minimal AdapterContext for testing."""
    return AdapterContext(
        adapter_id=adapter_id,
        publish_inbound=AsyncMock(),
        logger=logging.getLogger(f"test.{adapter_id}"),
        clock=lambda: datetime.now(timezone.utc),
        shutdown_event=asyncio.Event(),
    )


def _expire_drain_window(adapter: MeshtasticAdapter) -> None:
    """Move the drain deadline one second into the monotonic past.

    Simulates a fully elapsed drain window without sleeps or clock
    patching: the next drain-check evaluation must observe expiry, clear
    the deadline, and emit the single completion line.
    """
    adapter._startup_drain_deadline_monotonic = time.monotonic() - 1.0


# ---------------------------------------------------------------------------
# Packets inside the window are dropped
# ---------------------------------------------------------------------------


class TestDrainDropsWithinWindow:
    """Relay-classified packets are dropped while the window is open."""

    async def test_packet_dropped_simulate_inbound(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(make_meshtastic_text_packet(text="backlog"))

        ctx.publish_inbound.assert_not_called()
        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 1
        assert diag["inbound_published"] == 0

        await adapter.stop()

    async def test_packet_dropped_on_packet_callback(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        adapter._on_packet(make_meshtastic_text_packet(text="backlog"))

        await wait_until(
            lambda: adapter.diagnostics()["startup_drain_packets_dropped"] >= 1
        )

        ctx.publish_inbound.assert_not_called()
        assert adapter.diagnostics()["inbound_published"] == 0

        await adapter.stop()

    async def test_packet_without_rx_time_dropped(self) -> None:
        """Packets lacking rxTime are dropped — the rxTime gate cannot judge them."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        packet = make_meshtastic_text_packet(text="no rxTime")
        assert "rxTime" not in packet
        await adapter.simulate_inbound(packet)

        ctx.publish_inbound.assert_not_called()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()

    async def test_malformed_rx_time_dropped(self) -> None:
        """Malformed rxTime does not bypass the drain window."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        packet = make_meshtastic_text_packet(text="bad rxTime")
        packet["rxTime"] = "not-a-number"
        await adapter.simulate_inbound(packet)

        ctx.publish_inbound.assert_not_called()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()

    async def test_stale_rx_time_packet_counts_as_drain_drop(self) -> None:
        """Backlog with stale rxTime is counted by the drain, not the rxTime gate.

        The drain is evaluated first, so drain-dropped packets never enter
        the startup-backlog counters.
        """
        config = make_meshtastic_config(
            startup_backlog_suppress_seconds=10.0,
            startup_packet_drain_seconds=30.0,
        )
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        stale_rx = adapter._adapter_start_epoch - 1000.0
        packet = make_meshtastic_text_packet(text="old")
        packet["rxTime"] = stale_rx
        await adapter.simulate_inbound(packet)

        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 1
        assert diag["startup_backlog_packets_seen"] == 0
        assert diag["startup_backlog_packets_suppressed"] == 0
        ctx.publish_inbound.assert_not_called()

        await adapter.stop()

    async def test_multiple_packets_accumulate_dropped_counter(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        for i in range(3):
            await adapter.simulate_inbound(
                make_meshtastic_text_packet(text=f"backlog {i}", packet_id=100 + i)
            )

        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 3
        ctx.publish_inbound.assert_not_called()

        await adapter.stop()

    async def test_non_relay_packets_not_gated(self) -> None:
        """Non-relay classifications flow normally during the drain window."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(
            {
                "fromId": "!node1",
                "id": 1,
                "decoded": {"portnum": "telemetry"},
            }
        )

        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 0
        assert diag["classifier_packets_seen"] == 1

        await adapter.stop()


# ---------------------------------------------------------------------------
# Expiry: packets relay again, completion logged once
# ---------------------------------------------------------------------------


class TestDrainExpiry:
    """After the window expires, packets relay and the window stays closed."""

    async def test_packet_after_expiry_relayed(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))

        ctx.publish_inbound.assert_called_once()
        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 0
        assert diag["inbound_published"] == 1

        await adapter.stop()

    async def test_completion_info_logged_once(self, caplog) -> None:
        """The drain-end completion line is emitted exactly once."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        with caplog.at_level(logging.INFO, logger="test.mesh-drain"):
            await adapter.simulate_inbound(
                make_meshtastic_text_packet(text="first", packet_id=1)
            )
            await adapter.simulate_inbound(
                make_meshtastic_text_packet(text="second", packet_id=2)
            )

        completion_lines = [
            rec
            for rec in caplog.records
            if "startup packet drain window ended" in rec.getMessage()
        ]
        assert len(completion_lines) == 1
        assert "dropped=0" in completion_lines[0].getMessage()
        assert ctx.publish_inbound.call_count == 2

        await adapter.stop()

    async def test_dropped_count_reported_on_completion(self, caplog) -> None:
        """The completion line carries the number of packets the drain dropped."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(
            make_meshtastic_text_packet(text="backlog", packet_id=1)
        )
        _expire_drain_window(adapter)
        with caplog.at_level(logging.INFO, logger="test.mesh-drain"):
            await adapter.simulate_inbound(
                make_meshtastic_text_packet(text="live", packet_id=2)
            )

        completion_lines = [
            rec
            for rec in caplog.records
            if "startup packet drain window ended" in rec.getMessage()
        ]
        assert len(completion_lines) == 1
        assert "dropped=1" in completion_lines[0].getMessage()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()

    async def test_window_stays_closed_after_expiry(self) -> None:
        """The deadline is cleared at expiry; later checks return False."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        assert adapter._startup_drain_deadline_monotonic is not None
        _expire_drain_window(adapter)

        assert adapter._check_startup_packet_drain(1) is False
        assert adapter._startup_drain_deadline_monotonic is None
        # Second evaluation sees the cleared deadline: no second completion.
        assert adapter._check_startup_packet_drain(2) is False

        await adapter.stop()

    async def test_expiry_logged_on_first_gate_evaluation(self, caplog) -> None:
        """Expiry logs even when the next packet is suppressed by rxTime.

        The drain check runs before the rxTime gate, so the completion line
        fires on the drain check itself, not on successful relay.
        """
        config = make_meshtastic_config(
            startup_backlog_suppress_seconds=10.0,
            startup_packet_drain_seconds=30.0,
        )
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        with caplog.at_level(logging.INFO, logger="test.mesh-drain"):
            stale_rx = adapter._adapter_start_epoch - 1000.0
            packet = make_meshtastic_text_packet(text="stale", packet_id=1)
            packet["rxTime"] = stale_rx
            await adapter.simulate_inbound(packet)

        completion_lines = [
            rec
            for rec in caplog.records
            if "startup packet drain window ended" in rec.getMessage()
        ]
        assert len(completion_lines) == 1
        # rxTime suppression remains armed behind the drain gate.
        diag = adapter.diagnostics()
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert diag["inbound_published"] == 0

        await adapter.stop()

    async def test_drop_debug_log_format(self, caplog) -> None:
        """Per-packet drop logs use the suppression log format with remaining time."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        with caplog.at_level(logging.DEBUG, logger="test.mesh-drain"):
            await adapter.simulate_inbound(
                make_meshtastic_text_packet(text="backlog", packet_id=77)
            )

        drop_lines = [
            rec
            for rec in caplog.records
            if "startup packet drain dropped" in rec.getMessage()
        ]
        assert len(drop_lines) == 1
        message = drop_lines[0].getMessage()
        assert "transport=meshtastic" in message
        assert "packet_id=77" in message
        assert "remaining=" in message

        await adapter.stop()


# ---------------------------------------------------------------------------
# Disabled drain (window == 0)
# ---------------------------------------------------------------------------


class TestDisabledDrain:
    """startup_packet_drain_seconds == 0 disables the drain entirely."""

    async def test_zero_window_leaves_deadline_unarmed(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        assert adapter._startup_drain_deadline_monotonic is None

        await adapter.stop()

    async def test_zero_window_relays_immediately(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))

        ctx.publish_inbound.assert_called_once()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 0

        await adapter.stop()

    async def test_zero_window_no_completion_log(self, caplog) -> None:
        """A disabled drain never logs a completion line."""
        config = make_meshtastic_config(startup_packet_drain_seconds=0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        with caplog.at_level(logging.DEBUG, logger="test.mesh-drain"):
            await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))

        assert "startup packet drain" not in caplog.text

        await adapter.stop()


# ---------------------------------------------------------------------------
# Arming, defaults, restart, and diagnostics
# ---------------------------------------------------------------------------


class TestDrainArmingAndDiagnostics:
    """Deadline arming, session resets, and evidence exposure."""

    async def test_production_default_arms_deadline(self) -> None:
        """The default 15.0 window arms a future deadline on start."""
        from medre.config.adapters.meshtastic import MeshtasticConfig

        config = MeshtasticConfig(adapter_id="mesh-drain")
        assert config.startup_packet_drain_seconds == 15.0
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        deadline = adapter._startup_drain_deadline_monotonic
        assert deadline is not None
        assert deadline > time.monotonic()

        await adapter.stop()

    async def test_diagnostics_expose_drain_evidence(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(make_meshtastic_text_packet(text="backlog"))
        diag = adapter.diagnostics()

        assert diag["startup_drain_packets_dropped"] == 1
        assert isinstance(diag["startup_drain_packets_dropped"], int)
        assert diag["startup_packet_drain_seconds"] == 30.0
        assert isinstance(diag["startup_packet_drain_seconds"], float)

        await adapter.stop()

    async def test_restart_rearms_deadline_and_resets_counter(self) -> None:
        """stop()+start() re-arms the window and resets the dropped count."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()

        await adapter.start(ctx)
        await adapter.simulate_inbound(make_meshtastic_text_packet(text="backlog"))
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()
        await adapter.start(ctx)

        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 0
        deadline = adapter._startup_drain_deadline_monotonic
        assert deadline is not None
        assert deadline > time.monotonic()

        await adapter.stop()

    async def test_rx_time_suppression_untouched_behind_drain(self) -> None:
        """With the drain expired, stale rxTime packets are still suppressed."""
        config = make_meshtastic_config(
            startup_backlog_suppress_seconds=10.0,
            startup_packet_drain_seconds=30.0,
        )
        adapter = MeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        stale_rx = adapter._adapter_start_epoch - 1000.0
        packet = make_meshtastic_text_packet(text="stale", packet_id=1)
        packet["rxTime"] = stale_rx
        await adapter.simulate_inbound(packet)

        ctx.publish_inbound.assert_not_called()
        diag = adapter.diagnostics()
        assert diag["startup_backlog_packets_seen"] == 1
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert diag["inbound_published"] == 0

        await adapter.stop()


@pytest.mark.parametrize("initial_refusal", [False, True])
@pytest.mark.parametrize("drain_seconds", [0.0, 15.0])
async def test_degraded_start_arms_at_first_receive_subscription(
    monkeypatch, initial_refusal, drain_seconds
) -> None:
    """Retained starts and initial refusals must not spend the drain offline."""
    clock = SimpleNamespace(wall=1_800_000_000.0, monotonic=100.0)
    monkeypatch.setattr(
        adapter_module,
        "time",
        SimpleNamespace(time=lambda: clock.wall, monotonic=lambda: clock.monotonic),
    )
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    monkeypatch.setattr(MeshtasticSession, "_reconnect_delay", lambda self, attempt: 0)
    ready = asyncio.Event()
    subscriptions = []
    attempts = 0
    native = SimpleNamespace(close=MagicMock())

    async def connect(session):
        nonlocal attempts
        attempts += 1
        if initial_refusal and attempts == 1:
            raise MeshtasticConnectionError("offline")
        await ready.wait()
        return native

    def subscribe(session):
        subscriptions.append(adapter._adapter_start_epoch)
        assert adapter._adapter_start_epoch == clock.wall
        expected = clock.monotonic + drain_seconds if drain_seconds else None
        assert adapter._startup_drain_deadline_monotonic == expected
        # This is deliberately inside registration, not after the recovery
        # task has finished. A post-subscription health hook would be too late.
        session._on_receive(
            make_meshtastic_text_packet(text="queued without rxTime", packet_id=1),
            interface=native,
        )

    monkeypatch.setattr(MeshtasticSession, "_create_client_offloop", connect)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", subscribe)
    adapter = MeshtasticAdapter(
        make_meshtastic_config(
            connection_type="tcp",
            host="localhost",
            sync_timeout_ms=10,
            tcp_liveness_interval_seconds=0,
            startup_packet_drain_seconds=drain_seconds,
        )
    )
    ctx = _make_context()
    try:
        await adapter.start(ctx)
        session = adapter._session
        assert session is not None and session.reconnecting and not session.connected
        assert adapter._adapter_start_epoch is None
        assert adapter._startup_drain_deadline_monotonic is None
        clock.wall += 20
        clock.monotonic += 20
        ready.set()
        assert await wait_until(lambda: session.connected)
        assert subscriptions == [clock.wall]
        assert attempts == (2 if initial_refusal else 1)
        if drain_seconds:
            assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1
            ctx.publish_inbound.assert_not_awaited()
        else:
            assert await wait_until(lambda: ctx.publish_inbound.await_count == 1)
        clock.wall += 30
        clock.monotonic += 30
        # The rxTime gate starts at connection readiness too. This packet
        # was fresh relative to the old start epoch, but is now stale.
        stale = make_meshtastic_text_packet(text="old rxTime", packet_id=2)
        stale["rxTime"] = clock.wall - 40
        session._on_receive(stale, interface=native)
        assert adapter.diagnostics()["startup_backlog_packets_suppressed"] == 1
        session._on_receive(
            make_meshtastic_text_packet(text="live", packet_id=3), interface=native
        )
        assert await wait_until(
            lambda: ctx.publish_inbound.await_count == (1 if drain_seconds else 2)
        )
    finally:
        ready.set()
        await adapter.stop()


async def test_initial_subscription_failure_rearms_but_later_reconnect_does_not(
    monkeypatch,
) -> None:
    """The first successful subscription owns the baseline, not failed attempts."""
    clock = SimpleNamespace(wall=1_800_000_000.0, monotonic=100.0)
    monkeypatch.setattr(
        adapter_module,
        "time",
        SimpleNamespace(time=lambda: clock.wall, monotonic=lambda: clock.monotonic),
    )
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    monkeypatch.setattr(MeshtasticSession, "_reconnect_delay", lambda self, attempt: 0)
    clients = [SimpleNamespace(close=MagicMock()) for _ in range(3)]
    calls = 0
    first = asyncio.Event()

    async def connect(session):
        nonlocal calls
        calls += 1
        if calls == 1:
            await first.wait()
        return clients[calls - 1]

    def subscribe(session):
        if calls == 1:
            assert adapter._startup_drain_deadline_monotonic == 115.0
            clock.wall += 20
            clock.monotonic += 20
            raise MeshtasticConnectionError("subscription failed")
        if calls == 2:
            assert adapter._startup_drain_deadline_monotonic == 135.0
        if calls == 3:
            assert adapter._startup_drain_deadline_monotonic is None

    monkeypatch.setattr(MeshtasticSession, "_create_client_offloop", connect)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", subscribe)
    adapter = MeshtasticAdapter(
        make_meshtastic_config(
            connection_type="tcp",
            host="localhost",
            sync_timeout_ms=10,
            tcp_liveness_interval_seconds=0,
            startup_packet_drain_seconds=15.0,
        )
    )
    ctx = _make_context()
    try:
        await adapter.start(ctx)
        session = adapter._session
        assert session is not None and session.reconnecting
        first.set()
        assert await wait_until(lambda: session.connected and calls == 2)
        clients[0].close.assert_called_once()
        baseline = adapter._adapter_start_epoch
        assert baseline == clock.wall
        clock.wall += 20
        clock.monotonic += 20
        assert adapter._check_startup_packet_drain(1) is False
        session.notify_connection_lost()
        assert await wait_until(lambda: session.connected and calls == 3)
        assert adapter._adapter_start_epoch == baseline
        assert adapter._startup_drain_deadline_monotonic is None
    finally:
        first.set()
        await adapter.stop()


async def test_retired_initial_connection_hook_cannot_rearm_restart() -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config(startup_packet_drain_seconds=15))
    ctx = _make_context()
    try:
        await adapter.start(ctx)
        session = adapter._session
        assert session is not None
        old_hook = session._initial_connection_callback
        assert old_hook is not None
        await adapter.stop()
        await adapter.start(ctx)
        _expire_drain_window(adapter)
        assert adapter._check_startup_packet_drain(1) is False
        baseline = adapter._adapter_start_epoch
        old_hook()
        assert adapter._adapter_start_epoch == baseline
        assert adapter._startup_drain_deadline_monotonic is None
    finally:
        await adapter.stop()

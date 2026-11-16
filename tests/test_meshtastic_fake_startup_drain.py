"""Focused tests for FakeMeshtasticAdapter startup packet drain mirroring.

Verifies that the fake mirrors the real adapter's content-independent
startup drain window: relay-classified packets arriving while the window
is open are dropped before decode and publish regardless of ``rxTime``,
the window clears lazily after expiry with a single completion line, and
the rxTime-based startup backlog suppression stays intact behind it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_text_packet,
)

from medre.adapters.fakes.meshtastic import FakeMeshtasticAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(adapter_id: str = "fake-drain") -> AdapterContext:
    """Build a minimal AdapterContext for testing."""
    return AdapterContext(
        adapter_id=adapter_id,
        publish_inbound=AsyncMock(),
        logger=logging.getLogger(f"test.{adapter_id}"),
        clock=lambda: datetime.now(timezone.utc),
        shutdown_event=asyncio.Event(),
    )


def _expire_drain_window(adapter: FakeMeshtasticAdapter) -> None:
    """Move the drain deadline one second into the monotonic past."""
    adapter._startup_drain_deadline_monotonic = time.monotonic() - 1.0


# ---------------------------------------------------------------------------
# Packets inside the window are dropped
# ---------------------------------------------------------------------------


class TestFakeDrainDropsWithinWindow:
    """Relay-classified packets are dropped while the fake's window is open."""

    async def test_packet_dropped_simulate_inbound(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(make_meshtastic_text_packet(text="backlog"))

        ctx.publish_inbound.assert_not_called()
        assert adapter.inbound_events == []
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()

    async def test_packet_without_rx_time_dropped(self) -> None:
        """Packets lacking rxTime are dropped — the rxTime gate cannot judge them."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        packet = make_meshtastic_text_packet(text="no rxTime")
        assert "rxTime" not in packet
        await adapter.simulate_inbound(packet)

        ctx.publish_inbound.assert_not_called()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1

        await adapter.stop()

    async def test_stale_rx_time_packet_counts_as_drain_drop(self) -> None:
        """Drain-dropped packets never enter the rxTime suppression counters."""
        config = make_meshtastic_config(
            startup_backlog_suppress_seconds=10.0,
            startup_packet_drain_seconds=30.0,
        )
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        packet = make_meshtastic_text_packet(text="old")
        packet["rxTime"] = adapter._adapter_start_epoch - 1000.0
        await adapter.simulate_inbound(packet)

        diag = adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 1
        assert diag["startup_backlog_packets_seen"] == 0
        assert diag["startup_backlog_packets_suppressed"] == 0
        ctx.publish_inbound.assert_not_called()

        await adapter.stop()

    async def test_non_relay_packets_not_gated(self) -> None:
        """Non-relay classifications are returned before the drain gate runs."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(
            {
                "fromId": "!node1",
                "id": 1,
                "decoded": {"portnum": "telemetry"},
            }
        )

        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 0
        ctx.publish_inbound.assert_not_called()

        await adapter.stop()


# ---------------------------------------------------------------------------
# Expiry: packets relay again, completion logged once
# ---------------------------------------------------------------------------


class TestFakeDrainExpiry:
    """After the window expires, packets relay and the window stays closed."""

    async def test_packet_after_expiry_relayed(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))

        ctx.publish_inbound.assert_called_once()
        assert len(adapter.inbound_events) == 1
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 0

        await adapter.stop()

    async def test_completion_info_logged_once(self, caplog) -> None:
        """The drain-end completion line is emitted exactly once."""
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        with caplog.at_level(logging.INFO, logger="medre.adapters.fakes.meshtastic"):
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
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(
            make_meshtastic_text_packet(text="backlog", packet_id=1)
        )
        _expire_drain_window(adapter)
        with caplog.at_level(logging.INFO, logger="medre.adapters.fakes.meshtastic"):
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
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        assert adapter._startup_drain_deadline_monotonic is not None
        _expire_drain_window(adapter)

        assert adapter._check_startup_packet_drain(1) is False
        assert adapter._startup_drain_deadline_monotonic is None
        assert adapter._check_startup_packet_drain(2) is False

        await adapter.stop()

    async def test_rx_time_suppression_untouched_behind_drain(self) -> None:
        """With the drain expired, stale rxTime packets are still suppressed."""
        config = make_meshtastic_config(
            startup_backlog_suppress_seconds=10.0,
            startup_packet_drain_seconds=30.0,
        )
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        _expire_drain_window(adapter)
        packet = make_meshtastic_text_packet(text="stale", packet_id=1)
        packet["rxTime"] = adapter._adapter_start_epoch - 1000.0
        await adapter.simulate_inbound(packet)

        ctx.publish_inbound.assert_not_called()
        diag = adapter.diagnostics()
        assert diag["startup_backlog_packets_seen"] == 1
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert adapter.inbound_events == []

        await adapter.stop()


# ---------------------------------------------------------------------------
# Disabled drain (window == 0)
# ---------------------------------------------------------------------------


class TestFakeDisabledDrain:
    """startup_packet_drain_seconds == 0 disables the fake's drain."""

    async def test_zero_window_leaves_deadline_unarmed(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        assert adapter._startup_drain_deadline_monotonic is None

        await adapter.stop()

    async def test_zero_window_relays_immediately(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))

        ctx.publish_inbound.assert_called_once()
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 0

        await adapter.stop()


# ---------------------------------------------------------------------------
# Arming, defaults, restart, and diagnostics
# ---------------------------------------------------------------------------


class TestFakeDrainArmingAndDiagnostics:
    """Deadline arming, session resets, and evidence exposure mirror the real adapter."""

    async def test_production_default_arms_deadline(self) -> None:
        """The default 15.0 window arms a future deadline on start."""

        adapter = FakeMeshtasticAdapter()
        assert adapter._config.startup_packet_drain_seconds == 15.0
        ctx = _make_context()
        await adapter.start(ctx)

        deadline = adapter._startup_drain_deadline_monotonic
        assert deadline is not None
        assert deadline > time.monotonic()

        await adapter.stop()

    async def test_diagnostics_expose_drain_evidence(self) -> None:
        config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
        adapter = FakeMeshtasticAdapter(config)
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
        adapter = FakeMeshtasticAdapter(config)
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


async def test_repeated_start_does_not_rearm_completed_drain() -> None:
    """Like the real adapter, an already-running fake retains its baseline."""
    config = make_meshtastic_config(startup_packet_drain_seconds=30.0)
    adapter = FakeMeshtasticAdapter(config)
    ctx = _make_context()
    try:
        await adapter.start(ctx)
        await adapter.simulate_inbound(make_meshtastic_text_packet(text="backlog"))
        _expire_drain_window(adapter)
        assert adapter._check_startup_packet_drain(1) is False
        baseline = adapter._adapter_start_epoch
        await adapter.start(ctx)
        assert adapter._adapter_start_epoch == baseline
        assert adapter._startup_drain_deadline_monotonic is None
        assert adapter.diagnostics()["startup_drain_packets_dropped"] == 1
        await adapter.simulate_inbound(make_meshtastic_text_packet(text="live"))
        ctx.publish_inbound.assert_awaited_once()
    finally:
        await adapter.stop()

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
from unittest.mock import AsyncMock

from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.async_utils import wait_until
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_text_packet,
)

from medre.adapters.meshtastic.adapter import MeshtasticAdapter

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

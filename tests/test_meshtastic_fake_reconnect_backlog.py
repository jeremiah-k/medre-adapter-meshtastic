"""FakeMeshtasticAdapter mirrors the reconnect receive-watermark policy.

The fake exists so core fixtures and fake-tier tests can exercise the
meshtastic policy surface without a session or SDK.  These tests pin the
mirrored behavior: per-generation receive-ready watermarks, rxTime judged
against the packet's own generation, and the generation-scoped
unprovable-packet bootstrap window armed by :meth:`simulate_reconnect`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_text_packet,
)

from medre.adapters.fakes import meshtastic as fake_module
from medre.adapters.fakes.meshtastic import FakeMeshtasticAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(adapter_id: str = "fake-reconnect") -> AdapterContext:
    """Build a minimal AdapterContext for testing."""
    return AdapterContext(
        adapter_id=adapter_id,
        publish_inbound=AsyncMock(),
        logger=logging.getLogger(f"test.{adapter_id}"),
        clock=lambda: datetime.now(timezone.utc),
        shutdown_event=asyncio.Event(),
    )


def _text(text: str, packet_id: int, rx_time: float | None) -> dict:
    packet = make_meshtastic_text_packet(text=text, packet_id=packet_id)
    if rx_time is not None:
        packet["rxTime"] = rx_time
    return packet


def _expire_drain_window(adapter: FakeMeshtasticAdapter) -> None:
    """Move the startup drain deadline into the monotonic past.

    Mirrors the established fake-drain test pattern: the default 15s
    startup drain is settled lazily so reconnect packets reach the
    watermark gates under the production configuration.
    """
    adapter._startup_drain_deadline_monotonic = time.monotonic() - 1.0


# ---------------------------------------------------------------------------
# Watermark and window semantics mirror the real adapter
# ---------------------------------------------------------------------------


class TestFakeReconnectWatermark:
    """Per-generation watermark behavior behind simulate_reconnect()."""

    async def test_start_records_generation_one_watermark(self) -> None:
        adapter = FakeMeshtasticAdapter(make_meshtastic_config())
        ctx = _make_context()
        await adapter.start(ctx)

        assert adapter._connection_generation == 1
        assert adapter.diagnostics()["receive_ready_generation"] == 1
        assert adapter.diagnostics()["reconnect_receive_bootstrap_seconds"] == 5.0
        watermark = adapter._receive_ready_epochs[1]
        assert watermark <= time.time()

        await adapter.stop()

    async def test_queued_backlog_with_rxtime_suppressed_after_reconnect(self) -> None:
        """A pre-reconnect rxTime is judged against the NEW watermark."""
        adapter = FakeMeshtasticAdapter(make_meshtastic_config())
        ctx = _make_context()
        await adapter.start(ctx)
        _expire_drain_window(adapter)

        adapter.simulate_reconnect()
        generation = adapter._connection_generation
        watermark = adapter._receive_ready_epochs[generation]
        backlog = _text("queued", 1, watermark - 60.0)
        await adapter.simulate_inbound(backlog)

        diag = adapter.diagnostics()
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert diag["receive_ready_generation"] == generation
        assert adapter.inbound_events == []

        await adapter.stop()

    async def test_fresh_rxtime_relays_inside_bootstrap_window(self) -> None:
        """Provably fresh traffic relays immediately after reconnect."""
        adapter = FakeMeshtasticAdapter(make_meshtastic_config())
        ctx = _make_context()
        await adapter.start(ctx)
        _expire_drain_window(adapter)

        generation = adapter.simulate_reconnect()
        watermark = adapter._receive_ready_epochs[generation]
        fresh = _text("live", 1, watermark + 1.0)
        await adapter.simulate_inbound(fresh)

        diag = adapter.diagnostics()
        assert diag["reconnect_backlog_packets_dropped"] == 0
        assert diag["startup_backlog_packets_suppressed"] == 0
        assert len(adapter.inbound_events) == 1

        await adapter.stop()

    async def test_missing_rxtime_dropped_inside_window_only(self, caplog) -> None:
        """Unprovable packets drop in the window, relay after lazy expiry."""
        config = make_meshtastic_config(startup_packet_drain_seconds=15.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)
        _expire_drain_window(adapter)

        generation = adapter.simulate_reconnect()
        assert adapter._reconnect_bootstrap is not None
        await adapter.simulate_inbound(_text("in window", 1, None))
        assert adapter.diagnostics()["reconnect_backlog_packets_dropped"] == 1
        assert adapter.inbound_events == []

        adapter._reconnect_bootstrap = (generation, time.monotonic() - 1.0, 1)
        with caplog.at_level(logging.INFO, logger="medre.adapters.fakes.meshtastic"):
            await adapter.simulate_inbound(_text("after window", 2, None))

        completion_lines = [
            rec
            for rec in caplog.records
            if "reconnect receive bootstrap window ended" in rec.getMessage()
        ]
        assert len(completion_lines) == 1
        assert "dropped=1" in completion_lines[0].getMessage()
        assert len(adapter.inbound_events) == 1

        await adapter.stop()

    async def test_drain_zero_disables_window_but_not_watermark(self) -> None:
        """startup_packet_drain_seconds == 0: no window; watermark still applies."""
        config = make_meshtastic_config(startup_packet_drain_seconds=0.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)

        adapter.simulate_reconnect()
        generation = adapter._connection_generation
        watermark = adapter._receive_ready_epochs[generation]
        assert adapter._reconnect_bootstrap is None

        await adapter.simulate_inbound(_text("no rxTime", 1, None))
        assert len(adapter.inbound_events) == 1
        assert adapter.diagnostics()["reconnect_backlog_packets_dropped"] == 0

        await adapter.simulate_inbound(_text("queued", 2, watermark - 60.0))
        assert adapter.diagnostics()["startup_backlog_packets_suppressed"] == 1
        assert len(adapter.inbound_events) == 1

        await adapter.stop()

    async def test_explicit_generation_gates_against_that_generation(
        self, monkeypatch
    ) -> None:
        """A generation passed to simulate_inbound judges against its watermark.

        With the two watermarks separated in wall-clock time, a packet
        stamped mid-outage is stale against the reconnected generation and
        fresh against the retired one — the generation, not the adapter,
        selects the baseline.
        """
        clock = SimpleNamespace(wall=1_800_000_000.0, monotonic=1000.0)
        monkeypatch.setattr(
            fake_module,
            "time",
            SimpleNamespace(time=lambda: clock.wall, monotonic=lambda: clock.monotonic),
        )
        config = make_meshtastic_config(startup_packet_drain_seconds=0.0)
        adapter = FakeMeshtasticAdapter(config)
        ctx = _make_context()
        await adapter.start(ctx)
        generation_one = adapter._connection_generation
        watermark_one = adapter._receive_ready_epochs[generation_one]

        clock.wall += 3600
        clock.monotonic += 3600
        adapter.simulate_reconnect()
        generation_two = adapter._connection_generation
        watermark_two = adapter._receive_ready_epochs[generation_two]
        assert watermark_two == watermark_one + 3600

        mid_outage = _text("mid outage", 1, watermark_one + 1800.0)
        await adapter.simulate_inbound(mid_outage, connection_generation=generation_two)
        assert adapter.diagnostics()["startup_backlog_packets_suppressed"] == 1
        assert adapter.inbound_events == []

        await adapter.simulate_inbound(mid_outage, connection_generation=generation_one)
        assert adapter.diagnostics()["startup_backlog_packets_suppressed"] == 1
        assert len(adapter.inbound_events) == 1

        await adapter.stop()

    async def test_reconnect_requires_started_adapter(self) -> None:
        adapter = FakeMeshtasticAdapter(make_meshtastic_config())
        try:
            adapter.simulate_reconnect()
        except RuntimeError as exc:
            assert "has not been started" in str(exc)
        else:
            raise AssertionError("simulate_reconnect before start must raise")


async def test_fake_window_constants_match_real_adapter() -> None:
    """The fake mirrors the real adapter's reconnect policy constants."""
    from medre.adapters.meshtastic import adapter as real_adapter_module

    assert (
        fake_module._RECONNECT_RECEIVE_BOOTSTRAP_SECONDS
        == real_adapter_module._RECONNECT_RECEIVE_BOOTSTRAP_SECONDS
    )
    assert (
        fake_module._RECEIVE_READY_EPOCH_HISTORY
        == real_adapter_module._RECEIVE_READY_EPOCH_HISTORY
    )

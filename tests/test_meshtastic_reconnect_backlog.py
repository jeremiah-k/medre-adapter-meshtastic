"""Reconnect receive-watermark policy: queued backlog must not relay as fresh.

After a transparent reconnect the radio may empty its FROMRADIO queue of
pre-reconnect packets.  The startup packet drain window is long expired and
the ``rxTime`` suppression gate was anchored only at the ORIGINAL receive-ready
epoch, so queued pre-reconnect traffic with a valid pre-disconnect ``rxTime``
passed both gates and relayed as fresh traffic.

The reconnect receive-watermark policy closes that gap while preserving
legitimate post-reconnect traffic:

* Every receive-ready connection generation records its own wall-clock
  watermark, and the ``rxTime`` gate judges each packet against its own
  generation's watermark.
* A short generation-scoped monotonic bootstrap window drops packets that
  cannot prove their age (no usable ``rxTime``) right after a reconnect;
  provably fresh packets are never dropped by it, so a reconnect never
  spends a blind content-independent drain on live packets.

Behavioral reference: mmrelay's per-connection ``RELAY_START_TIME`` reset and
its reconnect pre-start bootstrap window — matched as a behavior class, not
an implementation.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

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

_INITIAL_WALL = 1_800_000_000.0
_INITIAL_MONOTONIC = 1000.0


# ---------------------------------------------------------------------------
# Harness: deterministic clock, real session, scripted installs
# ---------------------------------------------------------------------------


class _Harness:
    """Drives a real MeshtasticSession with a fully controlled clock.

    Connects are monkeypatched to hand out scripted fake clients; the
    adapter's module-level ``time`` namespace is replaced so wall-clock
    and monotonic decisions advance only when the test advances them.
    """

    def __init__(
        self,
        monkeypatch,
        *,
        drain_seconds: float = 15.0,
        suppress_seconds: float = 5.0,
        subscribe_errors: dict[int, Exception] | None = None,
    ) -> None:
        self.clock = SimpleNamespace(wall=_INITIAL_WALL, monotonic=_INITIAL_MONOTONIC)
        monkeypatch.setattr(
            adapter_module,
            "time",
            SimpleNamespace(
                time=lambda: self.clock.wall, monotonic=lambda: self.clock.monotonic
            ),
        )
        monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
        monkeypatch.setattr(
            MeshtasticSession, "_reconnect_delay", lambda self, attempt: 0
        )
        self.clients: list[SimpleNamespace] = []
        self.installs = 0
        self.subscribe_errors = subscribe_errors or {}
        self._subscribe_originals: list[tuple[int, float]] = []
        monkeypatch.setattr(
            MeshtasticSession, "_create_client_offloop", lambda session: self._connect()
        )
        monkeypatch.setattr(
            MeshtasticSession, "_subscribe_callbacks", lambda session: self._subscribe()
        )
        self.adapter = MeshtasticAdapter(
            make_meshtastic_config(
                connection_type="tcp",
                host="localhost",
                sync_timeout_ms=10,
                tcp_liveness_interval_seconds=0,
                startup_packet_drain_seconds=drain_seconds,
                startup_backlog_suppress_seconds=suppress_seconds,
            )
        )
        adapter_id = self.adapter.adapter_id
        self.ctx = AdapterContext(
            adapter_id=adapter_id,
            publish_inbound=AsyncMock(),
            logger=logging.getLogger(f"test.{adapter_id}"),
            clock=lambda: datetime.now(timezone.utc),
            shutdown_event=asyncio.Event(),
        )

    # -- Session script -----------------------------------------------------

    async def _connect(self) -> Any:
        await asyncio.sleep(0)
        client = SimpleNamespace(close=MagicMock())
        self.clients.append(client)
        return client

    def _subscribe(self) -> None:
        self.installs += 1
        self._subscribe_originals.append(
            (self.installs, self.clock.wall, self.clock.monotonic)
        )
        error = self.subscribe_errors.get(self.installs)
        if error is not None:
            raise error

    # -- Operations ----------------------------------------------------------

    @property
    def session(self) -> MeshtasticSession:
        session = self.adapter._session
        assert session is not None
        return session

    def receive_ready_epoch(self, generation: int) -> float:
        epoch = self.adapter._receive_ready_epochs.get(generation)
        assert epoch is not None
        return epoch

    async def start(self) -> None:
        await self.adapter.start(self.ctx)
        assert self.installs == 1
        assert self.session.connected

    async def reconnect(self) -> tuple[int, SimpleNamespace]:
        """Drop the transport and wait for the transparent reconnect."""
        generation_before = self.session.connection_generation
        self.session.notify_connection_lost()
        assert await wait_until(
            lambda: self.session.connected
            and not self.session.reconnecting
            and self.session.connection_generation != generation_before
        )
        generation = self.session.connection_generation
        return generation, self.clients[-1]

    def on_receive(self, client: SimpleNamespace, packet: dict[str, Any]) -> None:
        self.session._on_receive(packet, interface=client)


def _text(text: str, packet_id: int, rx_time: float | None) -> dict[str, Any]:
    packet = make_meshtastic_text_packet(text=text, packet_id=packet_id)
    if rx_time is not None:
        packet["rxTime"] = rx_time
    return packet


# ---------------------------------------------------------------------------
# The gap: queued pre-reconnect traffic relays as fresh
# ---------------------------------------------------------------------------


async def test_queued_prereconnect_packet_with_rxtime_does_not_relay(
    monkeypatch,
) -> None:
    """A queued packet older than the reconnect watermark must be suppressed.

    The radio hears a packet a minute before the transport drops, reconnects
    transparently, and then drains its FROMRADIO queue to the fresh session.
    Under the startup-only baseline this packet relays as fresh; the
    reconnect watermark policy suppresses it instead.
    """
    harness = _Harness(monkeypatch)
    try:
        await harness.start()

        # Settle the initial connection: let the startup drain expire lazily
        # with a steady-state live packet.
        harness.clock.wall += 20
        harness.clock.monotonic += 20
        harness.on_receive(harness.clients[0], _text("steady", 1, None))
        assert await wait_until(lambda: harness.ctx.publish_inbound.await_count == 1)
        assert harness.adapter.diagnostics()["startup_drain_packets_dropped"] == 0

        # One hour later the transport drops and reconnects transparently.
        harness.clock.wall += 3600
        harness.clock.monotonic += 3600
        generation, client = await harness.reconnect()
        watermark = harness.receive_ready_epoch(generation)
        assert watermark == harness.clock.wall

        # The radio empties its FROMRADIO queue: a packet it heard one minute
        # before the disconnect reaches the fresh generation with a valid,
        # well-formed rxTime.
        harness.on_receive(client, _text("queued", 2, harness.clock.wall - 60))

        diag = harness.adapter.diagnostics()
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert harness.ctx.publish_inbound.await_count == 1
    finally:
        await harness.adapter.stop()


# ---------------------------------------------------------------------------
# Wall-clock edge matrix (rxTime vs the generation watermark)
# ---------------------------------------------------------------------------


class TestWallClockEdgeMatrix:
    """rxTime decisions run against the packet's own generation watermark."""

    async def test_valid_fresh_rxtime_relays_inside_bootstrap_window(
        self, monkeypatch
    ) -> None:
        """Provably fresh traffic relays immediately after reconnect.

        This is the property that separates the watermark policy from a
        blind drain re-arm: a live packet arriving one second after the
        reconnect — while the bootstrap window is still open — is published.
        """
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            fresh = _text("live after reconnect", 2, watermark + 1.0)
            harness.on_receive(client, fresh)
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 2
            )
            diag = harness.adapter.diagnostics()
            assert diag["reconnect_backlog_packets_dropped"] == 0
            assert diag["startup_backlog_packets_suppressed"] == 0
        finally:
            await harness.adapter.stop()

    async def test_rxtime_within_suppress_tolerance_of_watermark_relays(
        self, monkeypatch
    ) -> None:
        """rxTime slightly before the watermark (inside the tolerance) relays.

        ``startup_backlog_suppress_seconds`` doubles as the clock-skew
        tolerance: a packet stamped a few seconds before the receive-ready
        watermark cannot be distinguished from queue backlog by a
        trailing radio clock, so only strictly older packets are judged
        stale.
        """
        harness = _Harness(monkeypatch, suppress_seconds=5.0)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            harness.on_receive(client, _text("in tolerance", 2, watermark - 3.0))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 2
            )
        finally:
            await harness.adapter.stop()

    async def test_future_rxtime_relays_after_reconnect(self, monkeypatch) -> None:
        """A timestamp ahead of the host clock is never judged stale.

        Future timestamps indicate a radio clock running ahead of the host,
        not queue backlog; the trusted local radio's traffic is accepted.
        The honest tradeoff: backlog on an ahead-skewed radio cannot be
        distinguished from live traffic by rxTime and relays.
        """
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            harness.on_receive(client, _text("ahead clock", 2, watermark + 900.0))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 2
            )
            diag = harness.adapter.diagnostics()
            assert diag["startup_backlog_packets_suppressed"] == 0
        finally:
            await harness.adapter.stop()

    async def test_large_radio_clock_trail_can_suppress_fresh_traffic(
        self, monkeypatch
    ) -> None:
        """Honest skew edge: a radio clock trailing beyond the tolerance
        can have genuinely fresh packets suppressed after a reconnect.

        The suppression window is the skew tolerance by design; this
        failure mode exists identically at the initial connection today
        and is stated, not hidden.
        """
        harness = _Harness(monkeypatch, suppress_seconds=5.0)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            # Radio clock trails the host by 30s: a packet received right
            # now stamps at watermark - 30, past the 5s tolerance.
            harness.on_receive(client, _text("trailing clock", 2, watermark - 30.0))
            diag = harness.adapter.diagnostics()
            assert diag["startup_backlog_packets_suppressed"] == 1
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()


# ---------------------------------------------------------------------------
# Monotonic bootstrap window (unprovable ages right after a reconnect)
# ---------------------------------------------------------------------------


class TestReconnectBootstrapWindow:
    """The generation-scoped window drops only ages it cannot judge."""

    async def test_missing_rxtime_dropped_inside_window(self, monkeypatch) -> None:
        """A packet without rxTime arriving in the window is dropped.

        It cannot prove it is not pre-reconnect queue backlog; the loss is
        bounded to the short window and counted for evidence.
        """
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()

            harness.on_receive(client, _text("no rxTime", 2, None))

            diag = harness.adapter.diagnostics()
            assert diag["reconnect_backlog_packets_dropped"] == 1
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()

    async def test_malformed_rxtime_dropped_inside_window(self, monkeypatch) -> None:
        """A malformed rxTime cannot prove age either; the window drops it."""
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()

            malformed = _text("bad rxTime", 2, None)
            malformed["rxTime"] = "not-a-number"
            harness.on_receive(client, malformed)

            assert (
                harness.adapter.diagnostics()["reconnect_backlog_packets_dropped"] == 1
            )
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()

    async def test_missing_rxtime_relays_after_window_expiry_with_single_completion(
        self, monkeypatch, caplog
    ) -> None:
        """After the window lazily expires, unprovable packets relay again.

        The completion INFO line fires exactly once with this window's
        drop count, and the window stays closed for the generation.
        """
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()

            harness.on_receive(client, _text("unprovable in window", 2, None))
            assert harness.ctx.publish_inbound.await_count == 1

            # Advance past the 5s bootstrap window; expiry is lazy.
            harness.clock.monotonic += 10
            harness.clock.wall += 10
            with caplog.at_level(logging.INFO, logger="test.mesh-1"):
                harness.on_receive(client, _text("unprovable after window", 3, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 2
            )
            completion_lines = [
                rec
                for rec in caplog.records
                if "reconnect receive bootstrap window ended" in rec.getMessage()
            ]
            assert len(completion_lines) == 1
            assert "dropped=1" in completion_lines[0].getMessage()
            # A second unprovable packet relays too; the window stays closed.
            harness.on_receive(client, _text("unprovable later", 4, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 3
            )
        finally:
            await harness.adapter.stop()

    async def test_repeated_reconnects_record_fresh_watermarks_and_rearm(
        self, monkeypatch
    ) -> None:
        """Each reconnect judges against its own watermark and window.

        Backlog from the first outage must not pass against the second
        reconnect's baseline, and each window independently drops
        unprovable packets (cumulative evidence counts both).
        """
        harness = _Harness(monkeypatch)
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            # First reconnect: unprovable packet dropped in the window.
            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation_one, client_one = await harness.reconnect()
            watermark_one = harness.receive_ready_epoch(generation_one)
            assert watermark_one == _INITIAL_WALL + 60 + 3600
            harness.on_receive(client_one, _text("outage one backlog", 2, None))
            assert (
                harness.adapter.diagnostics()["reconnect_backlog_packets_dropped"] == 1
            )

            # Second reconnect ten minutes later: the radio drains a packet
            # heard between the reconnects (stale against the NEW watermark
            # even though it postdates the first one).
            harness.clock.wall += 600
            harness.clock.monotonic += 600
            generation_two, client_two = await harness.reconnect()
            watermark_two = harness.receive_ready_epoch(generation_two)
            assert watermark_two == watermark_one + 600
            assert generation_two != generation_one
            harness.on_receive(
                client_two, _text("between outages", 3, watermark_two - 30.0)
            )
            diag = harness.adapter.diagnostics()
            assert diag["startup_backlog_packets_suppressed"] == 1
            assert diag["reconnect_backlog_packets_dropped"] == 1
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()

    async def test_failed_reconnect_install_does_not_break_next_window(
        self, monkeypatch
    ) -> None:
        """A failed install between successes leaves the policy coherent.

        The failed generation's watermark is recorded but its generation
        can never pass the ownership fence again; the next successful
        reconnect records its own watermark and window, and backlog
        suppression works against it.
        """
        harness = _Harness(
            monkeypatch,
            subscribe_errors={2: MeshtasticConnectionError("subscribe failed")},
        )
        try:
            await harness.start()
            harness.clock.wall += 60
            harness.clock.monotonic += 60
            harness.on_receive(harness.clients[0], _text("steady", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            # Install 2 failed; install 3 succeeded.
            assert harness.installs == 3
            watermark = harness.receive_ready_epoch(generation)
            assert watermark == harness.clock.wall

            harness.on_receive(client, _text("queued backlog", 2, watermark - 60.0))
            diag = harness.adapter.diagnostics()
            assert diag["startup_backlog_packets_suppressed"] == 1
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()


# ---------------------------------------------------------------------------
# Operator opt-outs and lifecycle fences
# ---------------------------------------------------------------------------


class TestOptOutsAndLifecycle:
    """Config opt-outs, restarts, and retired lifecycle hooks."""

    async def test_drain_zero_disables_bootstrap_but_not_watermark(
        self, monkeypatch
    ) -> None:
        """startup_packet_drain_seconds == 0 opts out of connect-window drops.

        The unprovable-packet window does not arm, so a missing-rxTime
        packet right after a reconnect relays; the wall-clock watermark is
        still recorded and still suppresses provably old backlog.
        """
        harness = _Harness(monkeypatch, drain_seconds=0.0)
        try:
            await harness.start()
            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            harness.on_receive(client, _text("no rxTime", 1, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )

            harness.on_receive(client, _text("queued backlog", 2, watermark - 60.0))
            diag = harness.adapter.diagnostics()
            assert diag["startup_backlog_packets_suppressed"] == 1
            assert diag["reconnect_backlog_packets_dropped"] == 0
            assert harness.ctx.publish_inbound.await_count == 1
        finally:
            await harness.adapter.stop()

    async def test_suppress_zero_keeps_bootstrap_window(self, monkeypatch) -> None:
        """startup_backlog_suppress_seconds == 0 disables only the wall gate.

        The monotonic bootstrap window is a separate drop family and still
        bounds unprovable ages right after a reconnect.
        """
        harness = _Harness(monkeypatch, suppress_seconds=0.0)
        try:
            await harness.start()
            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            watermark = harness.receive_ready_epoch(generation)

            harness.on_receive(client, _text("no rxTime", 1, None))
            assert (
                harness.adapter.diagnostics()["reconnect_backlog_packets_dropped"] == 1
            )
            assert harness.ctx.publish_inbound.await_count == 0

            harness.clock.monotonic += 10
            harness.clock.wall += 10
            harness.on_receive(
                client, _text("old beyond watermark", 2, watermark - 600.0)
            )
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )
            assert (
                harness.adapter.diagnostics()["startup_backlog_packets_suppressed"] == 0
            )
        finally:
            await harness.adapter.stop()

    async def test_pending_degraded_start_records_no_watermark(
        self, monkeypatch
    ) -> None:
        """A pending first connection installs nothing and records nothing.

        The watermark, like the drain baseline, is spent by receive-ready
        connections — never by offline time before the first one.
        """
        ready = asyncio.Event()
        native = SimpleNamespace(close=MagicMock())
        harness = _Harness(monkeypatch)

        async def pending_connect(session: MeshtasticSession) -> Any:
            await ready.wait()
            return native

        monkeypatch.setattr(
            MeshtasticSession, "_create_client_offloop", pending_connect
        )
        try:
            await harness.adapter.start(harness.ctx)
            assert harness.session.reconnecting
            diag = harness.adapter.diagnostics()
            assert diag["receive_ready_generation"] is None
            assert diag["reconnect_backlog_packets_dropped"] == 0

            ready.set()
            assert await wait_until(lambda: harness.session.connected)
            diag = harness.adapter.diagnostics()
            assert (
                diag["receive_ready_generation"]
                == harness.session.connection_generation
            )
            assert diag["reconnect_receive_bootstrap_seconds"] == 5.0
        finally:
            ready.set()
            await harness.adapter.stop()

    async def test_restart_resets_watermarks_and_counters(self, monkeypatch) -> None:
        """stop()+start() starts a fresh watermark history and counters."""
        harness = _Harness(monkeypatch)
        ctx = harness.ctx
        try:
            await harness.start()
            harness.clock.wall += 3600
            harness.clock.monotonic += 3600
            generation, client = await harness.reconnect()
            harness.on_receive(client, _text("no rxTime", 1, None))
            diag = harness.adapter.diagnostics()
            assert diag["reconnect_backlog_packets_dropped"] == 1
            assert diag["receive_ready_generation"] == generation

            await harness.adapter.stop()
            await harness.adapter.start(ctx)

            diag = harness.adapter.diagnostics()
            assert diag["reconnect_backlog_packets_dropped"] == 0
            assert diag["receive_ready_generation"] == 1
            assert harness.adapter._receive_ready_epochs == {
                1: diag["adapter_start_epoch"]
            }
        finally:
            await harness.adapter.stop()

    async def test_retired_lifecycle_hook_cannot_record_watermark(
        self, monkeypatch
    ) -> None:
        """A stale session's connection-ready hook is inert after restart."""
        harness = _Harness(monkeypatch)
        ctx = harness.ctx
        try:
            await harness.start()
            session = harness.session
            old_hook = session._connection_ready_callback
            assert old_hook is not None
            old_generation = session.connection_generation
            await harness.adapter.stop()
            await harness.adapter.start(ctx)
            assert harness.adapter._receive_ready_epochs == {
                harness.session.connection_generation: harness.clock.wall
            }
            old_hook(old_generation + 100, False)
            assert len(harness.adapter._receive_ready_epochs) == 1
            assert (
                harness.adapter.diagnostics()["receive_ready_generation"]
                == harness.session.connection_generation
            )
        finally:
            await harness.adapter.stop()


# ---------------------------------------------------------------------------
# Session hook ordering
# ---------------------------------------------------------------------------


async def test_connection_ready_hook_fires_before_first_receive_dispatch(
    monkeypatch,
) -> None:
    """The watermark exists before the installed client can deliver a packet.

    Subscribing is where the first receive callback can dispatch; the
    generation's watermark must already be recorded at that point.
    """
    harness = _Harness(monkeypatch)
    observed: list[int] = []
    original_subscribe = MeshtasticSession._subscribe_callbacks

    def subscribe_and_observe(session: MeshtasticSession) -> None:
        observed.append(session.connection_generation)
        assert session.connection_generation in harness.adapter._receive_ready_epochs
        original_subscribe(session)

    monkeypatch.setattr(
        MeshtasticSession, "_subscribe_callbacks", subscribe_and_observe
    )
    try:
        await harness.start()
        harness.clock.wall += 60
        harness.clock.monotonic += 60
        harness.on_receive(harness.clients[0], _text("steady", 1, None))
        assert await wait_until(lambda: harness.ctx.publish_inbound.await_count == 1)
        harness.clock.wall += 3600
        harness.clock.monotonic += 3600
        await harness.reconnect()
        assert observed == [1, harness.session.connection_generation]
    finally:
        await harness.adapter.stop()


async def test_rapid_reconnect_retires_initial_drain(monkeypatch) -> None:
    harness = _Harness(monkeypatch)
    try:
        await harness.start()
        harness.on_receive(harness.clients[0], _text("initial", 1, harness.clock.wall))
        assert harness.adapter.diagnostics()["startup_drain_packets_dropped"] == 1
        original_deadline = harness.adapter._startup_drain_deadline_monotonic
        assert original_deadline is not None
        harness.clock.wall += 2
        harness.clock.monotonic += 2
        generation, client = await harness.reconnect()
        assert harness.clock.monotonic < original_deadline
        assert harness.adapter._startup_drain_deadline_monotonic is None
        watermark = harness.receive_ready_epoch(generation)
        harness.on_receive(client, _text("live", 2, watermark))
        assert await wait_until(lambda: harness.ctx.publish_inbound.await_count == 1)
        harness.on_receive(client, _text("unprovable", 3, None))
        harness.on_receive(client, _text("stale", 4, watermark - 60))
        assert harness.ctx.publish_inbound.await_count == 1
        diag = harness.adapter.diagnostics()
        assert diag["startup_drain_packets_dropped"] == 1
        assert diag["startup_backlog_packets_suppressed"] == 1
        assert diag["reconnect_backlog_packets_dropped"] == 1
    finally:
        await harness.adapter.stop()


async def test_generation_cutoff_survives_bootstrap_expiry(monkeypatch) -> None:
    harness = _Harness(monkeypatch, drain_seconds=15.0)
    try:
        await harness.start()
        harness.clock.wall += 3600
        harness.clock.monotonic += 3600
        generation, client = await harness.reconnect()
        watermark = harness.receive_ready_epoch(generation)
        stale_time = watermark - 1800
        harness.on_receive(client, _text("queued", 1, stale_time))
        harness.clock.wall += 60
        harness.clock.monotonic += 60
        harness.on_receive(client, _text("queued later", 2, stale_time))
        assert harness.adapter.diagnostics()["startup_backlog_packets_suppressed"] == 2
        assert harness.ctx.publish_inbound.await_count == 0
        harness.on_receive(client, _text("at cutoff", 3, watermark - 5))
        assert await wait_until(lambda: harness.ctx.publish_inbound.await_count == 1)
    finally:
        await harness.adapter.stop()


async def test_only_current_generation_settles_expired_bootstrap(
    monkeypatch, caplog
) -> None:
    harness = _Harness(monkeypatch)
    try:
        await harness.start()
        initial = harness.session.connection_generation
        harness.clock.wall += 20
        harness.clock.monotonic += 20
        generation, client = await harness.reconnect()
        harness.on_receive(client, _text("unknown", 1, None))
        harness.clock.wall += 5
        harness.clock.monotonic += 5
        with caplog.at_level(logging.INFO):
            assert not harness.adapter._check_reconnect_receive_bootstrap(
                _text("retired", 2, None), initial, 2
            )
            assert harness.adapter._reconnect_bootstrap is not None
            harness.on_receive(client, _text("fresh", 3, harness.clock.wall))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 1
            )
            harness.on_receive(client, _text("unknown later", 4, None))
            assert await wait_until(
                lambda: harness.ctx.publish_inbound.await_count == 2
            )
        assert harness.adapter._reconnect_bootstrap is None
        lines = [
            rec.getMessage()
            for rec in caplog.records
            if "reconnect receive bootstrap window ended" in rec.getMessage()
        ]
        assert len(lines) == 1
        assert "dropped=1" in lines[0]
        assert harness.adapter.diagnostics()["receive_ready_generation"] == generation
    finally:
        await harness.adapter.stop()

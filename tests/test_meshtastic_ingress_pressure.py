"""Boundary tests for bounded, loss-honest reader-thread ingress staging.

These tests pin the pre-scheduling reservation that bounds relayable
Meshtastic ingress: a held canonical publisher pins staged work so exact
refusal counts can be observed from a synthetic SDK reader thread. Every
refusal is counted and reported through the producer-pressure seam;
content-free warning summaries are rate-limited without hiding counts. No refusal
implies admission or a retry.

The pre-cap measurements recorded in ``docs/dev/ingress-pressure.md`` were
taken before any staging bound existed; these tests replace that unbounded
growth observation with the bounded contract. No radio, network, or SDK
install is required.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.contracts.adapter import AdapterContext

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    ProducerPressureReport = None
from medre_meshtastic_tests.helpers.async_utils import wait_until


def _packet(index: int) -> dict:
    """Build distinct relayable frames with no radio device or network."""
    return {
        "fromId": "!pressure-source",
        "toId": "",
        "channel": 0,
        "id": index + 1,
        "decoded": {"portnum": "text_message", "text": f"pressure {index}"},
    }


def _report(
    burst: int, staged: int, refused: int, delivered: int, elapsed: float
) -> None:
    """Optionally export a content-free boundary run for reproducible comparisons.

    The measured-budget qualification round reads these sanitized counters;
    they carry counts and elapsed local time only, never message bodies.
    """
    root = os.environ.get("MEDRE_PRESSURE_REPORT_DIR")
    if not root:
        return
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"meshtastic-{burst}.json").write_text(
        json.dumps(
            {
                "transport": "meshtastic",
                "tier": "synthetic_sdk_callback",
                "burst": burst,
                "staged": staged,
                "refused": refused,
                "delivered": delivered,
                "drained": delivered == staged,
                "elapsed_ms": round(elapsed * 1000, 2),
                "overflow_policy": "bounded_callback_capacity",
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


async def _start(
    adapter: MeshtasticAdapter,
    publish,
    reports: list[ProducerPressureReport] | None = None,
) -> None:
    """Start the adapter in fake mode with an optional producer-pressure sink."""
    ctx = AdapterContext(
        adapter_id=adapter.adapter_id,
        publish_inbound=publish,
        logger=logging.getLogger(f"test.meshtastic.ingress.{adapter.adapter_id}"),
        clock=lambda: datetime.now(UTC),
        shutdown_event=asyncio.Event(),
    )
    if reports is not None:
        ctx.report_producer_pressure = reports.append
    await adapter.start(ctx)


async def test_default_cap_stages_exactly_capacity_and_counts_overflow() -> None:
    """A 256-burst under held publication stages 100 and refuses 156 exactly."""
    gate = asyncio.Event()
    all_entered = asyncio.Event()
    seen: set[str] = set()
    published: set[str] = set()

    async def publish(event) -> None:
        seen.add(event.event_id)
        if len(seen) == 100:
            all_entered.set()
        await gate.wait()
        published.add(event.event_id)

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="ingress-boundary", connection_type="fake")
    )
    await _start(adapter, publish, reports)
    try:
        start = time.perf_counter()

        def callback_burst() -> None:
            for index in range(256):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(callback_burst)
        await asyncio.wait_for(all_entered.wait(), 10)

        # Exactly the default capacity is staged; every later packet is
        # refused before scheduling and never reaches the publisher.
        assert len(adapter._inbound_futures) == 100
        diagnostics = adapter.diagnostics()
        assert diagnostics["producer_overflow"] == 156
        assert diagnostics["inbound_published"] == 0
        assert diagnostics["inbound_staged_pending"] == 100
        assert diagnostics["max_pending_ingress_callbacks"] == 100
        assert len(seen) == 100

        overflow = [r for r in reports if r.outcome == "overflow"]
        scheduled = [r for r in reports if r.outcome == "scheduled"]
        assert len(overflow) == 156
        assert len(scheduled) == 100
        assert all(r.adapter_id == adapter.adapter_id for r in reports)
        assert all(r.count == 1 for r in overflow)
        assert all(r.reason == "callback_capacity" for r in overflow)
        assert all(r.reason is None for r in scheduled)

        gate.set()
        await asyncio.wait_for(
            asyncio.gather(
                *(asyncio.wrap_future(f) for f in tuple(adapter._inbound_futures))
            ),
            10,
        )
        assert adapter.diagnostics()["inbound_published"] == 100
        assert published == seen
        assert not adapter._inbound_futures
        _report(256, 100, 156, len(published), time.perf_counter() - start)
    finally:
        gate.set()
        await adapter.stop()


async def test_overflow_warnings_are_coalesced_without_losing_pressure(caplog) -> None:
    """One content-free warning per interval, but one fact per refused packet."""
    gate = asyncio.Event()
    entered = asyncio.Event()

    async def publish(_event) -> None:
        entered.set()
        await gate.wait()

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-warning-cap",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish, reports)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(0))
        await asyncio.wait_for(entered.wait(), 5)
        with caplog.at_level(
            logging.WARNING, logger="test.meshtastic.ingress.ingress-warning-cap"
        ):
            await asyncio.to_thread(
                lambda: [adapter._on_packet(_packet(i)) for i in range(1, 65)]
            )
            assert adapter.diagnostics()["producer_overflow"] == 64
            adapter._last_overflow_warning_at -= 6.0
            await asyncio.to_thread(adapter._on_packet, _packet(65))

        warnings = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "reader-thread ingress staging refused" in record.getMessage()
        ]
        assert len(warnings) == 2
        assert "total_refused=1" in warnings[0]
        assert "total_refused=65" in warnings[1]
        assert all("pressure-source" not in message for message in warnings)
        assert all("pressure 65" not in message for message in warnings)
        assert adapter.diagnostics()["producer_overflow"] == 65
        assert sum(report.outcome == "overflow" for report in reports) == 65
    finally:
        gate.set()
        await adapter.stop()


async def test_capacity_refusal_skips_node_lookup_and_codec(monkeypatch) -> None:
    """The already-full reader-thread path never enriches or decodes a packet."""
    gate = asyncio.Event()
    entered = asyncio.Event()

    async def publish(_event) -> None:
        entered.set()
        await gate.wait()

    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-no-decode",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(0))
        await asyncio.wait_for(entered.wait(), 5)
        enrich = Mock(side_effect=AssertionError("enrichment after refusal"))
        decode = Mock(side_effect=AssertionError("decode after refusal"))
        monkeypatch.setattr(adapter, "_enrich_with_node_info", enrich)
        monkeypatch.setattr(adapter._codec, "decode", decode)
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        enrich.assert_not_called()
        decode.assert_not_called()
        assert adapter.diagnostics()["producer_overflow"] == 1
        assert len(adapter._inbound_futures) == 1
    finally:
        gate.set()
        await adapter.stop()


async def test_decode_failure_releases_prescheduling_reservation(
    monkeypatch, caplog
) -> None:
    """A failed decoder must not strand reader-thread capacity."""
    gate = asyncio.Event()
    entered = asyncio.Event()

    async def publish(_event) -> None:
        entered.set()
        await gate.wait()

    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-decode-fail",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish)
    try:
        decode = adapter._codec.decode
        monkeypatch.setattr(
            adapter._codec, "decode", Mock(side_effect=ValueError("bad packet"))
        )
        with caplog.at_level(logging.ERROR):
            await asyncio.to_thread(adapter._on_packet, _packet(0))
        assert not adapter._inbound_futures
        assert adapter.diagnostics()["producer_overflow"] == 0
        monkeypatch.setattr(adapter._codec, "decode", decode)
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await asyncio.wait_for(entered.wait(), 5)
        assert len(adapter._inbound_futures) == 1
    finally:
        gate.set()
        await adapter.stop()


async def test_failed_scheduling_closes_coroutine_and_releases_slot(
    monkeypatch, caplog
) -> None:
    """An event-loop closure race neither leaks a coroutine nor a reservation."""
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-loop-race",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )

    async def publish(_event) -> None:
        raise AssertionError("must never publish after failed scheduling")

    await _start(adapter, publish)
    try:
        created = []

        def reject_submission(coro, _loop):
            created.append(coro)
            raise RuntimeError("event loop closed during submission")

        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", reject_submission)
        with caplog.at_level(logging.ERROR):
            await asyncio.to_thread(adapter._on_packet, _packet(0))
        assert len(created) == 1 and created[0].cr_frame is None
        assert not adapter._inbound_futures
        assert adapter.diagnostics()["producer_overflow"] == 0
        assert adapter._session is not None
        release = adapter._ingress_reservations.acquire(
            adapter._session.connection_generation
        )
        assert release is not None
        release()
    finally:
        await adapter.stop()


async def test_missing_loop_releases_slot_before_return() -> None:
    """No owner loop does not strand the reservation acquired before decode."""
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-no-loop",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )

    async def publish(_event) -> None:
        raise AssertionError("must not publish without a loop")

    await _start(adapter, publish)
    loop = adapter._loop
    try:
        adapter._loop = None
        await asyncio.to_thread(adapter._on_packet, _packet(0))
        assert not adapter._inbound_futures
        assert adapter._session is not None
        release = adapter._ingress_reservations.acquire(
            adapter._session.connection_generation
        )
        assert release is not None
        release()
    finally:
        adapter._loop = loop
        await adapter.stop()


async def test_below_cap_burst_stages_everything_and_drains_cleanly() -> None:
    """A 32-burst stays under the default cap and delivers in full after release."""
    gate = asyncio.Event()
    all_entered = asyncio.Event()
    seen: set[str] = set()
    published: set[str] = set()

    async def publish(event) -> None:
        seen.add(event.event_id)
        if len(seen) == 32:
            all_entered.set()
        await gate.wait()
        published.add(event.event_id)

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="ingress-below-cap", connection_type="fake")
    )
    await _start(adapter, publish, reports)
    try:
        start = time.perf_counter()

        def callback_burst() -> None:
            for index in range(32):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(callback_burst)
        await asyncio.wait_for(all_entered.wait(), 10)

        assert len(adapter._inbound_futures) == 32
        diagnostics = adapter.diagnostics()
        assert diagnostics["producer_overflow"] == 0
        assert diagnostics["inbound_published"] == 0
        assert [r.outcome for r in reports] == ["scheduled"] * 32

        gate.set()
        await asyncio.wait_for(
            asyncio.gather(
                *(asyncio.wrap_future(f) for f in tuple(adapter._inbound_futures))
            ),
            10,
        )
        assert adapter.diagnostics()["inbound_published"] == 32
        assert published == seen
        assert not adapter._inbound_futures
        _report(32, 32, 0, len(published), time.perf_counter() - start)
    finally:
        gate.set()
        await adapter.stop()


async def test_completed_publication_returns_its_reservation() -> None:
    """A finished publication frees its slot for the next packet."""
    gate = asyncio.Event()
    gates = {"active": gate}
    seen: list[str] = []

    async def publish(event) -> None:
        seen.append(event.event_id)
        await gates["active"].wait()

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-refill",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish, reports)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await wait_until(lambda: len(seen) == 1)

        # The only slot is held: the next packet is refused.
        await asyncio.to_thread(adapter._on_packet, _packet(2))
        assert len(adapter._inbound_futures) == 1
        assert adapter.diagnostics()["producer_overflow"] == 1

        gate.set()
        await wait_until(lambda: not adapter._inbound_futures)

        # The released slot stages the next packet again. The replacement
        # gate holds the new publication so the staged future stays pending.
        gate_two = asyncio.Event()
        gates["active"] = gate_two
        await asyncio.to_thread(adapter._on_packet, _packet(3))
        assert len(adapter._inbound_futures) == 1
        assert adapter.diagnostics()["producer_overflow"] == 1
        scheduled = [r for r in reports if r.outcome == "scheduled"]
        assert len(scheduled) == 2
        assert all(r.adapter_id == adapter.adapter_id for r in scheduled)
        gate_two.set()
    finally:
        gate.set()
        gates["active"].set()
        await adapter.stop()


async def test_failed_publication_returns_its_reservation() -> None:
    """A publication that exits through an error frees its slot."""
    gate = asyncio.Event()
    gates = {"active": gate}
    entered: list[str] = []

    async def publish(event) -> None:
        entered.append(event.event_id)
        await gates["active"].wait()
        raise RuntimeError("publisher failure")

    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-error",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await wait_until(lambda: len(entered) == 1)

        gate.set()
        await wait_until(lambda: not adapter._inbound_futures)

        # The failed publication released its slot: the next packet stages.
        # The replacement gate holds it so the staged future stays pending.
        gate_two = asyncio.Event()
        gates["active"] = gate_two
        await asyncio.to_thread(adapter._on_packet, _packet(2))
        assert len(adapter._inbound_futures) == 1
        assert adapter.diagnostics()["producer_overflow"] == 0
        gate_two.set()
    finally:
        gate.set()
        gates["active"].set()
        await adapter.stop()


async def test_stop_preserves_admission_uncertainty_and_restart_gets_full_capacity() -> (
    None
):
    """Running cancellation is ambiguous; a restart stages from full capacity."""
    gate = asyncio.Event()
    gates = {"active": gate}
    entered: list[asyncio.Task] = []
    cancelled = asyncio.Event()

    async def publish(event) -> None:
        task = asyncio.current_task()
        assert task is not None
        entered.append(task)
        try:
            await gates["active"].wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-stop",
            connection_type="fake",
            max_pending_ingress_callbacks=2,
        )
    )
    await _start(adapter, publish, reports)
    try:
        for index in range(3):
            await asyncio.to_thread(adapter._on_packet, _packet(index))
        await wait_until(lambda: len(entered) == 2)

        # Two slots held; the third packet is refused.
        assert len(adapter._inbound_futures) == 2
        assert adapter.diagnostics()["producer_overflow"] == 1
        pre_stop = [r.outcome for r in reports]
        assert pre_stop.count("scheduled") == 2
        assert pre_stop.count("overflow") == 1

        await adapter.stop()
        await asyncio.wait_for(cancelled.wait(), 5)
        await asyncio.gather(*entered, return_exceptions=True)

        # Both publishers started; cancellation cannot establish non-admission.
        cancelled_reports = [
            r for r in reports if r.outcome == "cancelled_before_admission"
        ]
        assert cancelled_reports == []  # Both publishers had already started.
        assert not adapter._inbound_futures
    finally:
        gate.set()

    # Restart: a fresh session gets full capacity — no stale reservations.
    gate_two = asyncio.Event()
    gates["active"] = gate_two
    restart_reports: list[ProducerPressureReport] = []
    await _start(adapter, publish, restart_reports)
    try:
        for index in range(10, 12):
            await asyncio.to_thread(adapter._on_packet, _packet(index))
        await wait_until(lambda: len(adapter._inbound_futures) == 2)

        # Both packets stage: the new session inherited no stale hold.
        assert len(adapter._inbound_futures) == 2
        assert [r.outcome for r in restart_reports] == ["scheduled"] * 2
        assert adapter.diagnostics()["producer_overflow"] == 1

        # The bound still holds in the new session: a third packet refuses.
        await asyncio.to_thread(adapter._on_packet, _packet(12))
        assert [r.outcome for r in restart_reports] == [
            "scheduled",
            "scheduled",
            "overflow",
        ]
        assert adapter.diagnostics()["producer_overflow"] == 2
    finally:
        gate_two.set()
        await adapter.stop()


async def test_reconnect_generation_releases_stale_reservations() -> None:
    """A reconnected session stages from full capacity without stale slots."""
    gate = asyncio.Event()
    seen: list[str] = []

    async def publish(event) -> None:
        seen.append(event.event_id)
        await gate.wait()

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-reconnect",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish, reports)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await wait_until(lambda: len(seen) == 1)
        assert len(adapter._inbound_futures) == 1

        # Simulate the session replacing its client: the connection
        # generation advances exactly as the reconnect path does.
        assert adapter._session is not None
        adapter._session._activate_client(object())  # type: ignore[arg-type]
        generation = adapter._session.connection_generation
        assert generation >= 1

        await asyncio.to_thread(adapter._on_packet, _packet(2))
        # The old publication is still staged, and the new packet still
        # stages: the generation change retired the stale reservation.
        assert len(adapter._inbound_futures) == 2
        assert adapter.diagnostics()["producer_overflow"] == 0
        assert [r.outcome for r in reports] == ["scheduled"] * 2
    finally:
        gate.set()
        await adapter.stop()


async def test_absent_producer_sink_still_bounds_and_counts() -> None:
    """Without the optional sink the adapter degrades gracefully and still bounds."""
    gate = asyncio.Event()
    all_entered = asyncio.Event()
    seen: set[str] = set()
    published: set[str] = set()

    async def publish(event) -> None:
        seen.add(event.event_id)
        if len(seen) == 100:
            all_entered.set()
        await gate.wait()
        published.add(event.event_id)

    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="ingress-no-sink", connection_type="fake")
    )
    await _start(adapter, publish)
    try:

        def callback_burst() -> None:
            for index in range(256):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(callback_burst)
        await asyncio.wait_for(all_entered.wait(), 10)

        assert len(adapter._inbound_futures) == 100
        assert adapter.diagnostics()["producer_overflow"] == 156

        gate.set()
        await asyncio.wait_for(
            asyncio.gather(
                *(asyncio.wrap_future(f) for f in tuple(adapter._inbound_futures))
            ),
            10,
        )
        assert adapter.diagnostics()["inbound_published"] == 100
        assert published == seen
    finally:
        gate.set()
        await adapter.stop()


async def test_ack_and_routing_callbacks_are_never_throttled() -> None:
    """Full staging capacity never delays or destroys ACK/routing callbacks."""
    gate = asyncio.Event()
    seen: list[str] = []

    async def publish(event) -> None:
        seen.append(event.event_id)
        await gate.wait()

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="ingress-ack-boundary",
            connection_type="fake",
            max_pending_ingress_callbacks=1,
        )
    )
    await _start(adapter, publish, reports)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await wait_until(lambda: len(seen) == 1)
        assert len(adapter._inbound_futures) == 1

        # The only slot is held — a relay packet would be refused now.
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
        await asyncio.to_thread(adapter._on_packet, ack_packet)

        # The routing callback still reached send correlation untouched.
        snapshot = adapter.diagnostics()["send_correlation"]
        assert snapshot["unmatched"] == 1
        assert adapter.diagnostics()["producer_overflow"] == 0
        assert len(adapter._inbound_futures) == 1
        assert not [r for r in reports if r.outcome == "overflow"]
    finally:
        gate.set()
        await adapter.stop()


async def test_stop_fences_reader_callbacks_with_pending_publications(
    monkeypatch,
) -> None:
    """Shutdown drains/cancels staged work without scheduling late callbacks."""
    gate = asyncio.Event()
    entered = asyncio.Event()
    exited = asyncio.Event()
    cancelled = asyncio.Event()
    publishers: list[asyncio.Task] = []
    published = []
    reports: list[ProducerPressureReport] = []

    async def publish(event) -> None:
        task = asyncio.current_task()
        assert task is not None
        publishers.append(task)
        entered.set()
        try:
            await gate.wait()
            published.append(event.event_id)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            exited.set()

    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="ingress-shutdown", connection_type="fake")
    )
    await _start(adapter, publish, reports)
    try:
        await asyncio.to_thread(adapter._on_packet, _packet(1))
        await asyncio.wait_for(entered.wait(), 5)
        assert adapter._inbound_futures
        await adapter.stop()
        await asyncio.wait_for(exited.wait(), 5)
        await asyncio.gather(*publishers, return_exceptions=True)
        assert cancelled.is_set()
        assert all(task.cancelled() for task in publishers)
        assert not adapter._inbound_futures
        cancelled_reports = [
            r for r in reports if r.outcome == "cancelled_before_admission"
        ]
        assert (
            cancelled_reports == []
        )  # Running cancellation is not non-admission proof.
        gate.set()
        schedule = Mock(wraps=asyncio.run_coroutine_threadsafe)
        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", schedule)
        await asyncio.to_thread(adapter._on_packet, _packet(2))
        schedule.assert_not_called()
        assert not adapter._inbound_futures
        assert not published
        assert adapter.diagnostics()["inbound_published"] == 0
    finally:
        gate.set()
        for task in publishers:
            task.cancel()
        await asyncio.gather(*publishers, return_exceptions=True)
        await adapter.stop()


def test_stale_generation_does_not_reset_active_staging_capacity():
    """A delayed reader cannot rewind a pool and replenish an exhausted budget."""
    from medre.adapters.meshtastic.adapter import _IngressStagingReservations

    pool = _IngressStagingReservations(1)
    release = pool.acquire(2)
    assert release is not None
    assert pool.acquire(1) is None
    assert pool.acquire(None) is None
    assert pool.acquire(2) is None
    release()
    current = pool.acquire(2)
    assert current is not None
    current()


async def test_missing_report_type_keeps_local_overflow_accounting(monkeypatch):
    """Older cores without the optional report model retain bounded behavior."""
    from medre.adapters.meshtastic import adapter as module

    monkeypatch.setattr(module, "ProducerPressureReport", None)
    reports = []
    adapter = MeshtasticAdapter(MeshtasticConfig(adapter_id="old-core"))
    await _start(adapter, lambda event: None, reports)
    try:
        adapter._record_ingress_overflow()
        assert adapter.diagnostics()["producer_overflow"] == 1
        assert reports == []
    finally:
        await adapter.stop()


async def test_unstarted_future_cancellation_reports_only_known_pre_admission():
    """A queued future with no publication attempt supplies a negative fact."""
    import concurrent.futures
    import threading

    adapter = MeshtasticAdapter(MeshtasticConfig(adapter_id="unstarted"))
    reports = []
    await _start(adapter, lambda event: None, reports)
    try:
        future = concurrent.futures.Future()
        adapter._inbound_futures.add(future)
        adapter._inbound_admission_started[future] = threading.Event()
        future.add_done_callback(
            lambda done: adapter._finish_inbound_future(done, lambda: None)
        )
        await adapter._drain_background_tasks(timeout=0)
        assert future.cancelled()
        assert (
            sum(r.count for r in reports if r.outcome == "cancelled_before_admission")
            == 1
        )
        assert not adapter._inbound_admission_started
    finally:
        await adapter.stop()

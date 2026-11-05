"""Sustained-ingress qualification for bounded staging at and above the cap.

Extends the boundary tests in ``test_meshtastic_ingress_pressure.py`` with
the measured-budget record the provisional staging default was awaiting:
bursts at 1x, 4x, and 16x of the default capacity, a sustained
producer/consumer imbalance cycle, stop/restart under full staging load, and
content-free memory/latency evidence. Everything runs against the real
adapter in ``connection_type="fake"`` from a synthetic SDK reader thread; no
radio, network, or SDK install is required.

The record is loss-honest, not a throughput result: staged work stays
pinned at the configured bound, every above-cap packet is an explicit
counted refusal, and capacity is reused without leaks across bursts and
across sessions.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import sys
import threading
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.contracts.adapter import AdapterContext

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    ProducerPressureReport = None
from medre_meshtastic_tests.helpers.async_utils import wait_until

DEFAULT_CAP = 100
SOURCE_ID = "!qualification-source"
_BURSTS = (100, 400, 1600)
_SUSTAINED_CYCLES = 20
_SUSTAINED_PRODUCE = 120
_SUSTAINED_RELEASE = 16


def _packet(index: int) -> dict:
    """Build distinct relayable frames with no radio device or network."""
    return {
        "fromId": SOURCE_ID,
        "toId": "",
        "channel": 0,
        "id": index + 1,
        "decoded": {"portnum": "text_message", "text": f"qual {index}"},
    }


def _qual_report(name: str, payload: dict) -> None:
    """Optionally export one content-free qualification run record.

    Extends the ``MEDRE_PRESSURE_REPORT_DIR`` pattern of the boundary
    suite: counts, byte peaks, and elapsed durations only, never message
    bodies or sender identity.
    """
    root = os.environ.get("MEDRE_PRESSURE_REPORT_DIR")
    if not root:
        return
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"meshtastic-qual-{name}.json").write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _ru_maxrss_kb() -> int | None:
    """Best-effort process RSS high-water mark, recorded as context only."""
    try:
        import resource
    except ImportError:
        return None
    usage = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    return usage // 1024 if sys.platform == "darwin" else usage


@pytest.mark.parametrize(
    "platform,raw,expected", [("linux", 4096, 4096), ("darwin", 4096, 4)]
)
def test_process_rss_units_are_kilobytes(
    monkeypatch: pytest.MonkeyPatch, platform: str, raw: int, expected: int
) -> None:
    """Export the same RSS unit on Linux and macOS."""
    from types import SimpleNamespace

    fake_resource = SimpleNamespace(
        RUSAGE_SELF=0, getrusage=lambda _kind: SimpleNamespace(ru_maxrss=raw)
    )
    monkeypatch.setitem(sys.modules, "resource", fake_resource)
    monkeypatch.setattr(sys, "platform", platform)
    assert _ru_maxrss_kb() == expected


async def _start(
    adapter: MeshtasticAdapter,
    publish,
    reports: list[ProducerPressureReport] | None = None,
) -> None:
    """Start the adapter in fake mode with an optional producer-pressure sink."""
    ctx = AdapterContext(
        adapter_id=adapter.adapter_id,
        publish_inbound=publish,
        logger=logging.getLogger(f"test.meshtastic.sustained.{adapter.adapter_id}"),
        clock=lambda: datetime.now(UTC),
        shutdown_event=asyncio.Event(),
    )
    if reports is not None:
        ctx.report_producer_pressure = reports.append
    await adapter.start(ctx)


def _report_counts(reports: list[ProducerPressureReport]) -> dict[str, int]:
    """Sum reported counts per outcome for seam reconciliation.

    Outcomes that never occurred are reported as zero so callers can
    reconcile exact totals for bursts with no refusals.
    """
    totals: dict[str, int] = {"scheduled": 0, "overflow": 0}
    for report in reports:
        totals[report.outcome] = totals.get(report.outcome, 0) + report.count
    return totals


def _make_holder_publisher(holder: dict):
    """Publisher that holds each event on its own gate until released.

    The holder dict carries the current cycle's gate, entered set, and
    completion signal so a single closure serves repeated cycles.
    """

    async def publish(event) -> None:
        holder["seen"].add(event.event_id)
        if len(holder["seen"]) == DEFAULT_CAP:
            holder["all_entered"].set()
        await holder["gate"].wait()
        holder["published"].add(event.event_id)

    return publish


def _new_holder() -> dict:
    return {
        "gate": asyncio.Event(),
        "all_entered": asyncio.Event(),
        "seen": set(),
        "published": set(),
    }


@pytest.mark.parametrize("burst", _BURSTS)
async def test_above_cap_bursts_stage_exact_capacity_and_reuse_it(
    burst: int,
) -> None:
    """Bursts at 1x/4x/16x of the cap stage exactly the cap, twice per session.

    Every over-cap packet is an exact, counted refusal that never reaches
    the publisher; the staged 100 deliver in full and the pool refills to
    full capacity for a repeated burst in the same session, so no slot
    leaks across cycles.
    """
    holder = _new_holder()
    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id=f"sustained-burst-{burst}", connection_type="fake")
    )
    await _start(adapter, _make_holder_publisher(holder), reports)
    try:
        for cycle in (1, 2):
            holder["gate"] = asyncio.Event()
            holder["all_entered"] = asyncio.Event()
            holder["seen"] = set()
            holder["published"] = set()
            reports_before = len(reports)
            base = (cycle - 1) * burst

            def burst_callback(base=base, burst=burst) -> None:
                for index in range(burst):
                    adapter._on_packet(_packet(base + index))

            await asyncio.to_thread(burst_callback)
            await asyncio.wait_for(holder["all_entered"].wait(), 10)

            # Exactly the configured capacity is staged; every further
            # packet is refused before scheduling and never published.
            staged_futures = tuple(adapter._inbound_futures)
            assert len(staged_futures) == DEFAULT_CAP
            diagnostics = adapter.diagnostics()
            assert diagnostics["producer_overflow"] == cycle * (burst - 100)
            assert diagnostics["inbound_published"] == (cycle - 1) * DEFAULT_CAP

            cycle_reports = reports[reports_before:]
            totals = _report_counts(cycle_reports)
            assert totals["scheduled"] == DEFAULT_CAP
            assert totals["overflow"] == burst - 100
            assert all(r.count == 1 for r in cycle_reports)
            assert all(
                r.reason == "callback_capacity"
                for r in cycle_reports
                if r.outcome == "overflow"
            )
            assert all(
                r.reason is None for r in cycle_reports if r.outcome == "scheduled"
            )
            assert all(r.adapter_id == adapter.adapter_id for r in cycle_reports)

            holder["gate"].set()
            await asyncio.wait_for(
                asyncio.gather(*(asyncio.wrap_future(f) for f in staged_futures)), 10
            )
            assert await wait_until(lambda: not adapter._inbound_futures)
            assert holder["published"] == holder["seen"]
            assert adapter.diagnostics()["inbound_published"] == (cycle * DEFAULT_CAP)
    finally:
        holder["gate"].set()
        await adapter.stop()


async def test_sustained_imbalance_keeps_exact_invariants_at_every_boundary() -> None:
    """A bounded produce/release cycle keeps the staging contract exact.

    The producer thread keeps submitting above capacity while the
    publisher admits and releases in small batches. Every cycle boundary
    reconciles submitted == staged + refused, staged <= cap, and
    staged == published + in-flight. The cycle is driven by a condition
    variable and completion counters; nothing sleeps on wall-clock
    timings.
    """
    held_order: list[str] = []
    gates: dict[str, asyncio.Event] = {}
    completed = {"count": 0}

    async def publish(event) -> None:
        held_order.append(event.event_id)
        gate = asyncio.Event()
        gates[event.event_id] = gate
        await gate.wait()
        completed["count"] += 1

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="sustained-imbalance", connection_type="fake")
    )
    await _start(adapter, publish, reports)

    produced = {"count": 0}
    target = {"value": 0}
    stop_producer = {"flag": False}
    cond = threading.Condition()

    def producer() -> None:
        index = 0
        while True:
            with cond:
                while target["value"] <= produced["count"]:
                    if stop_producer["flag"]:
                        return
                    cond.wait()
            while produced["count"] < target["value"]:
                adapter._on_packet(_packet(index))
                index += 1
                produced["count"] += 1

    thread = threading.Thread(
        target=producer, name="sustained-ingress-producer", daemon=True
    )
    thread.start()
    try:
        released_total = 0
        for _cycle in range(1, _SUSTAINED_CYCLES + 1):
            with cond:
                target["value"] += _SUSTAINED_PRODUCE
                cond.notify_all()
            assert await wait_until(
                lambda t=target: produced["count"] == t["value"],
                timeout=10,
                interval=0.01,
            )
            produced_cum = produced["count"]
            overflow_cum = adapter.diagnostics()["producer_overflow"]
            staged_cum = sum(1 for r in reports if r.outcome == "scheduled")
            # Every submitted callback is either staged or explicitly
            # refused — no third state, at every cycle boundary.
            assert staged_cum + overflow_cum == produced_cum
            assert len(adapter._inbound_futures) <= DEFAULT_CAP
            # All publishers of this cycle have taken their staging slot.
            assert await wait_until(
                lambda s=staged_cum: len(held_order) == s,
                timeout=10,
                interval=0.01,
            )

            release_to = released_total + _SUSTAINED_RELEASE
            for event_id in held_order[released_total:release_to]:
                gates[event_id].set()
            released_total = release_to
            assert await wait_until(
                lambda r=released_total: completed["count"] == r,
                timeout=10,
                interval=0.01,
            )
            # Retire the done callbacks, then reconcile exactly: staged
            # work splits into published plus still-pending futures.
            assert await wait_until(
                lambda s=staged_cum: adapter.diagnostics()["inbound_published"]
                + len(adapter._inbound_futures)
                == s,
                timeout=10,
                interval=0.01,
            )
            diagnostics = adapter.diagnostics()
            assert diagnostics["inbound_published"] == released_total
            assert len(adapter._inbound_futures) == staged_cum - released_total
            assert diagnostics["producer_overflow"] == overflow_cum
            assert staged_cum + diagnostics["producer_overflow"] == produced_cum

        with cond:
            stop_producer["flag"] = True
            cond.notify_all()
        thread.join(timeout=10)
        assert not thread.is_alive()

        staged_final = sum(1 for r in reports if r.outcome == "scheduled")
        for event_id in held_order[released_total:]:
            gates[event_id].set()
        assert await wait_until(
            lambda total=staged_final: completed["count"] == total,
            timeout=10,
            interval=0.01,
        )
        assert await wait_until(
            lambda: not adapter._inbound_futures, timeout=10, interval=0.01
        )

        produced_cum = produced["count"]
        staged_cum = sum(1 for r in reports if r.outcome == "scheduled")
        totals = _report_counts(reports)
        diagnostics = adapter.diagnostics()
        assert produced_cum == _SUSTAINED_CYCLES * _SUSTAINED_PRODUCE
        assert diagnostics["inbound_published"] == staged_cum == len(held_order)
        assert diagnostics["producer_overflow"] == totals["overflow"]
        assert staged_cum + diagnostics["producer_overflow"] == produced_cum
        assert diagnostics["producer_overflow"] > 0
        _qual_report(
            "sustained",
            {
                "transport": "meshtastic",
                "tier": "synthetic_sdk_callback",
                "record": "sustained_imbalance",
                "cycles": _SUSTAINED_CYCLES,
                "produced_per_cycle": _SUSTAINED_PRODUCE,
                "released_per_cycle": _SUSTAINED_RELEASE,
                "submitted": produced_cum,
                "staged": staged_cum,
                "refused": diagnostics["producer_overflow"],
                "published": diagnostics["inbound_published"],
                "drained": not adapter._inbound_futures,
            },
        )
    finally:
        with cond:
            stop_producer["flag"] = True
            cond.notify_all()
        thread.join(timeout=10)
        for gate in gates.values():
            gate.set()
        await adapter.stop()


async def test_stop_while_saturated_cancels_staged_work_and_restart_is_full() -> None:
    """Stopping at full staging cancels every staged submission; restart is full.

    The pre-stop snapshot of submissions known not to have entered
    publication is exactly the count stop-drain reports (here zero: every
    held publisher had started, and running cancellation is ambiguous).
    The retired pool is left fully released, and the replacement session
    stages an above-cap burst under the identical contract.
    """
    holder = _new_holder()
    publishers: list[asyncio.Task] = []
    cancelled = asyncio.Event()

    async def publish(event) -> None:
        task = asyncio.current_task()
        assert task is not None
        publishers.append(task)
        holder["seen"].add(event.event_id)
        if len(holder["seen"]) == DEFAULT_CAP:
            holder["all_entered"].set()
        try:
            await holder["gate"].wait()
            holder["published"].add(event.event_id)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="sustained-stop", connection_type="fake")
    )
    await _start(adapter, publish, reports)
    pool_before = adapter._ingress_reservations
    generation_before = adapter._session.connection_generation
    try:

        def burst_callback() -> None:
            for index in range(260):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(burst_callback)
        await asyncio.wait_for(holder["all_entered"].wait(), 10)

        assert len(adapter._inbound_futures) == DEFAULT_CAP
        assert adapter.diagnostics()["producer_overflow"] == 160
        pre_stop_tasks = tuple(publishers)
        pre_start_unstarted = sum(
            1
            for future in tuple(adapter._inbound_futures)
            if not adapter._inbound_admission_started[future].is_set()
        )

        await adapter.stop()

        assert not adapter._inbound_futures
        # Let the staged publishers finish propagating their cancellation.
        await asyncio.gather(*pre_stop_tasks, return_exceptions=True)
        await asyncio.wait_for(cancelled.wait(), 5)
        assert all(task.cancelled() for task in pre_stop_tasks)
        cancelled_reports = [
            r for r in reports if r.outcome == "cancelled_before_admission"
        ]
        # Count equals the futures actually cancelled pre-start: all
        # staged publishers had started, so the report is absent and the
        # running cancellations stay ambiguous.
        assert sum(r.count for r in cancelled_reports) == pre_start_unstarted
        assert cancelled_reports == []
    finally:
        holder["gate"].set()

    # Every reservation returned to the retired pool: full capacity is
    # re-acquirable on it after stop-drain.
    released = [pool_before.acquire(generation_before) for _ in range(DEFAULT_CAP)]
    assert all(release is not None for release in released)
    for release in released:
        release()

    # Restart: full capacity immediately, identical above-cap contract.
    holder["gate"] = asyncio.Event()
    holder["all_entered"] = asyncio.Event()
    holder["seen"] = set()
    holder["published"] = set()
    restart_reports: list[ProducerPressureReport] = []
    await _start(adapter, publish, restart_reports)
    try:
        overflow_before = adapter.diagnostics()["producer_overflow"]

        def restart_burst() -> None:
            for index in range(1000, 1160):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(restart_burst)
        await asyncio.wait_for(holder["all_entered"].wait(), 10)

        staged_futures = tuple(adapter._inbound_futures)
        assert len(staged_futures) == DEFAULT_CAP
        diagnostics = adapter.diagnostics()
        assert diagnostics["producer_overflow"] - overflow_before == 60
        totals = _report_counts(restart_reports)
        assert totals["scheduled"] == DEFAULT_CAP
        assert totals["overflow"] == 60
        assert not [
            r for r in restart_reports if r.outcome == "cancelled_before_admission"
        ]

        holder["gate"].set()
        await asyncio.wait_for(
            asyncio.gather(*(asyncio.wrap_future(f) for f in staged_futures)), 10
        )
        assert await wait_until(lambda: not adapter._inbound_futures)
        # Only the staged 100 were published; every refused packet is gone.
        assert holder["published"] == holder["seen"]
        assert len(holder["published"]) == DEFAULT_CAP
        assert adapter.diagnostics()["inbound_published"] == DEFAULT_CAP
    finally:
        holder["gate"].set()
        await adapter.stop()


class _TimedReservations:
    """Proxy timing reader-thread reservation acquires without altering them.

    Delegation is exact: the one-shot releaser from the wrapped pool is
    returned untouched; only the acquire wall time is sampled.
    """

    def __init__(self, inner, samples: list[float]) -> None:
        self._inner = inner
        self._samples = samples

    @property
    def capacity(self) -> int:
        return self._inner.capacity

    def acquire(self, generation: int | None):
        started = time.perf_counter()
        try:
            return self._inner.acquire(generation)
        finally:
            self._samples.append(time.perf_counter() - started)


async def test_stop_drain_coalesces_queued_cancellations_under_load() -> None:
    """Stop-drain reports one coalesced count for queued, unstarted staging.

    The event loop is wedged on a synchronous callback while the reader
    thread saturates staging, so every staged submission is queued but has
    not taken its first step — deterministically unstarted. The stop-drain
    then cancels them all before their first publication attempt and
    reports exactly one coalesced ``cancelled_before_admission`` count
    equal to the unstarted snapshot.
    """
    loop = asyncio.get_running_loop()
    wedge_entered = threading.Event()
    unwedge = threading.Event()
    drain_ran = threading.Event()
    resumed = asyncio.Event()
    observed: dict = {}

    async def publish(_event) -> None:
        raise AssertionError("queued staging must never enter publication")

    reports: list[ProducerPressureReport] = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="sustained-drain", connection_type="fake")
    )
    await _start(adapter, publish, reports)

    def wedge_callback() -> None:
        wedge_entered.set()
        if not unwedge.wait(timeout=30):
            raise AssertionError("reader thread never finished the burst")
        # No background tasks exist here, so the whole drain coroutine
        # runs to completion synchronously: the inbound-futures cancel
        # section and the coalesced report execute before any queued
        # publisher task can take its first step.
        assert not adapter._background_tasks
        drain = adapter._drain_background_tasks(timeout=0)
        try:
            drain.send(None)
        except StopIteration:
            pass
        else:
            drain.close()
            raise AssertionError(
                "stop-drain suspended; expected synchronous completion"
            )
        observed["futures_after_drain"] = len(adapter._inbound_futures)
        drain_ran.set()
        loop.call_soon(resumed.set)

    def burst_callback() -> None:
        if not wedge_entered.wait(timeout=30):
            raise AssertionError("event loop never wedged")
        for index in range(160):
            adapter._on_packet(_packet(index))
        # The loop is wedged: nothing can have consumed a staged future.
        observed["staged_snapshot"] = len(adapter._inbound_futures)
        observed["unstarted_snapshot"] = sum(
            1
            for future in tuple(adapter._inbound_futures)
            if not adapter._inbound_admission_started[future].is_set()
        )
        unwedge.set()
        if not drain_ran.wait(timeout=30):
            raise AssertionError("stop-drain never ran")

    loop.call_soon_threadsafe(wedge_callback)
    thread = threading.Thread(
        target=burst_callback, name="sustained-drain-reader", daemon=True
    )
    thread.start()
    try:
        await asyncio.wait_for(resumed.wait(), 30)
        thread.join(timeout=10)
        assert not thread.is_alive()

        assert observed["staged_snapshot"] == DEFAULT_CAP
        assert observed["unstarted_snapshot"] == DEFAULT_CAP
        assert observed["futures_after_drain"] == 0
        assert not adapter._inbound_futures
        diagnostics = adapter.diagnostics()
        assert diagnostics["producer_overflow"] == 60
        assert diagnostics["inbound_published"] == 0
        totals = _report_counts(reports)
        assert totals["scheduled"] == DEFAULT_CAP
        assert totals["overflow"] == 60
        # Exactly one coalesced report, equal to the futures actually
        # cancelled pre-start.
        cancelled_reports = [
            r for r in reports if r.outcome == "cancelled_before_admission"
        ]
        assert len(cancelled_reports) == 1
        assert cancelled_reports[0].count == observed["unstarted_snapshot"]
        assert cancelled_reports[0].reason is None

        # A full stop afterwards double-counts nothing: the staged set
        # is already empty.
        await adapter.stop()
        assert (
            sum(r.count for r in reports if r.outcome == "cancelled_before_admission")
            == observed["unstarted_snapshot"]
        )
    finally:
        await adapter.stop()


@pytest.mark.parametrize("burst", _BURSTS)
async def test_burst_memory_grows_sublinearly_and_reader_acquire_stays_bounded(
    burst: int,
) -> None:
    """Pending work tracks the cap, not the burst; the reader never parks.

    Peak traced allocation for the 1600-burst must grow sublinearly
    against the 100-burst run in the same process: staged pending work is
    bounded by the cap, so a linear burst would multiply the staged set
    by 16x. Reader-thread ``acquire`` latency is recorded per run and
    asserted only to stay tiny and bounded — this is not a throughput
    SLA; a parking acquire would block until a held publication
    completed, i.e. for the whole saturation phase.
    """
    peaks: dict[int, int] = {}
    max_acquire: dict[int, float] = {}
    samples_seen: dict[int, int] = {}

    for size in (100, burst):
        packets = [_packet(index) for index in range(size)]
        holder = _new_holder()
        publisher = _make_holder_publisher(holder)

        reports: list[ProducerPressureReport] = []
        adapter = MeshtasticAdapter(
            MeshtasticConfig(
                adapter_id=f"sustained-evidence-{size}", connection_type="fake"
            )
        )
        await _start(adapter, publisher, reports)
        samples: list[float] = []
        started_tracing = False
        adapter._ingress_reservations = _TimedReservations(
            adapter._ingress_reservations, samples
        )
        try:
            gc.collect()
            started_tracing = not tracemalloc.is_tracing()
            if started_tracing:
                tracemalloc.start()
            tracemalloc.reset_peak()
            tracing_baseline = tracemalloc.get_traced_memory()[0]

            def burst_callback(frames=packets, target=adapter) -> None:
                for packet in frames:
                    target._on_packet(packet)

            await asyncio.to_thread(burst_callback)
            await asyncio.wait_for(holder["all_entered"].wait(), 10)

            staged = len(adapter._inbound_futures)
            refused = adapter.diagnostics()["producer_overflow"]
            _current, raw_peak = tracemalloc.get_traced_memory()
            peak = max(0, raw_peak - tracing_baseline)
            if started_tracing:
                tracemalloc.stop()
                started_tracing = False
            peaks[size] = peak
            max_acquire[size] = max(samples)
            samples_seen[size] = len(samples)
            assert staged == DEFAULT_CAP
            assert refused == size - 100

            holder["gate"].set()
            await asyncio.wait_for(
                asyncio.gather(
                    *(asyncio.wrap_future(f) for f in tuple(adapter._inbound_futures))
                ),
                10,
            )
            assert await wait_until(lambda a=adapter: not a._inbound_futures)
            drained = adapter.diagnostics()["inbound_published"] == DEFAULT_CAP
            assert drained
            _qual_report(
                f"burst-{size}",
                {
                    "transport": "meshtastic",
                    "tier": "synthetic_sdk_callback",
                    "record": "above_cap_qualification",
                    "burst": size,
                    "staged": staged,
                    "refused": refused,
                    "delivered": adapter.diagnostics()["inbound_published"],
                    "drained": drained,
                    "tracemalloc_peak_bytes": peak,
                    "max_reader_acquire_ms": round(max_acquire[size] * 1000, 3),
                    "ru_maxrss_kb": _ru_maxrss_kb(),
                },
            )
        finally:
            holder["gate"].set()
            if started_tracing:
                tracemalloc.stop()
            await adapter.stop()

    assert samples_seen[burst] == burst
    # Bounded, not a throughput SLA: an uncontended non-blocking take is
    # microseconds; parking on a slot would last the saturation phase.
    assert max_acquire[burst] < 0.1
    if burst == 1600:
        # Sublinear against the 16x linear factor, with generous slack
        # for CI noise in the fixed per-run overhead.
        assert peaks[1600] <= peaks[100] * 8 + 1_000_000

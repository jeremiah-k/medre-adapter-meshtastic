"""Process-lifetime restart-cycle qualification for bounded ingress staging.

Extends the single-session saturation record in
``tests/test_meshtastic_sustained_ingress.py`` with the per-session versus
process-lifetime qualification: many adapter restart cycles in one process.
Every cycle starts a session, drives an above-cap burst with publication
held so the stop retires a fully-stranded staging pool, and stops. The test
pins full per-session capacity, exact per-cycle pressure accounting, exact
post-stop drains (reservations, pending futures, and admission trackers at
zero by count), dead-pool isolation of late one-shot releasers, per-session
diagnostics isolation, and a bounded whole-process retained footprint under
``tracemalloc`` with exact counters carrying the primary evidence.

Synthetic frames run through the real classifier and codec in
``connection_type="fake"`` from a synthetic SDK reader thread; no radio,
network, or SDK install is involved. Determinism: bursts run on a joined
reader thread, completion is observed through events, and nothing sleeps on
wall-clock timings.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import sys
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.contracts.adapter import AdapterContext

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    ProducerPressureReport = None

DEFAULT_CAP = 100
_SOURCE_ID = "!process-lifetime-source"
_RESTART_CYCLES = 25
_WARMUP_CYCLES = 6
_BURST_ROTATION = (
    DEFAULT_CAP * 2,
    DEFAULT_CAP * 4,
    DEFAULT_CAP * 16,
    DEFAULT_CAP + 1,
)
_MEMORY_EXTRA_LIMIT = 256_000
_CYCLE_TIMEOUT = 10.0
_QUAL_LOGGER = "test.meshtastic.process-lifetime"


def _packet(index: int) -> dict:
    """Build distinct relayable frames with no radio device or network."""
    return {
        "fromId": _SOURCE_ID,
        "toId": "",
        "channel": 0,
        "id": index + 1,
        "decoded": {"portnum": "text_message", "text": f"lifetime {index}"},
    }


def _qual_report(name: str, payload: dict) -> None:
    """Optionally export one content-free qualification run record.

    Extends the ``MEDRE_PRESSURE_REPORT_DIR`` pattern of the boundary and
    sustained suites: cycle counts, byte totals, and durations only, never
    message bodies or sender identity.
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


def _report_totals(reports: list[ProducerPressureReport]) -> dict[str, int]:
    """Sum reported counts per outcome for exact per-cycle reconciliation.

    Outcomes that never occurred are reported as zero so callers can
    reconcile exact totals, including the required absence of
    ``cancelled_before_admission`` in cycles where every staged publisher
    had deterministically started.
    """
    totals: dict[str, int] = {
        "scheduled": 0,
        "overflow": 0,
        "cancelled_before_admission": 0,
    }
    for report in reports:
        totals[report.outcome] = totals.get(report.outcome, 0) + report.count
    return totals


def _make_holder() -> dict:
    return {"gate": asyncio.Event(), "all_entered": asyncio.Event(), "seen": set()}


def _make_holder_publisher(holder: dict):
    """Publisher that holds every event on the cycle gate until stop.

    The gate is never set inside a cycle: staged publications stay held, so
    each stop retires a pool whose full capacity is stranded in staged work.
    """

    async def publish(event) -> None:
        holder["seen"].add(event.event_id)
        if len(holder["seen"]) == DEFAULT_CAP:
            holder["all_entered"].set()
        await holder["gate"].wait()

    return publish


def _pool_has_exactly(pool, generation: int | None, expected: int) -> bool:
    """Probe a pool for exactly *expected* available slots, then restore it.

    Acquires up to *expected* slots, requires one further acquire to be
    refused (no capacity beyond the bound), and releases everything back
    through the one-shot releasers. Returns a boolean so the probe can also
    serve as a plain assertion operand.
    """
    taken = []
    for _ in range(expected):
        release = pool.acquire(generation)
        if release is None:
            break
        taken.append(release)
    exact = len(taken) == expected
    if exact:
        extra = pool.acquire(generation)
        if extra is None:
            pass  # refused: capacity ends exactly at the expected bound
        else:
            extra()
            exact = False  # capacity beyond the bound: over-grant
    for release in taken:
        release()
    return exact


async def _start_cycle(
    adapter: MeshtasticAdapter,
    holder: dict,
    reports: list[ProducerPressureReport],
) -> None:
    """Start one restart cycle with a fresh context and pressure sink."""
    ctx = AdapterContext(
        adapter_id=adapter.adapter_id,
        publish_inbound=_make_holder_publisher(holder),
        logger=logging.getLogger(_QUAL_LOGGER),
        clock=lambda: datetime.now(UTC),
        shutdown_event=asyncio.Event(),
    )
    # Qualification log records are orthogonal to the staging contract;
    # disabling propagation keeps retained-tracing measurements scoped to
    # adapter state instead of test-harness log capture.
    ctx.logger.propagate = False
    ctx.report_producer_pressure = reports.append
    await adapter.start(ctx)


async def _run_restart_cycle(
    adapter: MeshtasticAdapter,
    *,
    burst: int,
    packet_base: int,
    cumulative_overflow: int,
    prior_held: list,
) -> tuple[dict, list]:
    """Run one start/burst/stop restart cycle and reconcile it exactly.

    The burst runs on a joined reader thread with publication held, so the
    stop retires a pool stranded at full capacity. Returns the cycle record
    and the one-shot releasers held back from the retired pool; the next
    cycle fires them after its own start to prove late releases land in the
    dead pool only.
    """
    holder = _make_holder()
    reports: list[ProducerPressureReport] = []
    await _start_cycle(adapter, holder, reports)
    pool = adapter._ingress_reservations
    generation = adapter._session.connection_generation

    # The replacement pool is private and full before any traffic, and no
    # staged work from a retired session re-populates the new session.
    assert _pool_has_exactly(pool, generation, DEFAULT_CAP)
    assert not adapter._inbound_futures
    # Retired sessions never bleed into the new session: the cumulative
    # refusal counter is frozen between sessions.
    assert adapter.diagnostics()["producer_overflow"] == cumulative_overflow

    # Prior session's late one-shot releasers fire while the replacement
    # session is live. They release into the retired pool only; the
    # post-fire full-capacity audit on the live pool proves they granted it
    # nothing.
    for release in prior_held:
        release()
    assert _pool_has_exactly(pool, generation, DEFAULT_CAP)

    def burst_callback(base=packet_base, size=burst, target=adapter) -> None:
        for index in range(size):
            target._on_packet(_packet(base + index))

    await asyncio.to_thread(burst_callback)
    await asyncio.wait_for(holder["all_entered"].wait(), _CYCLE_TIMEOUT)

    # Exactly the cap is staged and the live pool is exhausted: a further
    # acquire fails while every staged publisher holds its slot.
    staged = tuple(adapter._inbound_futures)
    assert len(staged) == DEFAULT_CAP
    assert adapter.diagnostics()["inbound_staged_pending"] == DEFAULT_CAP
    assert pool.acquire(generation) is None
    # Every staged publisher has entered publication, so every stop-time
    # cancellation below is the unambiguous running kind.
    unstarted = sum(
        1
        for future in staged
        if not adapter._inbound_admission_started[future].is_set()
    )
    assert unstarted == 0

    totals = _report_totals(reports)
    assert totals["scheduled"] == DEFAULT_CAP
    assert totals["overflow"] == burst - DEFAULT_CAP
    assert totals["cancelled_before_admission"] == 0
    assert all(r.count == 1 for r in reports)
    assert all(
        r.reason == "callback_capacity" for r in reports if r.outcome == "overflow"
    )
    assert all(r.reason is None for r in reports if r.outcome != "overflow")
    assert all(r.adapter_id == adapter.adapter_id for r in reports)

    # Stop with publication held: the full pool is stranded in staged work
    # and must be returned entirely during teardown.
    await adapter.stop()

    await asyncio.gather(
        *(asyncio.wrap_future(future) for future in staged),
        return_exceptions=True,
    )

    # Exact post-stop ledger: nothing from this session survives anywhere.
    assert not adapter._inbound_futures
    assert not adapter._inbound_admission_started
    assert not adapter._background_tasks
    assert adapter.diagnostics()["inbound_staged_pending"] == 0
    assert adapter.diagnostics()["inbound_published"] == 0
    assert adapter.diagnostics()["producer_overflow"] == (
        cumulative_overflow + burst - DEFAULT_CAP
    )
    # No report can arrive after stop: the per-cycle totals stay exact.
    assert _report_totals(reports) == totals

    # The retired pool is behaviorally full again: exactly cap slots were
    # returned by the stop drain's cancellations, none lost, none extra.
    assert _pool_has_exactly(pool, generation, DEFAULT_CAP)
    # A retired pool refuses generation-less acquires outright.
    assert pool.acquire(None) is None

    held = [pool.acquire(generation) for _ in range(DEFAULT_CAP)]
    assert all(release is not None for release in held)

    record = {
        "burst": burst,
        "generation": generation,
        "scheduled": totals["scheduled"],
        "overflow": totals["overflow"],
        "cancelled_before_admission": totals["cancelled_before_admission"],
    }
    return record, held


async def test_restart_cycles_start_full_stage_exact_and_retire_completely() -> None:
    """Twenty-five restart cycles leave no process-lifetime accumulation.

    Each cycle proves full replacement capacity, exact staged/refused
    accounting under a held publication, a complete post-stop drain, and
    dead-pool isolation of late one-shot releasers; the whole run proves a
    bounded retained footprint, with the exact per-cycle counters carrying
    the primary evidence.
    """
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="process-lifetime-qual", connection_type="fake")
    )

    # Whole-process memory guard: ownership rule — tracing is stopped again
    # only if this test started it, and pre-existing tracing is rebased on
    # the post-warmup baseline snapshot instead. Warmup cycles (and their
    # throwaway allocations) run inside the traced window so the baseline
    # carries the settled steady-state rather than a fresh tracer.
    started_tracing = not tracemalloc.is_tracing()
    if started_tracing:
        tracemalloc.start()

    pools: list = []
    bursts: list[int] = []
    held: list = []
    packet_cursor = 0
    cumulative_overflow = 0
    total_frames = 0

    for cycle in range(_WARMUP_CYCLES):
        burst = _BURST_ROTATION[cycle % len(_BURST_ROTATION)]
        record, held = await _run_restart_cycle(
            adapter,
            burst=burst,
            packet_base=packet_cursor,
            cumulative_overflow=cumulative_overflow,
            prior_held=held,
        )
        packet_cursor += burst
        total_frames += burst
        cumulative_overflow += record["overflow"]
        pools.append((adapter._ingress_reservations, record["generation"]))

    total_scheduled = 0
    total_overflow = 0
    total_burst = 0
    try:
        # Warmup traffic records are throwaway; the measured baseline below
        # observes the settled process state, not test bookkeeping.
        for release in held:
            release()
        held = []
        gc.collect()
        baseline = tracemalloc.get_traced_memory()[0]

        for cycle in range(_RESTART_CYCLES):
            burst = _BURST_ROTATION[cycle % len(_BURST_ROTATION)]
            record, held = await _run_restart_cycle(
                adapter,
                burst=burst,
                packet_base=packet_cursor,
                cumulative_overflow=cumulative_overflow,
                prior_held=held,
            )
            packet_cursor += burst
            total_frames += burst
            cumulative_overflow += record["overflow"]
            total_scheduled += record["scheduled"]
            total_overflow += record["overflow"]
            total_burst += record["burst"]
            bursts.append(record["burst"])
            pools.append((adapter._ingress_reservations, record["generation"]))

        # The final cycle's late releasers fire against its retired pool
        # with the adapter stopped; they can reach nothing else.
        for release in held:
            release()
        held = []

        # Exact process-lifetime counters after every restart cycle.
        diagnostics = adapter.diagnostics()
        assert not adapter._inbound_futures
        assert not adapter._inbound_admission_started
        assert not adapter._background_tasks
        assert diagnostics["inbound_staged_pending"] == 0
        assert diagnostics["inbound_published"] == 0
        assert diagnostics["producer_overflow"] == cumulative_overflow
        assert diagnostics["classifier_packets_seen"] == total_frames
        assert total_scheduled == _RESTART_CYCLES * DEFAULT_CAP
        assert total_overflow == total_burst - total_scheduled

        # Every retired pool across the whole run — warmup and measured —
        # still holds exactly its full capacity: no slot was lost from any
        # dead pool and no late release over-granted any other pool.
        for pool, generation in pools:
            assert _pool_has_exactly(pool, generation, DEFAULT_CAP)

        # Ledger-only bookkeeping is dropped before the footprint
        # measurement so the guard observes the process, not the test; the
        # retired pools stay held for the sweep above and are freed last.
        bursts.clear()
        pools.clear()
        gc.collect()
        retained = tracemalloc.get_traced_memory()[0]

        # Dual whole-process guards: retained bytes stay under both a
        # doubling of the post-warmup baseline and the absolute extra
        # budget for the whole restart-cycle run.
        assert baseline > 0
        assert retained < 2 * baseline
        assert retained - baseline < _MEMORY_EXTRA_LIMIT

        _qual_report(
            "process-lifetime",
            {
                "transport": "meshtastic",
                "tier": "synthetic_sdk_callback",
                "record": "restart_cycle_qualification",
                "restart_cycles": _RESTART_CYCLES,
                "warmup_cycles": _WARMUP_CYCLES,
                "submitted": total_burst,
                "staged": total_scheduled,
                "refused": total_overflow,
                "published": 0,
                "drained_per_cycle": True,
                "retained_baseline_bytes": baseline,
                "retained_final_bytes": retained,
                "retained_extra_bytes": retained - baseline,
                "memory_extra_limit_bytes": _MEMORY_EXTRA_LIMIT,
                "ru_maxrss_kb": _ru_maxrss_kb(),
            },
        )
    finally:
        for release in held:
            release()
        if started_tracing:
            tracemalloc.stop()

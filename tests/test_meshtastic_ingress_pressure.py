"""Measure reader-thread callback backlog before changing ingress policy.

This is a deterministic held-publication measurement, NOT an SDK/hardware soak.
No packet is silently dropped during the test. A scheduling limit cannot be
introduced without an explicit, observable overflow/admission policy.
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

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.contracts.adapter import AdapterContext


def _packet(index: int) -> dict:
    """Build distinct relayable frames with no radio device or network."""
    return {
        "fromId": "!pressure-source",
        "toId": "",
        "channel": 0,
        "id": index + 1,
        "decoded": {"portnum": "text_message", "text": f"pressure {index}"},
    }


def _report(burst: int, pending: int, delivered: int, elapsed: float) -> None:
    """Optionally export a content-free measurement for reproducible comparisons."""
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
                "peak_pending_callbacks": pending,
                "delivered": delivered,
                "drained": delivered == burst,
                "elapsed_ms": round(elapsed * 1000, 2),
                "overflow_policy": "not_configured",
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize("burst", [32, 256])
async def test_reader_thread_burst_reveals_pending_future_growth(burst: int) -> None:
    """Hold the sink while SDK-style callbacks run from a different thread."""
    gate = asyncio.Event()
    all_entered = asyncio.Event()
    seen: set[str] = set()
    published: set[str] = set()

    async def publish(event) -> None:
        seen.add(event.event_id)
        if len(seen) == burst:
            all_entered.set()
        await gate.wait()
        published.add(event.event_id)

    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="ingress-pressure", connection_type="fake")
    )
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshtastic.ingress.pressure"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    try:
        start = time.perf_counter()

        def callback_burst() -> None:
            for index in range(burst):
                adapter._on_packet(_packet(index))

        await asyncio.to_thread(callback_burst)
        await asyncio.wait_for(all_entered.wait(), 10)
        pending = len(adapter._inbound_futures)
        assert pending == burst, (
            "the existing SDK-thread scheduler creates one future for every "
            "blocked publish; this measurement should be revised if capped"
        )
        assert adapter.diagnostics()["inbound_published"] == 0
        gate.set()
        await asyncio.wait_for(
            asyncio.gather(
                *(asyncio.wrap_future(f) for f in tuple(adapter._inbound_futures))
            ),
            10,
        )
        assert adapter.diagnostics()["inbound_published"] == burst
        assert len(seen) == burst
        assert published == seen
        assert not adapter._inbound_futures
        _report(burst, pending, len(published), time.perf_counter() - start)
    finally:
        gate.set()
        await adapter.stop()


async def test_stop_fences_reader_callbacks_with_pending_publications(
    monkeypatch,
) -> None:
    """Shutdown drains/cancels measured pending work without scheduling late callbacks."""
    gate = asyncio.Event()
    entered = asyncio.Event()
    exited = asyncio.Event()
    cancelled = asyncio.Event()
    publishers: list[asyncio.Task] = []
    published = []

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
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshtastic.ingress.shutdown"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
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

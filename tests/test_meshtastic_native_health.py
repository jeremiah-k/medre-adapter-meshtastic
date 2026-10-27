"""Connection recovery and queue pressure publish truthful local radio readiness."""

import asyncio
import logging
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.async_utils import wait_until


def context(reports):
    """Use the runtime's public optional sink without native network dependencies."""
    ctx = AdapterContext(
        "mt",
        AsyncMock(),
        logging.getLogger("radio-health"),
        lambda: datetime.now(timezone.utc),
        asyncio.Event(),
    )
    ctx.report_health = reports.append
    return ctx


@pytest.fixture
def radio(monkeypatch):
    """Keep the real session/reconnect owner with an inert owned SDK client."""
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", lambda self: None)
    monkeypatch.setattr(MeshtasticSession, "_unsubscribe_callbacks", lambda self: None)
    monkeypatch.setattr(MeshtasticSession, "_reconnect_delay", lambda self, attempt: 0)

    async def connect(session):
        """Return a connected local interface without opening a transport."""
        connected = threading.Event()
        connected.set()
        return SimpleNamespace(isConnected=connected, close=MagicMock())

    monkeypatch.setattr(MeshtasticSession, "_create_client_offloop", connect)
    return MeshtasticAdapter(
        MeshtasticConfig(
            adapter_id="mt",
            connection_type="tcp",
            host="localhost",
            tcp_liveness_interval_seconds=0,
            queue_max_size=2,
            queue_warning_threshold_pct=40,
            queue_critical_threshold_pct=60,
        )
    )


async def test_native_disconnect_and_recovery_publish_without_probe(radio, monkeypatch):
    """The existing generation-scoped reconnect owner publishes both loss and recovery."""
    reports = []
    await radio.start(context(reports))
    session = radio._session
    entered, release = asyncio.Event(), asyncio.Event()
    original = MeshtasticSession._create_client_offloop

    async def connect(owner):
        """Retain the replacement attempt until the degraded observation is visible."""
        entered.set()
        await release.wait()
        return await original(owner)

    monkeypatch.setattr(MeshtasticSession, "_create_client_offloop", connect)
    old_client = session._client
    try:
        assert reports[-1].health == "healthy"
        await asyncio.to_thread(session._on_connection_lost, old_client)
        assert await wait_until(entered.is_set)
        assert reports[-1].health == "degraded"
        assert radio.diagnostics()["health"] == "degraded"
        release.set()
        assert await wait_until(lambda: reports[-1].health == "healthy")
        count = len(reports)
        session._on_connection_lost(old_client)
        await asyncio.sleep(0)
        assert len(reports) == count
    finally:
        release.set()
        await radio.stop()


async def test_queue_pressure_and_drain_publish_without_probe(radio):
    """Radio connectivity cannot hide a full queue, and draining clears degradation."""
    reports = []
    await radio.start(context(reports))
    radio._drain_task.cancel()
    await asyncio.gather(radio._drain_task, return_exceptions=True)
    radio._drain_task = None
    try:
        await radio._queue.enqueue({"text": "one"}, 0)
        assert reports[-1].health == "healthy"
        await radio._queue.enqueue({"text": "two"}, 0)
        assert reports[-1].health == "degraded"
        await radio._queue.dequeue()
        assert reports[-1].health == "healthy"
        count = len(reports)
        await radio._queue.dequeue()
        assert len(reports) == count
    finally:
        await radio.stop()


async def test_retired_session_cannot_publish_replacement_health(radio):
    """A retained native callback cannot publish another start's readiness."""
    reports = []
    await radio.start(context(reports))
    old = radio._session._health_callback
    await radio.stop()
    await radio.start(context(reports))
    try:
        count = len(reports)
        old()
        assert len(reports) == count
    finally:
        await radio.stop()


async def test_failed_observer_is_retried_on_next_native_event(radio):
    """Probe caches and an unavailable observer cannot suppress later notifications."""
    reports = []
    ctx = context(reports)

    def refused(info):
        """Fail only the optional observer, preserving session ownership."""
        raise RuntimeError("observer unavailable")

    ctx.report_health = refused
    await radio.start(ctx)
    try:
        assert (await radio.health_check()).health == "healthy"
        ctx.report_health = reports.append
        radio._session._notify_health_change()
        assert reports[-1].health == "healthy"
    finally:
        await radio.stop()


async def test_native_observation_does_not_schedule_probe_recovery(radio, monkeypatch):
    """Observations stay diagnostic while an explicit probe retains its backup trigger."""
    reports = []
    await radio.start(context(reports))
    calls = []

    def recovery(session, **kwargs):
        """Observe the existing probe recovery trigger without creating another loop."""
        calls.append(session)

    monkeypatch.setattr(MeshtasticSession, "notify_connection_lost", recovery)
    try:
        radio._session._client.isConnected.clear()
        radio._session._notify_health_change()
        assert reports[-1].health == "degraded"
        assert calls == []
        assert (await radio.health_check()).health == "degraded"
        assert calls == [radio._session]
    finally:
        await radio.stop()

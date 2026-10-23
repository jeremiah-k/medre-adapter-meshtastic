"""Offline radio construction must not freeze other adapter startup or shutdown."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from medre_meshtastic_tests.helpers.async_utils import wait_until

from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig


async def test_initial_radio_constructor_does_not_block_the_runtime_loop(monkeypatch):
    """A slow BLE constructor leaves the event loop free to start other adapters."""
    entered, heartbeat, release = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    observed = []
    client = SimpleNamespace(close=lambda: None, myInfo=SimpleNamespace(my_node_num=1))

    def connect(_):
        """Stand in for a blocking native connect, with an owned cleanup release."""
        entered.set()
        assert release.wait(2), "Test constructor was not released"
        return client

    def watchdog():
        """Observe actual loop progress while connection construction is blocked."""
        try:
            assert entered.wait(2)
            observed.append(heartbeat.wait(1))
        finally:
            release.set()

    monkeypatch.setattr(MeshtasticSession, "_create_client", connect)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", lambda _: None)
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    session = MeshtasticSession(
        MeshtasticConfig(
            "offline", connection_type="ble", ble_address="AA:BB:CC:DD:EE:FF"
        ),
        adapter_id="offline",
        platform="meshtastic",
    )
    monitor = threading.Thread(target=watchdog, daemon=True)
    monitor.start()

    async def progress():
        """Represent another adapter's work scheduled on the same runtime loop."""
        await asyncio.to_thread(entered.wait, 2)
        heartbeat.set()

    worker = asyncio.create_task(progress())
    try:
        await session.start()
        await worker
    finally:
        release.set()
        await session.stop()
        await asyncio.to_thread(monitor.join, 2)
    assert observed == [
        True
    ], "Offline radio construction blocked the runtime event loop"


def client(close=lambda: None):
    """Build a connected native-shaped interface without a real transport."""
    return SimpleNamespace(close=close, myInfo=SimpleNamespace(my_node_num=1))


def offline_session(monkeypatch, connect):
    """Use production session ownership with only the blocking SDK seam replaced."""
    monkeypatch.setattr(MeshtasticSession, "_create_client", connect)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", lambda _: None)
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    return MeshtasticSession(
        MeshtasticConfig(
            "offline",
            connection_type="ble",
            ble_address="AA:BB:CC:DD:EE:FF",
            sync_timeout_ms=50,
            reconnect_backoff_initial_seconds=0.01,
            reconnect_backoff_max_seconds=0.01,
        ),
        adapter_id="offline",
        platform="meshtastic",
    )


async def test_startup_budget_preserves_one_attempt_and_recovers_without_restart(
    monkeypatch,
):
    """A delayed radio becomes healthy by adoption, not a competing constructor."""
    release = threading.Event()
    calls = []
    native = client()

    def connect(_):
        """Remain offline until the test's explicit availability change."""
        calls.append(1)
        assert release.wait(3)
        return native

    session = offline_session(monkeypatch, connect)
    try:
        async with asyncio.timeout(1):
            await session.start()
        assert session.reconnecting and not session.connected
        assert len(calls) == 1
        # Trigger a duplicate connection-lost notification while startup owns
        # the attempt; recovery must retain the same native construction.
        session.notify_connection_lost()
        assert len(calls) == 1
        release.set()
        assert await wait_until(lambda: session.connected, timeout=1)
        assert session.client is native and len(calls) == 1
    finally:
        release.set()
        await session.stop()


async def test_quick_connection_failure_enters_lifetime_retry_and_recovers(monkeypatch):
    """A refused initial connection does not permanently fail the adapter."""
    from medre.adapters.meshtastic.errors import MeshtasticConnectionError

    available = threading.Event()
    calls = []
    native = client()

    def connect(_):
        """Simulate a real constructor refusal until the node comes online."""
        calls.append(1)
        if not available.is_set():
            raise MeshtasticConnectionError("test radio offline")
        return native

    session = offline_session(monkeypatch, connect)
    try:
        await session.start()
        assert session.reconnecting and not session.connected
        available.set()
        assert await wait_until(lambda: session.connected, timeout=1)
        assert session.client is native and len(calls) >= 2
    finally:
        await session.stop()


async def test_stop_during_pending_startup_disposes_the_late_client(monkeypatch):
    """A client returned after shutdown is closed and cannot revive the session."""
    release, closed = threading.Event(), threading.Event()
    native = client(closed.set)

    def connect(_):
        """Return only after session shutdown has completed."""
        assert release.wait(3)
        return native

    session = offline_session(monkeypatch, connect)
    try:
        await session.start()
        async with asyncio.timeout(1):
            await session.stop(timeout=0.2)
        release.set()
        assert await wait_until(closed.is_set, timeout=1)
        assert session.client is None and not session.connected
    finally:
        release.set()
        await session.stop()


async def test_cancelling_initial_start_keeps_late_client_disposal_owned(monkeypatch):
    """Cancellation before the startup deadline cannot leak or adopt an interface."""
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    native = client(closed.set)

    def connect(_):
        """Signal native construction before the async caller is cancelled."""
        entered.set()
        assert release.wait(3)
        return native

    session = offline_session(monkeypatch, connect)
    starting = asyncio.create_task(session.start())
    try:
        assert await wait_until(entered.is_set, timeout=1)
        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting
        release.set()
        assert await wait_until(closed.is_set, timeout=1)
        assert session.client is None and not session.connected
    finally:
        release.set()
        await session.stop()


@pytest.mark.parametrize(
    "value", [True, False, 0, -1, 1.5, float("inf"), float("nan"), "50"]
)
def test_startup_budget_requires_a_positive_integer(value):
    """Invalid deadline configuration fails validation rather than disabling bounds."""
    from medre.config.adapters.meshtastic import MeshtasticConfigError

    with pytest.raises(MeshtasticConfigError, match="sync_timeout_ms"):
        MeshtasticConfig("offline", sync_timeout_ms=value).validate()


async def test_offline_second_radio_does_not_block_other_runtime_adapters(
    tmp_path, monkeypatch
):
    """Production assembly starts Matrix and MeshCore and later heals the radio."""
    from medre.config.loader import parse_runtime_config
    from medre.config.paths import resolve
    from medre.runtime.builder import RuntimeBuilder

    pytest.importorskip("medre.adapters.fakes.matrix")
    pytest.importorskip("medre.adapters.fakes.meshcore")
    release = threading.Event()
    calls = []

    def connect(session):
        """Only the second configured node is offline; every native handle is owned."""
        calls.append(session._adapter_id)
        if session._adapter_id == "FXSF":
            assert release.wait(3)
        return client()

    monkeypatch.setattr(MeshtasticSession, "_create_client", connect)
    monkeypatch.setattr(MeshtasticSession, "_subscribe_callbacks", lambda _: None)
    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    paths = resolve(environ={"MEDRE_HOME": str(tmp_path)})
    config = parse_runtime_config(
        {
            "adapters": {
                "meshtastic": {
                    name: dict(
                        adapter_kind="real",
                        connection_type="ble",
                        ble_address="AA:BB:CC:DD:EE:" + tail,
                        sync_timeout_ms=50,
                    )
                    for name, tail in [("FXLF", "01"), ("FXSF", "02")]
                },
                "matrix": {
                    "matrix": dict(
                        adapter_kind="fake",
                        homeserver="https://example.invalid",
                        user_id="@bot:example.invalid",
                        access_token="test-only",
                        encryption_mode="plaintext",
                    )
                },
                "meshcore": {"meshcore": {"adapter_kind": "fake"}},
            },
            "routes": {
                "second_radio": {
                    "source_adapters": ["matrix"],
                    "dest_adapters": ["FXSF"],
                    "directionality": "bidirectional",
                    "context_map": {"!room:example.invalid": {"dest_context": "0"}},
                }
            },
            "storage": {"backend": "sqlite", "path": str(tmp_path / "events.sqlite")},
        },
        paths,
    )
    app = RuntimeBuilder(config, paths).build()
    try:
        async with asyncio.timeout(2):
            await app.start()
        assert set(app.started_adapter_ids) == {"FXLF", "FXSF", "matrix", "meshcore"}
        assert (await app.adapters["FXSF"].health_check()).health == "degraded"
        assert (await app.adapters["matrix"].health_check()).health == "healthy"
        assert (await app.adapters["meshcore"].health_check()).health == "healthy"
        assert app.startup_readiness is not None
        assert not app.startup_readiness.skipped
        event = app.adapters["matrix"].make_event(
            text="recovery route witness", channel="!room:example.invalid"
        )
        matched = app.router.match(event)
        assert len(matched) == 1
        assert matched[0].targets[0].adapter == "FXSF"
        assert (await app.refresh_live_health()).runtime_health == "degraded"
        release.set()
        assert await wait_until(
            lambda: app.adapters["FXSF"]._session.connected, timeout=1
        )
        assert (await app.adapters["FXSF"].health_check()).health == "healthy"
        assert calls.count("FXSF") == 1
        assert (await app.refresh_live_health()).runtime_health == "healthy"
        assert app.router.match(event) == matched
    finally:
        release.set()
        await app.stop()


async def test_late_constructor_failure_is_retrieved_after_shutdown(monkeypatch):
    """A stopped owner's native failure cannot become an unhandled task error."""
    release = threading.Event()
    errors = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()

    def connect(_):
        """Fail only after shutdown has cancelled the async attempt owner."""
        assert release.wait(3)
        raise RuntimeError("late native construction failure")

    session = offline_session(monkeypatch, connect)
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        await session.start()
        await session.stop()
        release.set()
        # Executor drain establishes native completion, and a loop barrier lets
        # the worker's disposal callback consume the completed failure.
        await loop.shutdown_default_executor()
        barrier = loop.create_future()
        loop.call_soon(barrier.set_result, None)
        await barrier
        assert not errors
    finally:
        release.set()
        loop.set_exception_handler(previous_handler)
        await session.stop()


async def test_cancel_at_completed_startup_closes_unadopted_result(monkeypatch):
    """Cancellation winning against task adoption cannot leak a ready interface."""
    closed = threading.Event()
    native = client(closed.set)
    session = offline_session(monkeypatch, lambda _: native)

    async def complete_and_cancel(_):
        """Cancel the caller in the same loop turn that produces its client."""
        starting.cancel()
        return native

    monkeypatch.setattr(
        MeshtasticSession, "_create_client_offloop", complete_and_cancel
    )
    starting = asyncio.create_task(session.start())
    with pytest.raises(asyncio.CancelledError):
        await starting
    assert closed.is_set()
    assert session.client is None
    await session.stop()


async def test_stop_closes_completed_startup_before_supervisor_adoption(monkeypatch):
    """Shutdown owns a completed result even before recovery first consumes it."""
    release, closed = threading.Event(), threading.Event()
    native = client(closed.set)

    def connect(_):
        """Wait past the startup deadline and then produce an unadopted client."""
        assert release.wait(3)
        return native

    async def delayed_supervisor(self, pending=None):
        """Hold adoption at a scheduler boundary while native construction ends."""
        await asyncio.Event().wait()

    monkeypatch.setattr(MeshtasticSession, "_reconnect_loop", delayed_supervisor)
    session = offline_session(monkeypatch, connect)
    try:
        await session.start()
        pending = session._startup_connect_task
        assert pending is not None
        release.set()
        assert await wait_until(pending.done, timeout=1)
        await session.stop()
        assert closed.is_set()
        assert session.client is None
    finally:
        release.set()
        await session.stop()


async def test_missing_transport_dependency_remains_a_startup_failure(monkeypatch):
    """A broken BLE installation must not enter lifetime connection retries."""
    import sys

    monkeypatch.setattr("medre.adapters.meshtastic.session.HAS_MESHTASTIC", True)
    monkeypatch.setitem(sys.modules, "meshtastic.ble_interface", None)
    session = MeshtasticSession(
        MeshtasticConfig(
            "offline", connection_type="ble", ble_address="AA:BB:CC:DD:EE:FF"
        ),
        adapter_id="offline",
        platform="meshtastic",
    )
    with pytest.raises(ImportError):
        await session.start()
    assert not session.reconnecting
    assert session._startup_connect_task is None
    await session.stop()


async def test_loop_shutdown_cancellation_does_not_orphan_late_native_client(
    monkeypatch,
):
    """Cancelling asyncio's executor wrapper cannot bypass native-thread disposal."""
    release, closed = threading.Event(), threading.Event()
    built_by, closed_by = [], []

    def close():
        closed_by.append(threading.get_ident())
        closed.set()

    native = client(close)
    workers = []
    original = asyncio.to_thread

    async def tracked_thread(function, *args, **kwargs):
        workers.append(asyncio.current_task())
        return await original(function, *args, **kwargs)

    def connect(_):
        built_by.append(threading.get_ident())
        assert release.wait(3)
        return native

    monkeypatch.setattr(asyncio, "to_thread", tracked_thread)
    session = offline_session(monkeypatch, connect)
    try:
        await session.start()
        await session.stop(timeout=0.2)
        assert workers and not workers[0].done()
        # asyncio.run cancels all remaining tasks before draining its executor.
        workers[0].cancel()
        await asyncio.gather(workers[0], return_exceptions=True)
        release.set()
        assert await wait_until(
            closed.is_set, timeout=1
        ), "Native client escaped disposal after executor-wrapper cancellation"
        assert closed_by == built_by
        assert len(closed_by) == 1 and closed_by[0] != threading.get_ident()
        assert not session.connected and session.client is None
    finally:
        release.set()
        await session.stop()

"""Construction cancellation uses the SDK seam without importing a real radio."""

import asyncio
import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest

from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig


@pytest.mark.parametrize("mode", ["tcp", "serial", "ble"])
async def test_cancellation_reaches_the_sdk_constructor(monkeypatch, mode):
    """Cancelling construction wakes the SDK for every supported transport."""
    entered, retired = threading.Event(), threading.Event()
    signals = []

    def factory(*args, cancelEvent=None, **kwargs):
        """Stand in for a cooperative SDK constructor without touching hardware."""
        assert isinstance(cancelEvent, threading.Event)
        signals.append(cancelEvent)
        entered.set()
        try:
            assert cancelEvent.wait(2), "Host never signalled the SDK"
            raise RuntimeError("native construction cancelled")
        finally:
            retired.set()

    module = ModuleType(f"meshtastic.{mode}_interface")
    setattr(
        module,
        {"tcp": "TCPInterface", "serial": "SerialInterface", "ble": "BLEInterface"}[
            mode
        ],
        factory,
    )
    monkeypatch.setitem(sys.modules, module.__name__, module)
    session = MeshtasticSession(
        MeshtasticConfig(
            "radio",
            connection_type=mode,
            host="localhost",
            serial_port="/dev/test",
            ble_address="AA:BB:CC:DD:EE:FF",
        ),
        adapter_id="radio",
        platform="meshtastic",
    )
    task = asyncio.create_task(session._create_client_offloop())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(retired.wait, 2)
        assert signals[0].is_set()
        assert not session.connected
    finally:
        for signal in signals:
            signal.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_legacy_sdk_receives_no_new_keyword(monkeypatch):
    """A released SDK keeps its existing call contract."""
    expected = SimpleNamespace(close=lambda: None)

    def factory(*, hostname, portNumber, timeout, connectTimeout):
        return expected

    module = ModuleType("meshtastic.tcp_interface")
    module.TCPInterface = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    session = MeshtasticSession(
        MeshtasticConfig("radio", connection_type="tcp", host="localhost"),
        adapter_id="radio",
        platform="meshtastic",
    )
    assert session._create_client(cancel_event=threading.Event()) is expected


async def test_legacy_session_override_retains_native_result_ownership(monkeypatch):
    """A private override without the new keyword remains usable by startup."""
    expected = SimpleNamespace(close=lambda: None)
    session = MeshtasticSession(
        MeshtasticConfig("radio", connection_type="tcp", host="localhost"),
        adapter_id="radio",
        platform="meshtastic",
    )
    monkeypatch.setattr(MeshtasticSession, "_create_client", lambda self: expected)
    assert await session._create_client_offloop() is expected


def test_positional_only_sdk_signal_uses_legacy_constructor_contract(monkeypatch):
    """A non-keyword cancellation parameter does not silently broaden the SDK API."""
    signals = []
    expected = SimpleNamespace(close=lambda: None)

    def factory(cancelEvent=None, /, **kwargs):
        signals.append(cancelEvent)
        return expected

    module = ModuleType("meshtastic.tcp_interface")
    module.TCPInterface = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    session = MeshtasticSession(
        MeshtasticConfig("radio", connection_type="tcp", host="localhost"),
        adapter_id="radio",
        platform="meshtastic",
    )
    assert session._create_client(cancel_event=threading.Event()) is expected
    assert signals == [None]

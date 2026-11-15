"""Regression coverage for the standalone provider's native peer owner."""

import asyncio
import json
import sys
import threading

import pytest
from medre_meshtastic_tests.helpers.live_peer_common import (
    PeerProcess,
    async_packets_until,
    async_peer_context,
    poll_packets_until,
    read_jsonl,
)


def _dying_child() -> list[str]:
    """Child that exits non-zero without ever signalling readiness."""
    return [sys.executable, "-c", "import sys; sys.exit(3)"]


async def test_cancelled_peer_start_is_released_before_return() -> None:
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    closed = threading.Event()
    exits = []

    class Listener:
        def __enter__(self):
            loop.call_soon_threadsafe(started.set)
            assert release.wait(2)
            return self

        def __exit__(self, *_):
            exits.append(1)
            closed.set()

    async def run():
        async with async_peer_context(Listener()):
            pytest.fail("cancelled startup entered body")

    task = asyncio.create_task(run())
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    assert exits == [1]


@pytest.mark.parametrize("body_fails", [False, True])
async def test_repeated_cancellation_during_exit_joins_owner(body_fails):
    loop = asyncio.get_running_loop()
    exiting = asyncio.Event()
    release = threading.Event()
    exits = []

    class Listener:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            loop.call_soon_threadsafe(exiting.set)
            assert release.wait(3)
            exits.append(exc[0])

    async def run():
        async with async_peer_context(Listener()):
            if body_fails:
                raise ValueError("body failed")

    task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(exiting.wait(), 2)
        for _ in range(2):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exits == [ValueError if body_fails else None]


def test_jsonl_ignores_only_uncommitted_trailing_record(tmp_path):
    scratch = tmp_path / "peer.jsonl"
    scratch.write_text('{"id": 1}\n{"id":')
    assert read_jsonl(scratch) == [{"id": 1}]
    with scratch.open("a") as file:
        file.write(' 2}\n')
    assert read_jsonl(scratch) == [{"id": 1}, {"id": 2}]
    scratch.write_text('{"id":\n')
    with pytest.raises(json.JSONDecodeError):
        read_jsonl(scratch)


@pytest.mark.parametrize("timeout,interval", [(-1, 0.1), (float("nan"), 0.1), (1, 0)])
async def test_invalid_poll_window_rejected_before_read(timeout, interval):
    def read(*_):
        pytest.fail("invalid window must not start a read")

    with pytest.raises(ValueError):
        await async_packets_until(read, bool, timeout, interval)
    with pytest.raises(ValueError):
        poll_packets_until(read, bool, timeout, interval)


async def test_failed_cancelled_entry_does_not_call_exit():
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()

    class Listener:
        def __enter__(self):
            loop.call_soon_threadsafe(started.set)
            assert release.wait(3)
            raise ValueError("entry failed after its own cleanup")

        def __exit__(self, *_):
            pytest.fail("failed entry must own cleanup")

    async def run():
        async with async_peer_context(Listener()):
            pytest.fail("failed entry yielded")

    task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_failed_observer_cannot_supply_absence_evidence(tmp_path):
    owner = PeerProcess(tmp_path / "peer.ready", 1)
    with pytest.raises(AssertionError, match="died before readiness"):
        owner.start(_dying_child())
    with pytest.raises(AssertionError, match="exited early"):
        owner.raise_if_failed()

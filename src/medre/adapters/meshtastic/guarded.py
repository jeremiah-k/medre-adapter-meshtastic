"""Runtime-only completion ownership for a core-guarded queued radio packet.

The queue keeps its existing pacing owner. Waiting for this result retains core's
in-progress claim; no deferred parent feedback is emitted for individual parts.
Cancellation is visible to both the asyncio owner and the native worker thread.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable

from medre.core.contracts.adapter import AdapterPermanentError, AdapterSendError
from medre.core.contracts.delivery import AdapterHandoffResult


class GuardedQueuedSend:
    """One packet's local waiter and cancellation fence, never wire metadata."""

    def __init__(self, before_native_send: Callable[[], Awaitable[None]]) -> None:
        self.completion: asyncio.Future[AdapterHandoffResult] = (
            asyncio.get_running_loop().create_future()
        )
        self.cancelled = threading.Event()
        self.entered = False
        self._before_native_send = before_native_send

    async def grant(self) -> None:
        """Consume one core grant after pacing, refusing cancelled or duplicate work."""
        if self.cancelled.is_set() or self.entered:
            raise AdapterPermanentError(
                "guarded queue item is cancelled or already submitted"
            )
        self.entered = True
        await self._before_native_send()
        if self.cancelled.is_set():
            raise AdapterPermanentError("guarded queue item cancelled during grant")

    def complete(self, handoff: AdapterHandoffResult) -> None:
        """Resolve only the packet waiter; this is not parent delivery feedback."""
        native_id = handoff.native_message_id
        if (
            native_id is None
            or len(native_id) > 10
            or not native_id.isascii()
            or not native_id.isdecimal()
            or not 0 <= int(native_id) <= 0xFFFFFFFF
        ):
            self.fail("guarded native handoff did not retain a valid packet ID")
            return
        if not self.completion.done():
            self.completion.set_result(handoff)

    def fail(self, message: str, *, transient: bool = False) -> None:
        """Preserve pre-grant retryability while holding every consumed grant."""
        self.cancelled.set()
        if self.completion.done():
            return
        error = (
            AdapterSendError(message, transient=True)
            if transient and not self.entered
            else AdapterPermanentError(message)
        )
        self.completion.set_exception(error)

    def close(self) -> None:
        """Fence late native work when the owner finishes or its await is cancelled."""
        self.cancelled.set()
        if not self.completion.done():
            self.completion.cancel()

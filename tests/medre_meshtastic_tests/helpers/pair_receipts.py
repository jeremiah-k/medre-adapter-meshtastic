"""Wait for the durable handoff required by the pair's RF evidence."""

from __future__ import annotations

import asyncio
import time


async def await_sent_receipt(
    storage,
    event_id: str,
    *,
    target_adapter: str,
    route_id: str,
    timeout: float,
    interval: float = 0.25,
):
    """Refresh queued evidence until the selected destination records handoff."""
    deadline = time.monotonic() + timeout
    while True:
        receipts = await storage.list_receipts_for_event(event_id)
        matching = [
            receipt
            for receipt in receipts
            if receipt.target_adapter == target_adapter and receipt.route_id == route_id
        ]
        latest = max(matching, key=lambda receipt: receipt.sequence, default=None)
        if latest is not None and latest.status == "sent":
            return latest
        remaining = deadline - time.monotonic()
        assert remaining > 0, (
            f"no sent receipt for {target_adapter}/{route_id}; "
            f"latest status={getattr(latest, 'status', None)!r}"
        )
        await asyncio.sleep(min(interval, remaining))

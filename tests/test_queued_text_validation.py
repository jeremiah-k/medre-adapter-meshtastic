"""Corrupted queue records fail permanently before any native radio call."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.queue import QueueTerminalResult
from medre.config.adapters.meshtastic import MeshtasticConfig


@pytest.mark.parametrize("payload", [{"text": 42}, {"text": None}, {}, None, []])
async def test_corrupted_queued_payload_fails_permanently_without_native_send(payload):
    """An invalid retained record must not become a packet or consume retry attempts."""
    adapter = MeshtasticAdapter(
        MeshtasticConfig(adapter_id="radio", connection_type="fake")
    )
    native_send = AsyncMock()
    adapter._session = SimpleNamespace(connected=True, send=native_send)
    await adapter._queue.enqueue({"text": "original"}, 0)
    # Admission already copies/validates mappings. Inject corruption after that
    # boundary so this regression exercises the native drain owner itself.
    adapter._queue._queue[0]["payload"] = payload
    result = await adapter.send_one()
    assert isinstance(result, QueueTerminalResult)
    assert result.outcome == "permanent_failed"
    assert adapter.queue.pending_count == 0
    native_send.assert_not_awaited()

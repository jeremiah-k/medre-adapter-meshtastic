"""Preserve the validated receive generation across SDK callback dispatch."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from medre.core.contracts.adapter import AdapterContext
from medre_meshtastic_tests.helpers.async_utils import wait_until
from medre_meshtastic_tests.helpers.meshtastic import (
    make_meshtastic_config,
    make_meshtastic_text_packet,
)

from medre.adapters.meshtastic.adapter import MeshtasticAdapter


def context() -> AdapterContext:
    return AdapterContext(
        adapter_id="mesh-1",
        publish_inbound=AsyncMock(),
        logger=logging.getLogger("test.mesh-generation"),
        clock=lambda: datetime.now(timezone.utc),
        shutdown_event=asyncio.Event(),
    )


@pytest.mark.parametrize("retire_session", [False, True])
async def test_validated_packet_cannot_cross_replacement(retire_session: bool) -> None:
    """Interleave replacement after validation, before the adapter callback."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    ctx = context()
    old_client = SimpleNamespace(close=MagicMock())
    new_client = SimpleNamespace(close=MagicMock())
    try:
        await adapter.start(ctx)
        session = adapter._session
        assert session is not None
        session._activate_client(old_client)
        callback = session._generation_callback
        assert callback is not None
        packet = make_meshtastic_text_packet(text="retired", packet_id=1)
        if retire_session:
            # Capture the exact callback invocation which the session would
            # dispatch after returning from its locked validation section.
            capture = MagicMock()
            session._generation_callback = capture
            session._on_receive(packet, interface=old_client)
            capture.assert_called_once_with(packet, session.connection_generation)
            await adapter.stop()
            await adapter.start(ctx)
            callback(*capture.call_args.args)
        else:

            def interleave(raw: dict, generation: int) -> None:
                session._activate_client(new_client)
                callback(raw, generation)

            session._generation_callback = interleave
            session._on_receive(packet, interface=old_client)
            session._generation_callback = callback

        assert not adapter._inbound_futures
        ctx.publish_inbound.assert_not_awaited()
        current = adapter._session
        assert current is not None
        current._activate_client(new_client)
        current._on_receive(
            make_meshtastic_text_packet(text="current", packet_id=2),
            interface=new_client,
        )
        assert await wait_until(lambda: ctx.publish_inbound.await_count == 1)
    finally:
        await adapter.stop()

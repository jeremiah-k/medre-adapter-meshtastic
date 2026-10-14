"""Fixtures owned by this adapter test suite."""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

import pytest
from medre.core.events import (
    CanonicalEvent,
)
from medre.core.rendering import RenderingPipeline, TextRenderer
from medre.core.storage.sqlite.storage import SQLiteStorage
from medre_meshtastic_tests.helpers.config_env import isolated_config_env  # noqa: F401


@pytest.fixture
async def temp_storage() -> AsyncGenerator[SQLiteStorage, None]:
    """SQLiteStorage backed by a temporary file, cleaned up after test.

    When ``MEDRE_DOCKER_ARTIFACT_RUN_DIR`` is set, the database file is
    placed under that directory (with a unique suffix) and **not** deleted
    after the test so the artifact collector can retrieve it.

    The storage starts empty — tests that count canonical events see
    exactly the rows they create.  Tests that write outbox /
    native-ref rows directly (bypassing runtime admission) and rely on
    the :func:`medre_meshtastic_tests.helpers.storage_outbox.make_outbox_item` default
    event id should request :func:`outbox_temp_storage` instead, which
    pre-admits the sentinel event required by ``PRAGMA foreign_keys=ON``.
    """
    artifact_dir = os.environ.get("MEDRE_DOCKER_ARTIFACT_RUN_DIR")
    if artifact_dir:
        ad = Path(artifact_dir)
        ad.mkdir(parents=True, exist_ok=True)
        import uuid

        db_path = str(ad / f"storage-{uuid.uuid4().hex[:12]}.db")
    else:
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = f.name
        f.close()

    storage = SQLiteStorage(db_path=db_path)
    try:
        await storage.initialize()
    except BaseException:
        if not artifact_dir:
            with suppress(FileNotFoundError):
                os.unlink(db_path)
        raise
    try:
        yield storage
    finally:
        await storage.close()
        if not artifact_dir:
            with suppress(FileNotFoundError):
                os.unlink(db_path)


class _InboundCollector:
    """Callable that collects published inbound events into a list."""

    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []

    async def __call__(self, event: CanonicalEvent) -> None:
        self.events.append(event)


@pytest.fixture
def inbound_collector() -> _InboundCollector:
    """Callable that records every event passed to publish_inbound."""
    return _InboundCollector()


@pytest.fixture
def make_adapter_context(inbound_collector: _InboundCollector):
    """Factory that creates an AdapterContext wired to the inbound collector."""

    def _make(adapter_id: str = "test_adapter") -> Any:
        from medre.core.contracts.adapter import AdapterContext

        async def _report_delivery_feedback(_feedback: object) -> None:
            return None

        return AdapterContext(
            adapter_id=adapter_id,
            publish_inbound=inbound_collector,
            logger=logging.getLogger(f"test.{adapter_id}"),
            clock=lambda: datetime.now(timezone.utc),
            shutdown_event=asyncio.Event(),
            report_delivery_feedback=_report_delivery_feedback,
        )

    return _make


@pytest.fixture
def rendering_pipeline() -> RenderingPipeline:
    """RenderingPipeline with TextRenderer registered."""
    pipeline = RenderingPipeline()
    pipeline.register(TextRenderer(), priority=100)
    return pipeline

"""Outages back off calmly while native SDK diagnostics remain opt-in."""

import io
import logging
import sys

import pytest

from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.observability.logging import setup_logging


def test_default_outage_backoff_reaches_five_minutes(monkeypatch):
    """Long outages stop reconnecting every thirty seconds without disabling recovery."""
    owner = MeshtasticSession(
        MeshtasticConfig(adapter_id="radio"), "radio", "meshtastic"
    )
    monkeypatch.setattr(
        "medre.adapters.meshtastic.session.random.uniform", lambda _a, _b: 0
    )
    assert owner._reconnect_delay(1) >= 10
    assert owner._reconnect_delay(10_000) == 300


@pytest.mark.parametrize("debug", [False, True])
def test_native_connection_tracebacks_require_explicit_sdk_logging(monkeypatch, debug):
    """Keep the adapter's actionable warning and expose native tracebacks on request."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    loggers = {
        name: logging.getLogger(name)
        for name in ("medre", "meshtastic", "meshtastic.ble")
    }
    levels = {name: logger.level for name, logger in loggers.items()}
    root = logging.getLogger()
    handlers, root_level = list(root.handlers), root.level
    try:
        setup_logging(overrides={"meshtastic.ble": "DEBUG"} if debug else None)
        try:
            raise RuntimeError("No powered Bluetooth adapters found")
        except RuntimeError:
            loggers["meshtastic.ble"].warning("Failed to connect", exc_info=True)
        logging.getLogger("medre.adapters.radio").warning(
            "Bluetooth controller unavailable; retrying"
        )
        output = stream.getvalue()
        assert "Bluetooth controller unavailable; retrying" in output
        assert ("Traceback" in output) is debug
    finally:
        for handler in root.handlers[:]:
            if handler not in handlers:
                root.removeHandler(handler)
        root.setLevel(root_level)
        for name, logger in loggers.items():
            logger.setLevel(levels[name])

"""Startup-info provider: facts the adapter already holds at ready.

Verifies the pure-read ``startup_info()`` provider against the core
startup-info boundary (duck-typed discovery, ``collect_startup_info``,
block rendering, cache round-trip) using fake clients only — no radio,
no SDK, no device I/O.  Facts the adapter does not hold (board and
firmware details, a node address before ``myInfo`` arrives) must be
omitted, never probed.
"""

from __future__ import annotations

import ast
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from medre.core.startup_info import (
    CACHE_FILENAME,
    collect_startup_info,
    read_cache_file,
    render_startup_info_block,
    startup_info_callable,
    write_cache_file,
)
from medre_meshtastic_tests.helpers.meshtastic import make_meshtastic_config

from medre.adapters.fakes.meshtastic import FakeMeshtasticAdapter
from medre.adapters.meshtastic import startup_info as startup_info_module
from medre.adapters.meshtastic.adapter import MeshtasticAdapter
from medre.adapters.meshtastic.session import MeshtasticSession
from medre.config.adapters.meshtastic import MeshtasticConfig

_EMITTED_AT = "2026-11-19T00:00:00+00:00"


def _hold_node_id(adapter: MeshtasticAdapter, node_num: int = 0xA4B3C2D1) -> None:
    """Give the started session a fake client and run the held-state refresh.

    Mirrors the production path without any radio: after client
    activation the session refreshes ``_node_id`` from
    ``interface.myInfo.my_node_num``.  Only in-memory fake state is used.
    """
    session = adapter._session
    assert session is not None
    session._client = SimpleNamespace(myInfo=SimpleNamespace(my_node_num=node_num))
    session._refresh_node_id()


# ---------------------------------------------------------------------------
# Discovery: core duck-types the callable on the adapter object
# ---------------------------------------------------------------------------


def test_real_adapter_exposes_discoverable_startup_info() -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    assert startup_info_callable(adapter) is not None


def test_fake_adapter_exposes_discoverable_startup_info() -> None:
    adapter = FakeMeshtasticAdapter(adapter_id="fake_mesh")
    assert startup_info_callable(adapter) is not None


@pytest.mark.meshtastic_sdk
def test_startup_info_reports_node_id_from_sdk_my_info() -> None:
    mesh_pb2 = import_module("meshtastic.protobuf.mesh_pb2")
    config = make_meshtastic_config()
    adapter = MeshtasticAdapter(config)
    session = MeshtasticSession(config, "mesh-1", "meshtastic")
    session._activate_client(
        SimpleNamespace(myInfo=mesh_pb2.MyNodeInfo(my_node_num=0xA4B3C2D1))
    )
    session._refresh_node_id()
    adapter._session = session

    assert adapter.startup_info()["rows"][0] == [
        {"name": "node_id", "value": "!a4b3c2d1"}
    ]


# ---------------------------------------------------------------------------
# Facts follow held state; nothing is probed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"connection_type": "tcp", "host": "mesh.example.org", "port": 4403},
            [
                ("connection_type", "tcp", "link"),
                ("host", "mesh.example.org", "host"),
                ("port", 4403, "port"),
            ],
            id="tcp",
        ),
        pytest.param(
            {"connection_type": "tcp", "host": "mesh.example.org"},
            [
                ("connection_type", "tcp", "link"),
                ("host", "mesh.example.org", "host"),
            ],
            id="tcp-default-port",
        ),
        pytest.param(
            {"connection_type": "serial", "serial_port": "/dev/ttyACM0"},
            [
                ("connection_type", "serial", "link"),
                ("serial_port", "/dev/ttyACM0", "serial"),
            ],
            id="serial",
        ),
        pytest.param(
            {"connection_type": "ble", "ble_address": "AA:BB:CC:DD:EE:FF"},
            [
                ("connection_type", "ble", "link"),
                ("ble_address", "AA:BB:CC:DD:EE:FF", "ble"),
            ],
            id="ble",
        ),
        pytest.param(
            {},
            [("connection_type", "fake", "link")],
            id="fake",
        ),
    ],
)
def test_endpoint_facts_follow_configured_connection(
    overrides: dict[str, Any], expected: list[tuple[str, Any, str]]
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config(**overrides))
    rows = adapter.startup_info()["rows"]
    assert len(rows) == 1
    assert [(f["name"], f["value"], f["label"]) for f in rows[0]] == expected


def test_node_id_omitted_until_radio_myinfo_is_held() -> None:
    """A fresh adapter has no session, so no node address is offered."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    names = [f["name"] for row in adapter.startup_info()["rows"] for f in row]
    assert "node_id" not in names


async def test_held_node_id_forms_the_first_row(make_adapter_context) -> None:
    """The session's own node address, once held, leads the identity row."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        _hold_node_id(adapter)
        rows = adapter.startup_info()["rows"]
        assert len(rows) == 2
        assert rows[0] == [{"name": "node_id", "value": "!a4b3c2d1"}]
        assert [f["name"] for f in rows[1]] == ["connection_type"]
    finally:
        await adapter.stop()


def test_startup_info_snapshot_is_safe_from_worker_threads() -> None:
    """Core runs synchronous providers off-loop; the read must be stable."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    with ThreadPoolExecutor(max_workers=4) as pool:
        payloads = list(pool.map(lambda _: adapter.startup_info(), range(32)))
    assert payloads[0]["rows"] == [
        [{"name": "connection_type", "value": "fake", "label": "link"}]
    ]
    assert all(payload == payloads[0] for payload in payloads)


def test_provider_module_imports_nothing_from_core() -> None:
    """The provider stays duck-typed: plain data, no medre.core coupling."""
    source = Path(startup_info_module.__file__ or "").read_text(encoding="utf-8")
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            offenders.extend(
                alias.name
                for alias in node.names
                if alias.name == "medre.core" or alias.name.startswith("medre.core.")
            )
        elif isinstance(node, ast.ImportFrom) and not node.level:
            module = node.module or ""
            if module == "medre.core" or module.startswith("medre.core."):
                offenders.append(module)
    assert offenders == []


# ---------------------------------------------------------------------------
# Core boundary: collection, rendering, cache
# ---------------------------------------------------------------------------


async def test_started_adapter_emits_held_facts_through_core(
    make_adapter_context,
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        _hold_node_id(adapter)
        info = await collect_startup_info(
            "mesh-1", "meshtastic", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [(f.name, f.row, f.label) for f in info.fields] == [
            ("node_id", 0, None),
            ("connection_type", 1, "link"),
        ]
        assert info.fields[0].value == "!a4b3c2d1"
        assert info.fields[1].value == "fake"
        assert render_startup_info_block("meshtastic.mesh-1  ready", info) == (
            "meshtastic.mesh-1  ready\n" "  !a4b3c2d1\n" "  link fake"
        )
    finally:
        await adapter.stop()


async def test_fake_connection_never_reports_a_node_id(
    make_adapter_context,
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        info = await collect_startup_info(
            "mesh-1", "meshtastic", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [f.name for f in info.fields] == ["connection_type"]
    finally:
        await adapter.stop()


async def test_tcp_endpoint_renders_with_labels() -> None:
    config = make_meshtastic_config(
        connection_type="tcp", host="mesh.example.org", port=4403
    )
    adapter = MeshtasticAdapter(config)
    info = await collect_startup_info(
        "mesh-1", "meshtastic", adapter, emitted_at=_EMITTED_AT
    )
    assert info is not None
    assert render_startup_info_block("meshtastic.mesh-1  ready", info) == (
        "meshtastic.mesh-1  ready\n" "  link tcp · host mesh.example.org · port 4403"
    )


async def test_fake_adapter_facts_pass_the_core_boundary(tmp_path: Path) -> None:
    adapter = FakeMeshtasticAdapter(adapter_id="fake_mesh")
    info = await collect_startup_info(
        "fake_mesh", "meshtastic", adapter, emitted_at=_EMITTED_AT
    )
    assert info is not None
    assert [(f.name, f.row, f.value) for f in info.fields] == [
        ("connection_type", 0, "fake")
    ]
    path = tmp_path / CACHE_FILENAME
    write_cache_file(path, info)
    cached = read_cache_file(path, adapter_id="fake_mesh", transport="meshtastic")
    assert cached == info.with_cached_flags(True)
    assert read_cache_file(path, adapter_id="fake_mesh", transport="meshcore") is None


def test_config_error_surface_is_not_part_of_startup_info() -> None:
    """Only endpoint facts are offered; validated config stays out."""
    config = MeshtasticConfig(adapter_id="mesh-1", connection_type="fake")
    adapter = FakeMeshtasticAdapter(config)
    payload = adapter.startup_info()
    names = [f["name"] for row in payload["rows"] for f in row]
    assert names == ["connection_type"]


@pytest.mark.parametrize("next_node", [0xDEADBEEF, None])
async def test_startup_info_waits_for_completed_node_refresh(
    make_adapter_context, next_node: int | None
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    session = adapter._session
    assert session is not None
    _hold_node_id(adapter)
    paused = threading.Event()
    resume = threading.Event()
    attempted = threading.Event()
    lock = session._client_state_lock

    class ObservedLock:
        def __enter__(self):
            if paused.is_set():
                attempted.set()
            return lock.__enter__()

        def __exit__(self, *exc):
            return lock.__exit__(*exc)

    class PausedClient:
        @property
        def myInfo(self):
            paused.set()
            assert resume.wait(5), "refresh release not signalled"
            return SimpleNamespace(my_node_num=next_node)

    session._client_state_lock = ObservedLock()
    session._client = PausedClient()
    refresh = asyncio.create_task(asyncio.to_thread(session._refresh_node_id))
    reader = None
    try:
        assert await asyncio.to_thread(paused.wait, 3)
        reader = asyncio.create_task(asyncio.to_thread(adapter.startup_info))
        assert await asyncio.to_thread(
            attempted.wait, 3
        ), "snapshot did not acquire client lock"
        assert not reader.done()
        resume.set()
        await refresh
        rows = (await reader)["rows"]
        assert [
            fact["value"] for row in rows for fact in row if fact["name"] == "node_id"
        ] == ([] if next_node is None else ["!deadbeef"])
    finally:
        resume.set()
        await asyncio.gather(
            refresh, *([reader] if reader else []), return_exceptions=True
        )
        session._client_state_lock = lock
        session._client = None
        await adapter.stop()


async def test_replacement_client_does_not_inherit_previous_node_id(
    make_adapter_context,
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        _hold_node_id(adapter)
        session = adapter._session
        assert session is not None
        session._activate_client(SimpleNamespace(myInfo=None))
        assert session.node_id is None
        assert all(
            fact["name"] != "node_id"
            for row in adapter.startup_info()["rows"]
            for fact in row
        )
    finally:
        await adapter.stop()

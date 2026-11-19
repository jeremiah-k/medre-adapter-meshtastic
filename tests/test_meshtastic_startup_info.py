"""Startup-info provider: facts the adapter already holds at ready.

Verifies the pure-read ``startup_info()`` provider against the core
startup-info boundary (duck-typed discovery, ``collect_startup_info``,
block rendering, cache round-trip) using fake clients only — no radio,
no SDK, no device I/O.  Facts the adapter does not hold (a node address
or the device release facts before the radio's handshake payload
arrives) must be omitted, never probed.
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
    ``interface.myInfo.my_node_num``.  The fake client carries no
    ``metadata`` attribute, matching a radio that has not (yet)
    delivered its ``DeviceMetadata``.  Only in-memory fake state is used.
    """
    session = adapter._session
    assert session is not None
    session._client = SimpleNamespace(myInfo=SimpleNamespace(my_node_num=node_num))
    session._refresh_node_id()


def _hold_device_facts(
    adapter: MeshtasticAdapter,
    *,
    node_num: int = 0xA4B3C2D1,
    firmware_version: str = "2.6.4.abc",
    board_model: str = "RAK4631",
) -> None:
    """Hold the full handshake payload: node address plus release facts.

    Mirrors the production path for a radio that delivered its
    ``DeviceMetadata`` during the initial config exchange: the session
    captures the firmware release and board model atomically with the
    node address under the client lock.  Only in-memory fake state is
    used.
    """
    session = adapter._session
    assert session is not None
    session._client = SimpleNamespace(
        myInfo=SimpleNamespace(my_node_num=node_num),
        metadata=SimpleNamespace(
            firmware_version=firmware_version,
            hw_model=board_model,
        ),
    )
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


@pytest.mark.meshtastic_sdk
def test_startup_info_reports_device_release_from_sdk_metadata() -> None:
    """The SDK ``DeviceMetadata`` payload renders as labeled release facts."""
    mesh_pb2 = import_module("meshtastic.protobuf.mesh_pb2")
    config = make_meshtastic_config()
    adapter = MeshtasticAdapter(config)
    session = MeshtasticSession(config, "mesh-1", "meshtastic")
    session._activate_client(
        SimpleNamespace(
            myInfo=mesh_pb2.MyNodeInfo(my_node_num=0xA4B3C2D1),
            metadata=mesh_pb2.DeviceMetadata(
                firmware_version="2.6.4.abc123",
                hw_model=mesh_pb2.HardwareModel.RAK4631,
            ),
        )
    )
    session._refresh_node_id()
    adapter._session = session

    rows = adapter.startup_info()["rows"]
    assert rows[0] == [{"name": "node_id", "value": "!a4b3c2d1"}]
    assert [(f["name"], f["value"], f["label"]) for f in rows[1]] == [
        ("connection_type", "fake", "link"),
        ("firmware_version", "2.6.4.abc123", "firmware"),
        ("board_model", "RAK4631", "board"),
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


def test_device_release_facts_omitted_before_radio_metadata_is_held() -> None:
    """A fresh adapter has no session, so no release facts are offered."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    names = [f["name"] for row in adapter.startup_info()["rows"] for f in row]
    assert "firmware_version" not in names
    assert "board_model" not in names


def _session_with_payload(
    adapter: MeshtasticAdapter, metadata: SimpleNamespace | None
) -> None:
    """Install a session whose client carries the given held metadata."""
    config = make_meshtastic_config()
    session = MeshtasticSession(config, "mesh-1", "meshtastic")
    client = SimpleNamespace(myInfo=SimpleNamespace(my_node_num=0xA4B3C2D1))
    if metadata is not None:
        client.metadata = metadata
    session._activate_client(client)
    session._refresh_node_id()
    adapter._session = session


@pytest.mark.parametrize(
    ("metadata_kwargs", "expected"),
    [
        pytest.param(
            None,
            [],
            id="metadata-absent",
        ),
        pytest.param(
            {"firmware_version": None, "hw_model": "RAK4631"},
            [("board_model", "RAK4631", "board")],
            id="firmware-null",
        ),
        pytest.param(
            {"firmware_version": "", "hw_model": "RAK4631"},
            [("board_model", "RAK4631", "board")],
            id="firmware-empty",
        ),
        pytest.param(
            {"firmware_version": "2.6.4.abc"},
            [("firmware_version", "2.6.4.abc", "firmware")],
            id="firmware-only",
        ),
        pytest.param(
            {"firmware_version": "2.6.4.abc", "hw_model": 0},
            [("firmware_version", "2.6.4.abc", "firmware")],
            id="board-unset",
        ),
        pytest.param(
            {"firmware_version": "2.6.4.abc", "hw_model": "RAK4631"},
            [
                ("firmware_version", "2.6.4.abc", "firmware"),
                ("board_model", "RAK4631", "board"),
            ],
            id="firmware-and-board",
        ),
    ],
)
def test_device_release_facts_follow_delivered_metadata(
    metadata_kwargs: dict[str, Any] | None, expected: list[tuple[str, Any, str]]
) -> None:
    """Only release facts the delivered metadata payload carries render."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    metadata = (
        SimpleNamespace(**metadata_kwargs) if metadata_kwargs is not None else None
    )
    _session_with_payload(adapter, metadata)
    rows = adapter.startup_info()["rows"]
    assert len(rows) == 2
    assert [(f["name"], f["value"], f["label"]) for f in rows[1]] == [
        ("connection_type", "fake", "link"),
        *expected,
    ]


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
        lines = render_startup_info_block("meshtastic.mesh-1  ready", info).splitlines()
        assert lines[0] == "meshtastic.mesh-1  ready"
        assert lines[1] in {"  !a4b3c2d1", "  node id !a4b3c2d1"}
        assert lines[2:] == ["  link fake"]
    finally:
        await adapter.stop()


async def test_held_device_release_facts_render_through_core(
    make_adapter_context,
) -> None:
    """Release facts trail the endpoint on the second row with explicit labels."""
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        _hold_device_facts(adapter)
        info = await collect_startup_info(
            "mesh-1", "meshtastic", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [(f.name, f.row, f.label) for f in info.fields] == [
            ("node_id", 0, None),
            ("connection_type", 1, "link"),
            ("firmware_version", 1, "firmware"),
            ("board_model", 1, "board"),
        ]
        assert info.fields[1].value == "fake"
        assert info.fields[2].value == "2.6.4.abc"
        assert info.fields[3].value == "RAK4631"
        lines = render_startup_info_block("meshtastic.mesh-1  ready", info).splitlines()
        assert lines[0] == "meshtastic.mesh-1  ready"
        # The provider leaves this label unspecified; both core renderers are supported.
        assert lines[1] in {"  !a4b3c2d1", "  node id !a4b3c2d1"}
        assert lines[2:] == ["  link fake · firmware 2.6.4.abc · board RAK4631"]
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


async def test_fake_adapter_renders_staged_device_facts() -> None:
    """Fake parity: staged release facts render like the real session's."""
    adapter = FakeMeshtasticAdapter(adapter_id="fake_mesh")
    adapter.firmware_version = "2.6.4.abc"
    adapter.board_model = "RAK4631"
    info = await collect_startup_info(
        "fake_mesh", "meshtastic", adapter, emitted_at=_EMITTED_AT
    )
    assert info is not None
    assert [(f.name, f.row, f.value, f.label) for f in info.fields] == [
        ("connection_type", 0, "fake", "link"),
        ("firmware_version", 0, "2.6.4.abc", "firmware"),
        ("board_model", 0, "RAK4631", "board"),
    ]
    assert render_startup_info_block("meshtastic.fake_mesh  ready", info) == (
        "meshtastic.fake_mesh  ready\n"
        "  link fake · firmware 2.6.4.abc · board RAK4631"
    )


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

        metadata = SimpleNamespace(
            firmware_version="2.6.4.abc",
            hw_model="RAK4631",
        )

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
        held = {fact["name"]: fact["value"] for row in rows for fact in row}
        assert held["firmware_version"] == "2.6.4.abc"
        assert held["board_model"] == "RAK4631"
    finally:
        resume.set()
        await asyncio.gather(
            refresh, *([reader] if reader else []), return_exceptions=True
        )
        session._client_state_lock = lock
        session._client = None
        await adapter.stop()


async def test_replacement_client_does_not_inherit_previous_identity_facts(
    make_adapter_context,
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    try:
        _hold_device_facts(adapter)
        session = adapter._session
        assert session is not None
        session._activate_client(SimpleNamespace(myInfo=None))
        assert session.node_id is None
        assert session.firmware_version is None
        assert session.board_model is None
        assert all(
            fact["name"] not in ("node_id", "firmware_version", "board_model")
            for row in adapter.startup_info()["rows"]
            for fact in row
        )
    finally:
        await adapter.stop()


async def test_startup_snapshot_cannot_mix_replacement_generations(
    make_adapter_context,
) -> None:
    adapter = MeshtasticAdapter(make_meshtastic_config())
    await adapter.start(make_adapter_context("mesh-1"))
    session = adapter._session
    assert session is not None
    _hold_device_facts(adapter)
    lock = session._client_state_lock
    replaced = False

    class ReplaceAfterRead:
        def __enter__(self):
            return lock.__enter__()

        def __exit__(self, *exc):
            nonlocal replaced
            result = lock.__exit__(*exc)
            if not replaced:
                replaced = True
                session._activate_client(
                    SimpleNamespace(
                        myInfo=SimpleNamespace(my_node_num=0xDEADBEEF),
                        metadata=SimpleNamespace(
                            firmware_version="3.0.0", hw_model="HELTEC_V3"
                        ),
                    )
                )
                session._refresh_node_id()
            return result

    def facts():
        return {
            fact["name"]: fact["value"]
            for row in adapter.startup_info()["rows"]
            for fact in row
        }

    session._client_state_lock = ReplaceAfterRead()
    try:
        before = facts()
        assert replaced
        assert (
            before["node_id"],
            before["firmware_version"],
            before["board_model"],
        ) == ("!a4b3c2d1", "2.6.4.abc", "RAK4631")
        after = facts()
        assert (after["node_id"], after["firmware_version"], after["board_model"]) == (
            "!deadbeef",
            "3.0.0",
            "HELTEC_V3",
        )
    finally:
        session._client_state_lock = lock
        session._client = None
        await adapter.stop()

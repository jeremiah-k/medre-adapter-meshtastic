"""Meshtastic stopped-state declaration truthfulness.

The radio carries the identity; the declaration must say so with zero files
instead of inventing capturable state. No device or SDK import is involved.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from medre.adapters.meshtastic.plugin import adapter_spec
from medre.adapters.meshtastic.stopped_declaration import declaration
from medre.maintenance.stopped_declare import declaration_from_spec


def test_spec_references_the_declaration_module():
    reference = adapter_spec().stopped_state_declaration
    assert reference is not None
    assert reference.module == "medre.adapters.meshtastic.stopped_declaration"
    assert reference.name == "declaration"
    assert declaration().provider == "meshtastic"


def test_core_resolution_resolves_the_referenced_factory():
    resolved = declaration_from_spec(adapter_spec())
    assert resolved.provider == "meshtastic"
    coverage = resolved.coverage(
        SimpleNamespace(connection_type="tcp"), adapter_id="radio"
    )
    assert coverage.state_class == "device_attached"
    assert coverage.files == ()
    for connection_type in ("tcp", "serial", "ble"):
        exclusion = resolved.local_exclusion(
            SimpleNamespace(connection_type=connection_type), adapter_id="radio"
        )
        assert exclusion.scope == "unsupported"
        assert exclusion.method == "none"


def test_radio_providers_are_device_attached_with_zero_files():
    coverage = declaration().coverage(
        SimpleNamespace(connection_type="tcp"), adapter_id="radio"
    )
    assert coverage.state_class == "device_attached"
    assert coverage.files == ()
    assert any("firmware" in note for note in coverage.notes)
    assert any("snapshot" in note or "capture" in note for note in coverage.notes)
    assert any("power cycle" in note for note in coverage.notes)
    assert any("factory reset" in note for note in coverage.notes)
    assert not any("power-cycling or reflashing" in note for note in coverage.notes)


def test_fake_connectivity_is_stateless():
    coverage = declaration().coverage(
        SimpleNamespace(connection_type="fake"), adapter_id="radio"
    )
    assert coverage.state_class == "stateless_local"
    assert coverage.files == ()


@pytest.mark.parametrize(
    ("connection_type", "expected"),
    [("tcp", "TCP endpoint"), ("serial", "physical radio"), ("ble", "physical radio")],
)
def test_device_ownership_is_reported_unsupported(connection_type, expected):
    exclusion = declaration().local_exclusion(
        SimpleNamespace(connection_type=connection_type), adapter_id="radio"
    )
    assert exclusion.scope == "unsupported"
    assert exclusion.method == "none"
    assert expected in exclusion.note
    assert "exclusive" in exclusion.note
    if connection_type == "tcp":
        assert "physically attached to this host" not in exclusion.note


def test_device_attached_coverage_integrates_without_inventing_host_files(tmp_path):
    """Planning retains radio coverage without claiming its identity was captured."""
    from medre.maintenance.stopped_declare import (
        DeclaredFile,
        declared,
        plan_from_coverages,
    )

    core = declared(
        provider="core",
        adapter_id="core",
        files=(DeclaredFile(tmp_path / "core.sqlite", "core_sqlite", "sqlite"),),
    )
    radio = declaration().coverage(
        SimpleNamespace(connection_type="tcp"), adapter_id="remote-radio"
    )
    plan = plan_from_coverages(tmp_path, [core, radio])
    document = plan.inventory_document()
    assert document["providers"] == ["core"]
    assert [entry["path"] for entry in document["files"]] == ["core.sqlite"]
    coverage = next(
        row for row in plan.summary()["providers"] if row["provider"] == "meshtastic"
    )
    assert coverage["state_class"] == "device_attached"
    assert coverage["required_files"] == 0

"""Candidate SDKs must match their declared immutable source, not just a version."""

import json
from importlib import metadata
from types import SimpleNamespace

import pytest
from medre_meshtastic_tests.helpers import sdk_contract

COMMIT = "487116e023a8b4dcc60881e7fdefce2eab48bddc"
REPOSITORY = "https://github.com/jeremiah-k/mtjk.git"
REQUIREMENT = f"mtjk @ git+{REPOSITORY}@{COMMIT}"
ORIGIN = {"url": REPOSITORY, "vcs_info": {"vcs": "git", "commit_id": COMMIT}}


def install_metadata(monkeypatch, requirement=REQUIREMENT, origin=ORIGIN):
    """Model wheel metadata and installer provenance without importing the SDK."""
    monkeypatch.setattr(sdk_contract, "_distribution", lambda: "candidate-adapter")
    monkeypatch.setattr(
        metadata,
        "distribution",
        lambda name: SimpleNamespace(
            requires=[requirement, "PyPubSub==4.0.7"],
            version="2.7.11.post12" if name == "mtjk" else "4.0.7",
            read_text=lambda filename: (
                origin if isinstance(origin, str) else json.dumps(origin)
            ),
        ),
    )


def test_immutable_sdk_source_and_release_dependency_are_both_verified(monkeypatch):
    """A resolved Git commit and an ordinary version pin can share one provider."""
    install_metadata(monkeypatch)
    sdk_contract.assert_installed_extra_matches_declared_pins(
        "meshtastic", ("mtjk", "PyPubSub")
    )


@pytest.mark.parametrize(
    "origin",
    [
        None,
        [],
        "not-json",
        {"url": REPOSITORY, "dir_info": {"editable": True}},
        {"url": REPOSITORY, "vcs_info": {"vcs": "git", "commit_id": "0" * 40}},
        {"url": REPOSITORY, "vcs_info": {"vcs": "hg", "commit_id": COMMIT}},
        {"url": "https://example.org/other.git", "vcs_info": ORIGIN["vcs_info"]},
        {**ORIGIN, "subdirectory": "other-project"},
        {**ORIGIN, "dir_info": {"editable": True}},
    ],
)
def test_same_sdk_version_cannot_hide_missing_or_different_origin(monkeypatch, origin):
    """A matching package version never substitutes for source provenance."""
    install_metadata(monkeypatch, origin=origin)
    with pytest.raises(AssertionError, match="provenance"):
        sdk_contract.assert_installed_extra_matches_declared_pins(
            "meshtastic", ("mtjk",)
        )


@pytest.mark.parametrize("revision", ["main", "v2.7.11", COMMIT[:12], ""])
def test_mutable_or_abbreviated_candidate_references_are_rejected(
    monkeypatch, revision
):
    """Only an entire Git object ID qualifies as an immutable candidate pin."""
    install_metadata(monkeypatch, requirement=f"mtjk @ git+{REPOSITORY}@{revision}")
    with pytest.raises(AssertionError, match="immutable Git"):
        sdk_contract.assert_installed_extra_matches_declared_pins(
            "meshtastic", ("mtjk",)
        )


def test_release_pin_keeps_version_mismatch_detection(monkeypatch):
    """Candidate support cannot make ordinary version drift pass."""
    install_metadata(monkeypatch, requirement="mtjk==0.0.1")
    with pytest.raises(AssertionError, match="does not match"):
        sdk_contract.assert_installed_extra_matches_declared_pins(
            "meshtastic", ("mtjk",)
        )


def test_undeclared_sdk_cannot_be_qualified(monkeypatch):
    """An installed dependency must also belong to the provider declaration."""
    install_metadata(monkeypatch)
    with pytest.raises(AssertionError, match="not declared"):
        sdk_contract.assert_installed_extra_matches_declared_pins(
            "meshtastic", ("other-sdk",)
        )

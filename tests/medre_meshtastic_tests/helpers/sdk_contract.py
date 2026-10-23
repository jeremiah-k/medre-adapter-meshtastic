"""Candidate-branch SDK policy: exact releases or verified immutable Git origins."""

import json
import re
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from urllib.parse import urlsplit

from medre.testing.sdk_contract import DeclaredSdkPin
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version


@dataclass(frozen=True)
class GitSdkPin:
    """A candidate SDK identified by repository and complete Git object ID."""

    distribution: str
    repository: str
    commit: str


def _distribution() -> str:
    name = tomllib.loads(
        (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(
            encoding="utf-8"
        )
    )["project"]["name"]
    assert isinstance(name, str), "provider distribution name must be a string"
    return name


def _git_pin(requirement: Requirement) -> GitSdkPin:
    """Reject mutable refs, local checkouts and credential-bearing source URLs."""
    url = requirement.url or ""
    repository, _, revision = url.removeprefix("git+").rpartition("@")
    parsed = urlsplit(repository)
    assert (
        url.startswith("git+https://")
        and parsed.netloc
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and re.fullmatch(r"[0-9a-fA-F]{40}", revision)
    ), f"{requirement.name} requires an immutable Git pin"
    return GitSdkPin(requirement.name, repository, revision.lower())


def declared_extra_pins(
    extra: str, *, sdk_extra: str = ""
) -> dict[str, DeclaredSdkPin | GitSdkPin]:
    """Validate every active provider dependency before selecting installed SDKs."""
    pins: dict[str, DeclaredSdkPin | GitSdkPin] = {}
    for raw in metadata.distribution(_distribution()).requires or ():
        requirement = Requirement(raw)
        if requirement.marker is not None and not any(
            requirement.marker.evaluate({"extra": value}) for value in ("", sdk_extra)
        ):
            continue
        name = canonicalize_name(requirement.name)
        if name == "medre":
            continue
        pin: DeclaredSdkPin | GitSdkPin
        if requirement.url:
            pin = _git_pin(requirement)
        else:
            specifiers = list(requirement.specifier)
            assert (
                len(specifiers) == 1
                and specifiers[0].operator == "=="
                and "*" not in specifiers[0].version
            ), f"{requirement.name} dependency is not exact-pinned"
            pin = DeclaredSdkPin(requirement.name, specifiers[0].version)
        previous = pins.get(name)
        if previous is not None:
            same = (
                isinstance(previous, DeclaredSdkPin)
                and isinstance(pin, DeclaredSdkPin)
                and Version(previous.version) == Version(pin.version)
            ) or (
                isinstance(previous, GitSdkPin)
                and isinstance(pin, GitSdkPin)
                and previous.repository == pin.repository
                and previous.commit == pin.commit
            )
            assert same, f"conflicting SDK requirement: {name}"
        pins[name] = pin
    assert pins, f"{_distribution()} declares no active SDK pins"
    return pins


def assert_installed_extra_matches_declared_pins(
    extra: str, distributions: Iterable[str], *, sdk_extra: str = ""
) -> None:
    """Check release versions or installer-recorded Git repository and commit."""
    pins = declared_extra_pins(extra, sdk_extra=sdk_extra)
    requested = {canonicalize_name(name) for name in distributions}
    missing = requested - pins.keys()
    assert not missing, f"SDK distributions not declared: {sorted(missing)}"
    for name in sorted(requested):
        pin = pins[name]
        if isinstance(pin, DeclaredSdkPin):
            installed = metadata.version(pin.distribution)
            assert Version(installed) == Version(
                pin.version
            ), f"installed {pin.distribution} {installed} does not match pin {pin.version}"
            continue
        raw = metadata.distribution(pin.distribution).read_text("direct_url.json")
        try:
            origin = json.loads(raw or "null")
        except (ValueError, TypeError) as exc:
            raise AssertionError(f"invalid {pin.distribution} provenance") from exc
        assert isinstance(origin, dict), f"missing {pin.distribution} provenance"
        vcs = origin.get("vcs_info")
        assert (
            origin.get("url") == pin.repository
            and origin.get("subdirectory", "") == ""
            and "dir_info" not in origin
            and "archive_info" not in origin
            and isinstance(vcs, dict)
            and vcs.get("vcs") == "git"
            and vcs.get("commit_id") == pin.commit
        ), f"installed {pin.distribution} provenance does not match its immutable Git pin"

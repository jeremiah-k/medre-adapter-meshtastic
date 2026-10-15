"""Provider-owned test policy using shared installed-metadata qualification."""

import tomllib
from pathlib import Path
from typing import Iterable

from medre.testing.sdk_contract import assert_installed_sdk_pins, declared_sdk_pins


def _distribution() -> str:
    return tomllib.loads(
        (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(
            encoding="utf-8"
        )
    )["project"]["name"]


def declared_extra_pins(extra: str, *, sdk_extra: str = ""):
    return declared_sdk_pins(_distribution(), extras=(sdk_extra,) if sdk_extra else ())


def assert_installed_extra_matches_declared_pins(
    extra: str, distributions: Iterable[str], *, sdk_extra: str = ""
) -> None:
    assert_installed_sdk_pins(
        _distribution(), distributions, extras=(sdk_extra,) if sdk_extra else ()
    )

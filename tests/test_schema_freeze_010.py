# medre-test-tier: compatibility
"""Keep the initial Meshtastic native metadata contract stable for 0.1.0."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "docs" / "schemas"
FROZEN = json.loads((ROOT / "v0.1.0.freeze.json").read_text())


def test_meshtastic_schema_inventory_is_frozen() -> None:
    """Catch unreviewed new/removed transport-owned schemas."""
    assert {
        p.name
        for p in ROOT.iterdir()
        if p.name.endswith((".schema.json", ".schema.yaml", ".schema.sql"))
    } == set(FROZEN["schema_files_sha256"])


@pytest.mark.parametrize("name,digest", sorted(FROZEN["schema_files_sha256"].items()))
def test_meshtastic_schema_hash_is_frozen(name: str, digest: str) -> None:
    """Require deliberate version review before altering the baseline."""
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest


def test_frozen_meshtastic_schema_is_valid_and_accepts_native_example() -> None:
    """Freeze a valid machine-readable contract, not merely its byte hash."""
    from jsonschema import Draft202012Validator

    schema = json.loads((ROOT / "meshtastic-native-metadata.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["$id"] == (
        "https://raw.githubusercontent.com/jeremiah-k/"
        "medre-adapter-meshtastic/main/docs/schemas/meshtastic-native-metadata.schema.json"
    )
    example = json.loads(
        (ROOT / "examples" / "meshtastic-native-metadata-example.json").read_text()
    )
    Draft202012Validator(schema).validate(example)


def test_meshtastic_native_schema_rejects_untracked_classification_fields() -> None:
    """Do not silently change the 0.1.0 packet-classification vocabulary."""
    from jsonschema import Draft202012Validator

    schema = json.loads((ROOT / "meshtastic-native-metadata.schema.json").read_text())
    example = json.loads(
        (ROOT / "examples" / "meshtastic-native-metadata-example.json").read_text()
    )
    example["meshtastic"]["classification"]["unreviewed_flag"] = True
    assert not Draft202012Validator(schema).is_valid(example)


def test_frozen_schema_bytes_are_checkout_invariant() -> None:
    """Git must not rewrite frozen schema bytes on Windows (autocrlf)."""
    attributes = (Path(__file__).resolve().parents[1] / ".gitattributes").read_text(
        encoding="utf-8"
    )
    assert "docs/schemas/** -text" in attributes.splitlines()

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

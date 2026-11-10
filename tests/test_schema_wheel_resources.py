"""Exercise schema packaging against real setuptools build artifacts."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "docs" / "schemas"
TARGET = "medre/adapters/meshtastic/_schemas/"


def test_build_py_retires_stale_schema_output(tmp_path: Path) -> None:
    build_lib = tmp_path / "lib"
    stale = build_lib / TARGET / "removed.schema.json"
    stale.parent.mkdir(parents=True)
    stale.write_text('{"obsolete":true}', encoding="utf-8")
    subprocess.run(
        [sys.executable, "setup.py", "build_py", "--build-lib", str(build_lib)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert not stale.exists()
    frozen = json.loads((SCHEMAS / "v0.1.0.freeze.json").read_text())
    for name, digest in frozen["schema_files_sha256"].items():
        assert (
            hashlib.sha256((build_lib / TARGET / name).read_bytes()).hexdigest()
            == digest
        )


def test_build_py_rejects_undeclared_nested_schema(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    shutil.copyfile(ROOT / "setup.py", source / "setup.py")
    shutil.copytree(SCHEMAS, source / "docs" / "schemas")
    nested = source / "docs" / "schemas" / "examples" / "nested.schema.json"
    nested.parent.mkdir(exist_ok=True)
    nested.write_text("{}", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "setup.py", "build_py", "--build-lib", str(tmp_path / "lib")],
        cwd=source,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "Frozen schema inventory differs from its manifest" in result.stderr


def test_built_wheel_has_exact_frozen_schema_inventory(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "setup.py",
            "bdist_wheel",
            "--dist-dir",
            str(dist),
            "--bdist-dir",
            str(tmp_path / "wheel-staging"),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    (wheel,) = dist.glob("*.whl")
    frozen = json.loads((SCHEMAS / "v0.1.0.freeze.json").read_text())
    with ZipFile(wheel) as archive:
        expected = set(frozen["schema_files_sha256"])
        actual = {
            name.removeprefix(TARGET)
            for name in archive.namelist()
            if name.startswith(TARGET)
            and name.endswith((".schema.json", ".schema.yaml", ".schema.sql"))
        }
        assert actual == expected
        for name, digest in frozen["schema_files_sha256"].items():
            assert hashlib.sha256(archive.read(TARGET + name)).hexdigest() == digest
        assert TARGET + "v0.1.0.freeze.json" in archive.namelist()


@pytest.mark.parametrize("corrupt", [False, True])
def test_crlf_checkout_packages_only_digest_proven_frozen_bytes(
    tmp_path: Path, corrupt: bool
) -> None:
    """CRLF reconstruction never permits any other change to frozen schemas."""
    import hashlib

    root = Path(__file__).resolve().parents[1]
    source = tmp_path / "source"
    source.mkdir()
    for name in ("setup.py", "pyproject.toml", "MANIFEST.in", "README.md", "LICENSE"):
        shutil.copyfile(root / name, source / name)
    shutil.copytree(
        root / "src", source / "src", ignore=shutil.ignore_patterns("__pycache__")
    )
    schemas = source / "docs" / "schemas"
    shutil.copytree(root / "docs" / "schemas", schemas)
    manifest = json.loads((schemas / "v0.1.0.freeze.json").read_text())
    for filename in manifest["schema_files_sha256"]:
        path = schemas / filename
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        assert bytes([13, 10]) in path.read_bytes()
    if corrupt:
        filename = next(iter(manifest["schema_files_sha256"]))
        path = schemas / filename
        path.write_bytes(path.read_bytes() + b" ")
    artifacts = tmp_path / "artifacts"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(artifacts),
        ],
        cwd=source,
        capture_output=True,
        text=True,
        timeout=40,
    )
    if corrupt:
        assert result.returncode != 0
        assert "Frozen schema digest mismatch" in result.stderr
        return
    assert result.returncode == 0, result.stdout + result.stderr
    (wheel,) = artifacts.glob("*.whl")
    with ZipFile(wheel) as archive:
        for filename, digest in manifest["schema_files_sha256"].items():
            assert bytes((13, 10)) in (schemas / filename).read_bytes()
            assert (
                hashlib.sha256(
                    archive.read("medre/adapters/meshtastic/_schemas/" + filename)
                ).hexdigest()
                == digest
            )
            assert bytes((13, 10)) not in archive.read(
                "medre/adapters/meshtastic/_schemas/" + filename
            )

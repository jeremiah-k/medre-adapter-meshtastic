"""Build the adapter with only the declared, digest-verified frozen schemas."""

import hashlib
import json
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


def frozen_schema_bytes(path: Path, expected: str) -> bytes:
    """Recover frozen LF bytes only when the manifest proves exact identity.

    A Git clone of main followed by checkout of a candidate revision can retain
    unchanged CRLF files even when that revision adds a -text attribute.
    """
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() == expected:
        return data
    # The setuptools PEP 517 runner rewrites escaped CRLF literals in setup.py.
    restored = data.replace(bytes((13, 10)), bytes((10,)))
    if hashlib.sha256(restored).hexdigest() != expected:
        raise ValueError(
            f"Frozen schema digest mismatch: {path.name}; "
            f"expected={expected}; actual={hashlib.sha256(data).hexdigest()}; "
            f"lf={hashlib.sha256(restored).hexdigest()}; "
            f"size={len(data)}; restored_size={len(restored)}; "
            f"crlf_count={data.count(bytes((13, 10)))}; prefix={data[:16].hex()}"
        )
    return restored


class BuildPyWithSchemas(build_py):
    """Replace stale build outputs and verify the public schema freeze."""

    def run(self) -> None:
        super().run()
        source = Path(__file__).parent / "docs" / "schemas"
        freeze_file = source / "v0.1.0.freeze.json"
        frozen = json.loads(freeze_file.read_text(encoding="utf-8"))[
            "schema_files_sha256"
        ]
        if not isinstance(frozen, dict) or not frozen:
            raise ValueError("Frozen schema manifest must list schema digests")
        actual = {
            file.relative_to(source).as_posix()
            for file in source.rglob("*")
            if file.name.endswith((".schema.json", ".schema.yaml", ".schema.sql"))
        }
        if actual != set(frozen):
            raise ValueError("Frozen schema inventory differs from its manifest")
        target = Path(self.build_lib) / "medre" / "adapters" / "meshtastic" / "_schemas"
        shutil.rmtree(target, ignore_errors=True)
        self.mkpath(str(target))
        for name, digest in sorted(frozen.items()):
            if Path(name).name != name or not isinstance(digest, str):
                raise ValueError(f"Invalid or changed frozen schema: {name!r}")
            (target / name).write_bytes(frozen_schema_bytes(source / name, digest))
        self.copy_file(str(freeze_file), str(target / freeze_file.name))
        # Adapter examples and README are nonnormative, but intentionally
        # distributed; keep them while omitting stale previous-build outputs.
        for path in sorted(source.rglob("*")):
            if not path.is_file() or path.parent == source:
                continue
            relative = path.relative_to(source)
            dest = target / relative
            self.mkpath(str(dest.parent))
            self.copy_file(str(path), str(dest))
        readme = source / "README.md"
        if readme.is_file():
            self.copy_file(str(readme), str(target / readme.name))


setup(cmdclass={"build_py": BuildPyWithSchemas})

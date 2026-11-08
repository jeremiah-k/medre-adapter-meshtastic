"""Include the adapter's canonical schema bundle in built distributions."""

from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class BuildPyWithSchemas(build_py):
    """Copy schema resources from their single documented source directory."""

    def run(self) -> None:
        super().run()
        self.copy_tree(
            str(Path(__file__).parent / "docs" / "schemas"),
            str(
                Path(self.build_lib) / "medre" / "adapters" / "meshtastic" / "_schemas"
            ),
        )


setup(cmdclass={"build_py": BuildPyWithSchemas})

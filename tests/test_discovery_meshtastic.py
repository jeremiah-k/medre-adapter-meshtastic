"""Discovery contract for a separately installed MEDRE transport."""

from importlib.metadata import entry_points

from medre.adapter_registry import get_adapter_spec
from medre.adapter_spec import AdapterSpec


def test_adapter_entry_point() -> None:
    matches = [
        ep for ep in entry_points(group="medre.adapters") if ep.name == "meshtastic"
    ]
    assert len(matches) == 1
    spec = matches[0].load()()
    assert isinstance(spec, AdapterSpec)
    assert spec.transport == "meshtastic"
    assert spec.adapter.module == "medre.adapters.meshtastic.adapter"
    assert spec.config.module == "medre.config.adapters.meshtastic"
    assert get_adapter_spec("meshtastic") == spec

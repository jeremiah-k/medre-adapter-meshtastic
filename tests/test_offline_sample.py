"""The adapter-owned sample must exercise two offline instances independently."""

from medre import adapter_registry
from medre.adapter_registry import AdapterTypeRegistry
from medre.adapters.meshtastic.plugin import adapter_spec
from medre.config._yaml import parse_yaml_config
from medre.config.sample import generate_sample_config
from medre.runtime.smoke import run_fake_bridge_smoke


async def test_sample_routes_between_distinct_offline_instances(tmp_path, monkeypatch):
    monkeypatch.setattr(
        adapter_registry, "ADAPTER_REGISTRY", AdapterTypeRegistry((adapter_spec(),))
    )
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path))
    path = tmp_path / "sample.yaml"
    path.write_text(generate_sample_config())
    report = await run_fake_bridge_smoke(
        config_path=str(path), storage_path=str(tmp_path / "sample.sqlite")
    )
    assert report["status"] == "passed", report.get(
        "fail_reason", report.get("fail_reasons")
    )
    # Core owns instance naming. Qualify the provider against the emitted
    # configuration so both current and proposed core revisions can use it.
    document = parse_yaml_config(path.read_text())
    instances = set(document["adapters"][adapter_spec().transport])
    assert len(instances) == 2
    assert report["source_adapter"] in instances
    assert set(report["target_adapters"]) == instances - {report["source_adapter"]}
    assert report["shutdown_status"] == "stopped"

"""The adapter-owned sample must exercise two offline instances independently."""

from medre import adapter_registry
from medre.adapter_registry import AdapterTypeRegistry
from medre.adapters.meshtastic.plugin import adapter_spec
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
    # Core assigns self-describing instance IDs using each provider's transport.
    transport = adapter_spec().transport
    assert report["source_adapter"] == f"{transport}_1"
    assert report["target_adapters"] == [f"{transport}_2"]
    assert report["shutdown_status"] == "stopped"

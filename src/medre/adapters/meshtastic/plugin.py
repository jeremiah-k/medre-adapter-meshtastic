"""Declarative registration for the separately distributed adapter."""

from medre.adapter_registry import AdapterSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    return AdapterSpec(
        sample_config=SymbolRef("medre.adapters.meshtastic.sample", "sample_config"),
        transport="meshtastic",
        config=SymbolRef("medre.config.adapters.meshtastic", "MeshtasticConfig"),
        runtime_config=SymbolRef(
            "medre.config.adapters.meshtastic_runtime", "MeshtasticRuntimeConfig"
        ),
        adapter=SymbolRef("medre.adapters.meshtastic.adapter", "MeshtasticAdapter"),
        fake_adapter=SymbolRef(
            "medre.adapters.fakes.meshtastic", "FakeMeshtasticAdapter"
        ),
        renderer_factory=SymbolRef(
            "medre.adapters.meshtastic.renderer", "build_meshtastic_renderer"
        ),
        dependency_probe=SymbolRef(
            "medre.adapters.meshtastic.compat", "HAS_MESHTASTIC"
        ),
        distribution="mtjk",
        import_names=("mtjk", "meshtastic"),
        sdk_import_roots=("meshtastic", "serial", "serial_asyncio"),
        native_namespace_reader=SymbolRef(
            "medre.adapters.meshtastic.event_shape", "meshtastic_namespace"
        ),
        versioned_namespace_reader=SymbolRef(
            "medre.adapters.meshtastic.event_shape",
            "meshtastic_versioned_namespace",
        ),
        attribution_projector=SymbolRef(
            "medre.adapters.meshtastic.attribution",
            "project_meshtastic_attribution",
        ),
        support_endpoint_fields=(
            "host",
            "port",
            "serial_port",
            "ble_address",
            "channel_mapping",
        ),
        native_detection_priority=30,
    )

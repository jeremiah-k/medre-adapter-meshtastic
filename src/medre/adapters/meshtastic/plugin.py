"""Declarative registration for the separately distributed adapter."""

from medre import adapter_spec as registration
from medre.adapter_spec import AdapterSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    # Prompt metadata is optional; discovery works with released core versions
    # that do not expose the authoring form contract.
    native_setup = {}
    NativeSetupField = getattr(registration, "NativeSetupField", None)
    if NativeSetupField is not None:
        native_setup["native_setup_fields"] = (
            NativeSetupField(
                "connection_type",
                "Radio connection type",
                choices=("tcp", "serial", "ble"),
            ),
            NativeSetupField(
                "host", "TCP hostname", active_when=("connection_type", "tcp")
            ),
            NativeSetupField(
                "port",
                "TCP port (blank uses the protocol default)",
                required=False,
                active_when=("connection_type", "tcp"),
                numeric=True,
            ),
            NativeSetupField(
                "serial_port",
                "Serial device",
                active_when=("connection_type", "serial"),
            ),
            NativeSetupField(
                "ble_address",
                "BLE device address",
                active_when=("connection_type", "ble"),
            ),
        )
    return AdapterSpec(
        **native_setup,
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

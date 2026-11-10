"""Source-native portnum facts retain protocol meaning outside this adapter."""

import pytest
from medre.adapters.meshtastic.codec import MeshtasticCodec
from medre.adapters.meshtastic.packet_classifier import public_portnum
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.rendering.bridge_context import build_bridge_context


@pytest.mark.parametrize(
    "wire_value,expected", [(1, 1), ("TEXT_MESSAGE_APP", 1), ("text_message", 1)]
)
def test_real_codec_exports_protocol_portnum_in_generic_context(wire_value, expected):
    config = MeshtasticConfig(
        adapter_id="radio",
        detection_sensor_relay=True,
        chat_portnums=frozenset({"detection_sensor"}),
    )
    event = MeshtasticCodec("radio", config).decode(
        {
            "id": 0,
            "fromId": "!12345678",
            "to": 4294967295,
            "channel": 0,
            "decoded": {"portnum": wire_value, "text": "Public text"},
        }
    )
    context = build_bridge_context(event)
    assert context is not None
    assert context["source"]["native_facts"] == {
        "namespace": "meshtastic",
        "fields": {"portnum": expected},
    }
    assert context["source"]["native_ref"]["native_message_id"] == "0"


@pytest.mark.parametrize(
    "wire_value,expected",
    [
        ("POSITION_APP", 3),
        ("PRIVATE_APP", 256),
        (10, 10),
        ("DETECTION_SENSOR_APP", 10),
        ("detection_sensor", 10),
        (0, 0),
        (511, 511),
        ("0", 0),
        ("511", 511),
        (True, None),
        (-1, None),
        (512, None),
        ("999", None),
        ("١", None),
        ("-1", None),
        (None, None),
        ("unknown-custom", None),
    ],
)
def test_public_portnum_does_not_guess_unknown_protocol_values(wire_value, expected):
    assert public_portnum(wire_value) == expected

@pytest.mark.parametrize(
    "wire_value", [37, "MESH_BEACON_APP", "mesh_beacon", "mesh_beacon_app"]
)
def test_sdk_declared_portnum_names_keep_the_protocol_number(monkeypatch, wire_value):
    from medre.adapters.meshtastic import packet_classifier
    from medre.adapters.meshtastic import compat

    # SDK table resolution is lazy, and the fallback table is intentionally narrow.
    monkeypatch.setattr(packet_classifier, "_SDK_PORTNUM_FETCHED", False)
    monkeypatch.setattr(packet_classifier, "_SDK_PORTNUM_CACHE", None)
    monkeypatch.setattr(compat, "get_portnum_table", lambda: {37: "mesh_beacon_app"})
    assert public_portnum(wire_value) == 37


@pytest.mark.meshtastic_sdk
def test_all_installed_sdk_enum_names_preserve_public_protocol_numbers():
    from medre.adapters.meshtastic.compat import get_portnum_table

    table = get_portnum_table()
    assert table
    for number, name in table.items():
        if 0 <= number <= 511:
            assert public_portnum(name) == number, name

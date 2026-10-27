"""SDK-free native form metadata remains provider-owned and deterministic."""

import pytest

from medre.adapters.meshtastic.plugin import adapter_spec


def test_native_setup_fields_are_sdk_free_and_named_for_the_config_model():
    spec = adapter_spec()
    fields = spec.native_setup_fields
    names = {field.name for field in fields}
    config_model = spec.config.load()
    from dataclasses import fields as model_fields

    assert names <= {field.name for field in model_fields(config_model)}
    assert len(names) == len(fields)
    transport = next(field for field in fields if field.name == "connection_type")
    assert transport.choices == ("tcp", "serial", "ble")
    for field in fields:
        if field.name != "connection_type":
            assert field.active_when is not None


def test_tcp_port_can_use_the_protocol_default():
    port = next(
        field for field in adapter_spec().native_setup_fields if field.name == "port"
    )
    assert not port.required


def _run_native_form(monkeypatch, tmp_path, answers, secret=""):
    import io
    from argparse import Namespace

    from medre.cli import native_commands
    from medre.config._yaml import parse_yaml_config

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    output = tmp_path / "connection.yaml"
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr("sys.stdin", Terminal())
    replies = iter(answers)
    monkeypatch.setattr(native_commands, "_ask", lambda *args: next(replies))
    monkeypatch.setattr(native_commands.getpass, "getpass", lambda *args: secret)
    monkeypatch.setattr("medre.cli.bridge_commands._confirm_save", lambda *args: True)
    native_commands._config_native(
        Namespace(
            transport="meshtastic", adapter_id="bot", config=None, output=str(output)
        )
    )
    assert not (tmp_path / "state").exists()
    return parse_yaml_config(output.read_text())["adapters"]["meshtastic"]["bot"]


@pytest.mark.parametrize(
    "mode,answers,key,value",
    [
        ("tcp", ["radio.example.org", ""], "host", "radio.example.org"),
        ("serial", ["/dev/ttyUSB0"], "serial_port", "/dev/ttyUSB0"),
        ("ble", ["AA:BB:CC:DD:EE:FF"], "ble_address", "AA:BB:CC:DD:EE:FF"),
    ],
)
def test_native_form_validates_connection_choices(
    monkeypatch, tmp_path, mode, answers, key, value
):
    values = _run_native_form(monkeypatch, tmp_path, [mode, *answers], secret="012345")
    assert values["connection_type"] == mode
    assert values[key] == value
    assert "port" not in values
    assert "host" not in values or mode == "tcp"
    assert "serial_port" not in values or mode == "serial"
    assert "ble_address" not in values or mode == "ble"

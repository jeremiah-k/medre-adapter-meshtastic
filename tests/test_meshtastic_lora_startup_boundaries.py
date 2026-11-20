"""LoRa startup facts: firmware override rules and protobuf presence."""

from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace
from typing import Any

import pytest
from medre_meshtastic_tests.helpers.meshtastic import make_meshtastic_config

from medre.adapters.meshtastic.session import MeshtasticSession, _read_lora_radio_facts
from medre.adapters.meshtastic.startup_info import LoRaRadioFacts, build_startup_info


def _held_facts(**overrides: Any) -> LoRaRadioFacts:
    lora = SimpleNamespace(**overrides)
    facts = _read_lora_radio_facts(
        SimpleNamespace(
            localNode=SimpleNamespace(localConfig=SimpleNamespace(lora=lora))
        )
    )
    assert facts is not None
    return facts


def _fact_value(facts: LoRaRadioFacts, name: str) -> str | None:
    rows = build_startup_info(make_meshtastic_config(), lora_facts=facts)["rows"]
    return next(
        (fact["value"] for row in rows for fact in row if fact["name"] == name), None
    )


@pytest.mark.parametrize(
    ("use_preset", "preset", "value", "expected"),
    [
        (True, "LONG_FAST", 4, None),
        (True, "LONG_FAST", 5, None),
        (True, "LONG_FAST", 6, "4/6"),
        (True, "LONG_FAST", 8, "4/8"),
        (True, "LONG_FAST", 9, None),
        (True, "LONG_MODERATE", 7, None),
        (True, "LONG_MODERATE", 8, None),
        (True, "LONG_TURBO", 8, None),
        (True, "LONG_SLOW", 8, None),
        (True, "NARROW_FAST", 6, None),
        (True, "NARROW_FAST", 7, "4/7"),
        (True, "TINY_SLOW", 6, None),
        (True, "TINY_SLOW", 7, "4/7"),
        (True, "LITE_FAST", 6, "4/6"),
        (True, "MEDIUM_TURBO", 6, "4/6"),
        (True, "UNKNOWN_PRESET", 8, None),
        (True, "VERY_LONG_SLOW", 8, None),
        (False, "LONG_MODERATE", 4, "4/4"),
        (False, "LONG_MODERATE", 5, "4/5"),
        (False, "LONG_MODERATE", 8, "4/8"),
        (False, "LONG_MODERATE", 0, None),
        (False, "LONG_MODERATE", 3, None),
        (False, "LONG_MODERATE", 12, None),
        (False, "LONG_MODERATE", True, None),
        (None, "LONG_FAST", 8, None),
    ],
)
def test_coding_rate_reports_only_meaningful_settings(
    use_preset: bool | None, preset: str, value: int, expected: str | None
) -> None:
    facts = _held_facts(use_preset=use_preset, modem_preset=preset, coding_rate=value)
    assert _fact_value(facts, "coding_rate") == expected


@pytest.mark.parametrize(
    ("frequency", "expected"),
    [
        (915.0, "915 MHz"),
        (867.5, "867.5 MHz"),
        (915.0625, "915.0625 MHz"),
        (915.06255, "915.06255 MHz"),
        (float("inf"), None),
        (float("nan"), None),
        (0, None),
        (-1, None),
    ],
)
def test_frequency_override_preserves_held_precision(
    frequency: float, expected: str | None
) -> None:
    facts = _held_facts(override_frequency=frequency)
    assert _fact_value(facts, "frequency_override") == expected


@pytest.mark.parametrize("present", [False, True])
def test_lora_capture_honors_parent_presence_even_with_default_accessor(
    present: bool,
) -> None:
    config = SimpleNamespace(
        HasField=lambda field: field == "lora" and present,
        lora=SimpleNamespace(region="UNSET"),
    )
    client = SimpleNamespace(localNode=SimpleNamespace(localConfig=config))
    facts = _read_lora_radio_facts(client)
    if present:
        assert facts is not None
        assert facts.region == "UNSET"
    else:
        assert facts is None


@pytest.mark.parametrize(
    ("channel_num", "explicit", "derived"),
    [
        pytest.param(None, None, False, id="field-absent-no-slot-fact"),
        pytest.param(0, None, True, id="zero-default-is-derived"),
        pytest.param(14, 14, False, id="explicit-slot"),
    ],
)
def test_frequency_slot_capture_distinguishes_three_states(
    channel_num: int | None, explicit: int | None, derived: bool
) -> None:
    """The capture keys the derived marker on zero-with-config-delivered.

    ``channel_num`` has implicit protobuf presence, so an explicitly-set
    zero and an unset field collapse on the wire; with the parent
    ``HasField("lora")`` gate already satisfied, that collapse is exactly
    the derived case.  An explicit positive slot stays numeric.
    """
    overrides: dict[str, Any] = (
        {} if channel_num is None else {"channel_num": channel_num}
    )
    facts = _held_facts(**overrides)
    assert facts.frequency_slot == explicit
    assert facts.frequency_slot_derived is derived
    rendered = _fact_value(facts, "frequency_slot")
    if explicit is not None:
        assert rendered == explicit
    elif derived:
        assert rendered == "derived (name hash)"
    else:
        assert rendered is None


@pytest.mark.meshtastic_sdk
def test_sdk_local_config_presence_distinguishes_absent_and_unset_lora() -> None:
    localonly_pb2 = import_module("meshtastic.protobuf.localonly_pb2")
    config_pb2 = import_module("meshtastic.protobuf.config_pb2")
    config = localonly_pb2.LocalConfig()
    session = MeshtasticSession(make_meshtastic_config(), "mesh-1", "meshtastic")
    session._activate_client(
        SimpleNamespace(localNode=SimpleNamespace(localConfig=config))
    )
    assert not config.HasField("lora")
    session._refresh_node_id()
    assert session.startup_identity_snapshot()[4] is None
    config.lora.CopyFrom(config_pb2.Config.LoRaConfig())
    assert config.HasField("lora")
    session._refresh_node_id()
    facts = session.startup_identity_snapshot()[4]
    assert facts is not None
    assert facts.region == "UNSET"
    assert facts.coding_rate is None
    # A delivered default LoRaConfig carries channel_num zero: with the
    # parent presence gate satisfied, the wire collapse of "explicit
    # zero" and "not set" is exactly the derived case — the marker
    # states the derivation rule without computing the hash.
    assert facts.frequency_slot is None
    assert facts.frequency_slot_derived is True
    rows = build_startup_info(make_meshtastic_config(), lora_facts=facts)["rows"]
    assert ("frequency_slot", "derived (name hash)", "frequency slot") in [
        (fact["name"], fact["value"], fact["label"]) for row in rows for fact in row
    ]


def test_failed_optional_lora_read_keeps_other_identity_facts() -> None:
    class BrokenConfig:
        def HasField(self, field: str) -> bool:
            raise ValueError("unavailable config")

    session = MeshtasticSession(make_meshtastic_config(), "mesh-1", "meshtastic")
    session._activate_client(
        SimpleNamespace(
            myInfo=SimpleNamespace(my_node_num=0xA4B3C2D1),
            localNode=SimpleNamespace(localConfig=BrokenConfig()),
        )
    )
    session._refresh_node_id()
    assert session.startup_identity_snapshot() == ("!a4b3c2d1", None, None, None, None)


@pytest.mark.meshtastic_sdk
@pytest.mark.parametrize(("rate", "expected"), [(5, None), (6, "4/6"), (12, None)])
def test_sdk_preset_override_uses_wire_enum(rate: int, expected: str | None) -> None:
    config_pb2 = import_module("meshtastic.protobuf.config_pb2")
    lora = config_pb2.Config.LoRaConfig(
        use_preset=True,
        modem_preset=config_pb2.Config.LoRaConfig.LONG_FAST,
        coding_rate=rate,
    )
    config = config_pb2.Config(lora=lora)
    facts = _read_lora_radio_facts(
        SimpleNamespace(localNode=SimpleNamespace(localConfig=config))
    )
    assert facts is not None
    assert facts.preset == "LONG_FAST"
    assert _fact_value(facts, "coding_rate") == expected


@pytest.mark.meshtastic_sdk
def test_sdk_explicit_channel_num_stays_numeric_next_to_derived() -> None:
    """Real protobuf: an explicit slot renders as a number, zero as the marker."""
    config_pb2 = import_module("meshtastic.protobuf.config_pb2")

    def facts_with(channel_num: int) -> Any:
        config = config_pb2.Config(
            lora=config_pb2.Config.LoRaConfig(
                use_preset=True,
                modem_preset=config_pb2.Config.LoRaConfig.LONG_FAST,
                region=config_pb2.Config.LoRaConfig.US,
                hop_limit=3,
                channel_num=channel_num,
            )
        )
        facts = _read_lora_radio_facts(
            SimpleNamespace(localNode=SimpleNamespace(localConfig=config))
        )
        assert facts is not None
        return facts

    explicit = facts_with(27)
    assert explicit.frequency_slot == 27
    assert explicit.frequency_slot_derived is False
    assert _fact_value(explicit, "frequency_slot") == 27

    derived = facts_with(0)
    assert derived.frequency_slot is None
    assert derived.frequency_slot_derived is True
    assert _fact_value(derived, "frequency_slot") == "derived (name hash)"

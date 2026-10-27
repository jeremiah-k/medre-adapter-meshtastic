"""Strict normalization of native Meshtastic routing callbacks.

These tests pin the shape-level contract for ROUTING_APP callback
packets: what normalizes, what is refused, and that no source
classification can ever qualify as positive delivery evidence.
"""

from __future__ import annotations

from typing import Any

import pytest

from medre.adapters.meshtastic.routing_observations import (
    REJECT_MALFORMED,
    REJECT_NOT_ROUTING,
    REJECT_NO_ERROR_REASON,
    REJECT_REQUEST_ID_INVALID,
    REJECT_REQUEST_ID_ZERO,
    NativeRoutingResult,
    RejectedNativeRouting,
    classify_native_ack_source,
    normalize_native_routing_result,
    positive_evidence_rejection,
)


def _routing_packet(**overrides: Any) -> dict[str, Any]:
    """A source-shaped ROUTING_APP ACK as the mtjk SDK delivers it."""
    packet: dict[str, Any] = {
        "id": 987654321,
        "fromId": "!a1b2c3d4",
        "toId": "!11112222",
        "channel": 1,
        "hopStart": 3,
        "hopLimit": 3,
        "rxTime": 1760000000,
        "decoded": {
            "portnum": "ROUTING_APP",
            "requestId": 42,
            "routing": {"errorReason": "NONE"},
        },
    }
    packet.update(overrides)
    return packet


class TestAckNormalization:
    """Well-formed routing results normalize with typed fields."""

    def test_ack_normalizes_to_typed_result(self) -> None:
        result = normalize_native_routing_result(_routing_packet())
        assert isinstance(result, NativeRoutingResult)
        assert result.outcome == "ack"
        assert result.request_id == 42
        assert result.error_reason == "NONE"
        assert result.from_id == "!a1b2c3d4"
        assert result.to_id == "!11112222"
        assert result.channel_index == 1
        assert result.hop_start == 3
        assert result.hop_limit == 3
        assert result.rx_time is not None
        assert result.packet_id == 987654321

    def test_nak_normalizes_with_error_reason(self) -> None:
        packet = _routing_packet()
        packet["decoded"]["routing"] = {"errorReason": "NO_CHANNEL"}
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.outcome == "nak"
        assert result.error_reason == "NO_CHANNEL"

    def test_symbolic_lowercase_portnum_is_accepted(self) -> None:
        packet = _routing_packet()
        packet["decoded"]["portnum"] = "routing"
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.outcome == "ack"

    def test_numeric_from_derives_canonical_node_id(self) -> None:
        packet = _routing_packet()
        del packet["fromId"]
        packet["from"] = 0xA1B2C3D4
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.from_id == "!a1b2c3d4"

    def test_boolean_from_id_is_not_a_node_number(self) -> None:
        packet = _routing_packet()
        del packet["fromId"]
        packet["from"] = True
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.from_id is None

    def test_relay_node_normalizes_and_rejects_booleans(self) -> None:
        packet = _routing_packet()
        packet["relayNode"] = 7
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.relay_node == 7

        packet["relayNode"] = True
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.relay_node is None

    def test_ack_packet_own_id_zero_is_explicit_not_none(self) -> None:
        """Zero is a value for the packet's own id, never a fallback."""
        packet = _routing_packet(id=0)
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.packet_id == 0

    def test_ack_packet_own_id_boolean_is_dropped(self) -> None:
        packet = _routing_packet(id=True)
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.packet_id is None


class TestMalformedRoutingRejection:
    """Anything uninterpretable is refused with a typed reason."""

    @pytest.mark.parametrize(
        "packet",
        [
            None,
            "routing",
            17,
            {"decoded": None},
            {"decoded": "routing"},
            {"id": 1, "decoded": {"portnum": "TEXT_MESSAGE_APP"}},
        ],
    )
    def test_non_routing_shapes_are_rejected(self, packet: Any) -> None:
        result = normalize_native_routing_result(packet)
        assert isinstance(result, RejectedNativeRouting)
        assert result.reason in (REJECT_MALFORMED, REJECT_NOT_ROUTING)

    @pytest.mark.parametrize(
        "routing_payload",
        [None, "NONE", {}, {"errorReason": ""}, {"errorReason": 5}],
    )
    def test_uninterpretable_error_reason_is_rejected(
        self, routing_payload: Any
    ) -> None:
        packet = _routing_packet()
        packet["decoded"]["routing"] = routing_payload
        result = normalize_native_routing_result(packet)
        assert isinstance(result, RejectedNativeRouting)
        assert result.reason in (REJECT_MALFORMED, REJECT_NO_ERROR_REASON)

    @pytest.mark.parametrize("request_id", [None, "42", True, False, -1, 1.5, [42]])
    def test_non_plain_integer_request_ids_are_rejected(self, request_id: Any) -> None:
        packet = _routing_packet()
        packet["decoded"]["requestId"] = request_id
        result = normalize_native_routing_result(packet)
        assert isinstance(result, RejectedNativeRouting)
        assert result.reason == REJECT_REQUEST_ID_INVALID

    def test_zero_request_id_is_explicitly_rejected(self) -> None:
        """Firmware treats request id zero as not-an-ack; never a fallback."""
        packet = _routing_packet()
        packet["decoded"]["requestId"] = 0
        result = normalize_native_routing_result(packet)
        assert isinstance(result, RejectedNativeRouting)
        assert result.reason == REJECT_REQUEST_ID_ZERO


class TestAckSourceClassification:
    """Who apparently acknowledged, and the honest strength of each claim."""

    def test_own_node_is_local_implicit(self) -> None:
        result = normalize_native_routing_result(_routing_packet())
        assert isinstance(result, NativeRoutingResult)
        assert (
            classify_native_ack_source(result, own_node_id="!a1b2c3d4")
            == "local_implicit"
        )

    def test_destination_match_is_only_a_claim(self) -> None:
        result = normalize_native_routing_result(_routing_packet())
        assert isinstance(result, NativeRoutingResult)
        assert (
            classify_native_ack_source(result, expected_destination_id="!a1b2c3d4")
            == "claimed_destination"
        )

    def test_unrelated_node_is_third_party(self) -> None:
        result = normalize_native_routing_result(_routing_packet())
        assert isinstance(result, NativeRoutingResult)
        assert (
            classify_native_ack_source(
                result,
                own_node_id="!99998888",
                expected_destination_id="!11112222",
            )
            == "third_party"
        )

    def test_unknown_sender_is_undetermined(self) -> None:
        packet = _routing_packet()
        del packet["fromId"]
        packet["from"] = True  # unusable sender
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert result.from_id is None
        assert classify_native_ack_source(result) == "undetermined"

    def test_nak_sender_is_undetermined_for_positive_purposes(self) -> None:
        packet = _routing_packet()
        packet["decoded"]["routing"] = {"errorReason": "NO_CHANNEL"}
        result = normalize_native_routing_result(packet)
        assert isinstance(result, NativeRoutingResult)
        assert (
            classify_native_ack_source(result, own_node_id="!a1b2c3d4")
            == "undetermined"
        )


class TestPositiveEvidenceNeverQualifies:
    """No source can back a delivered observation under the contract."""

    @pytest.mark.parametrize(
        "source",
        ["local_implicit", "claimed_destination", "third_party", "undetermined"],
    )
    def test_every_source_is_rejected_with_a_reason(self, source: str) -> None:
        reason = positive_evidence_rejection(source)  # type: ignore[arg-type]
        assert isinstance(reason, str)
        assert reason.strip()
        assert "does not qualify" in reason

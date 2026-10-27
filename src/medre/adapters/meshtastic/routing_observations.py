"""Strict normalization of native Meshtastic routing callback packets.

Meshtastic delivers ROUTING_APP acknowledgment and error packets to the
same ``meshtastic.receive`` callback as chat traffic. This module is the
pure normalization boundary for those packets: it turns a raw SDK packet
dict into a typed :class:`NativeRoutingResult`, or a typed
:class:`RejectedNativeRouting` when the packet cannot be interpreted.

Everything here is classification only. No function in this module
touches storage, lifecycle state, or delivery feedback.

Evidence honesty
----------------

The firmware only answers ``want_ack`` packets with remote routing
results, and MEDRE's supported send path never sets ``want_ack``
(:func:`meshtastic.MeshInterface.sendText` defaults to ``wantAck=False``;
structured sends pass ``wantAck=False`` explicitly). The only ROUTING_APP
packet that can reference a MEDRE-sent broadcast today is the local
node's own implicit acknowledgment for an overheard rebroadcast
(firmware ``ReliableRouter::perhapsGenerateImplicitAckForOwnOverheard``),
which carries ``fromId`` equal to the local node and proves mesh
flooding activity only — never recipient acceptance.

Accordingly :func:`positive_evidence_rejection` returns a rejection
reason for *every* source classification: no native routing result that
MEDRE can observe qualifies as ``remote_service`` or ``end_to_end``
delivery evidence under the current contract. Callers MUST NOT construct
a positive delivery observation from any value this module produces.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping

from medre.adapters.meshtastic.packet_classifier import normalize_portnum
from medre.adapters.meshtastic.startup_backlog import extract_meshtastic_rx_time

# Canonical "!{num:08x}" node-id width, matching the packet classifier.
_NODE_ID_HEX_WIDTH: int = 8

NativeAckSource = Literal[
    "local_implicit",
    "claimed_destination",
    "third_party",
    "undetermined",
]

REJECT_NOT_ROUTING = "packet is not a routing payload"
REJECT_MALFORMED = "malformed routing packet"
REJECT_REQUEST_ID_ZERO = "routing request id zero cannot identify an original packet"
REJECT_REQUEST_ID_INVALID = "routing request id is not a plain integer"
REJECT_NO_ERROR_REASON = "routing packet carries no interpretable error reason"

# Acknowledgment source classifications and their defensible meaning.
_SOURCE_MEANING: dict[NativeAckSource, str] = {
    "local_implicit": (
        "local node echoed its own implicit acknowledgment for an overheard "
        "rebroadcast; proves mesh relaying activity only"
    ),
    "claimed_destination": (
        "a node at the destination address claimed routing acceptance of one "
        "packet; the claim is not authenticated and no user receipt is proven"
    ),
    "third_party": (
        "an unrelated node acknowledged; no destination acceptance is implied"
    ),
    "undetermined": (
        "the acknowledging node could not be identified; no acceptance is " "implied"
    ),
}


@dataclass(frozen=True, slots=True)
class NativeRoutingResult:
    """Typed facts read from one native routing callback packet.

    Every field is a *native event fact*: it comes from the packet the
    SDK delivered and is only as trustworthy as that delivery. Nothing
    here is a local correlation fact.
    """

    outcome: Literal["ack", "nak"]
    request_id: int
    error_reason: str
    from_id: str | None
    to_id: str
    channel_index: int | None
    packet_id: int | None
    hop_start: int | None
    hop_limit: int | None
    rx_time: datetime | None
    relay_node: int | None


@dataclass(frozen=True, slots=True)
class RejectedNativeRouting:
    """A routing-shaped packet that normalization refuses to interpret."""

    reason: str


def _normalize_node_id(raw: Any) -> str | None:
    """Return a canonical node id string, or ``None`` when unusable.

    Accepts ``"!abc123"`` style strings and plain integer node numbers.
    Booleans are rejected before the integer branch so ``True`` can never
    masquerade as node number 1.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        stripped = raw.strip()
        return stripped or None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        try:
            return f"!{int(raw):0{_NODE_ID_HEX_WIDTH}x}"
        except (TypeError, ValueError, OverflowError):
            return None
    return None


def _normalize_int_field(raw: Any) -> int | None:
    """Return *raw* as a plain int, rejecting bools and non-integers."""
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    return raw


def normalize_native_routing_result(
    packet: Any,
) -> NativeRoutingResult | RejectedNativeRouting:
    """Normalize one raw SDK packet dict into typed routing facts.

    Rejects anything that is not a decoded ROUTING_APP packet with an
    interpretable error reason and a positive integer ``requestId``.
    Zero is explicitly rejected (the firmware treats request id zero as
    "not an acknowledgment"); it is never silently replaced.
    """
    if not isinstance(packet, Mapping):
        return RejectedNativeRouting(reason=REJECT_MALFORMED)
    decoded = packet.get("decoded")
    if not isinstance(decoded, Mapping):
        return RejectedNativeRouting(reason=REJECT_MALFORMED)
    if normalize_portnum(decoded.get("portnum")) != "routing":
        return RejectedNativeRouting(reason=REJECT_NOT_ROUTING)
    routing = decoded.get("routing")
    if not isinstance(routing, Mapping):
        return RejectedNativeRouting(reason=REJECT_MALFORMED)
    raw_reason = routing.get("errorReason")
    if not isinstance(raw_reason, str) or not raw_reason.strip():
        return RejectedNativeRouting(reason=REJECT_NO_ERROR_REASON)
    outcome: Literal["ack", "nak"] = "ack" if raw_reason.strip() == "NONE" else "nak"

    raw_request_id = decoded.get("requestId")
    if isinstance(raw_request_id, bool):
        return RejectedNativeRouting(reason=REJECT_REQUEST_ID_INVALID)
    if isinstance(raw_request_id, int):
        if raw_request_id == 0:
            return RejectedNativeRouting(reason=REJECT_REQUEST_ID_ZERO)
        if raw_request_id < 0:
            return RejectedNativeRouting(reason=REJECT_REQUEST_ID_INVALID)
        request_id = raw_request_id
    else:
        return RejectedNativeRouting(reason=REJECT_REQUEST_ID_INVALID)

    # The acknowledgment's own packet id is diagnostic only. Zero is an
    # explicit value and is preserved; booleans are rejected.
    raw_packet_id = packet.get("id")
    if isinstance(raw_packet_id, bool):
        packet_id = None
    elif isinstance(raw_packet_id, int):
        packet_id = raw_packet_id
    else:
        packet_id = None

    relay_node = _normalize_int_field(packet.get("relayNode"))
    return NativeRoutingResult(
        outcome=outcome,
        request_id=request_id,
        error_reason=raw_reason.strip(),
        from_id=_normalize_node_id(packet.get("fromId", packet.get("from"))),
        to_id=str(packet.get("toId", "") or ""),
        channel_index=_normalize_int_field(packet.get("channel")),
        packet_id=packet_id,
        hop_start=_normalize_int_field(packet.get("hopStart")),
        hop_limit=_normalize_int_field(packet.get("hopLimit")),
        rx_time=extract_meshtastic_rx_time(packet),
        relay_node=relay_node,
    )


def classify_native_ack_source(
    result: NativeRoutingResult,
    *,
    own_node_id: str | None = None,
    expected_destination_id: str | None = None,
) -> NativeAckSource:
    """Classify who apparently emitted a routing acknowledgment.

    ``local_implicit`` fires when the acknowledgment's sender is the
    local node itself — the firmware's implicit acknowledgment for an
    overheard rebroadcast. ``claimed_destination`` requires the caller to
    supply the original packet's destination; the match is address
    equality only and never authenticates the sender.
    """
    if result.outcome != "ack" or result.from_id is None:
        return "undetermined"
    if own_node_id is not None and result.from_id == own_node_id:
        return "local_implicit"
    if (
        expected_destination_id is not None
        and result.from_id == expected_destination_id
    ):
        return "claimed_destination"
    return "third_party"


def positive_evidence_rejection(source: NativeAckSource) -> str:
    """Return why *source* can never qualify as positive delivery evidence.

    Every classification is rejected: the current contract admits
    positive packet observations only at ``remote_service`` or
    ``end_to_end`` strength, and no Meshtastic routing result reachable
    from MEDRE's supported send path establishes either. The returned
    string is the defensible meaning of the fact, phrased as a
    rejection.
    """
    meaning = _SOURCE_MEANING[source]
    return f"{meaning}; does not qualify as delivered evidence"

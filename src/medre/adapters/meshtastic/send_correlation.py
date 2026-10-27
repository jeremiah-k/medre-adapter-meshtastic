"""Process-local correlation of accepted native sends to routing callbacks.

When the outbound queue finishes one paced send, the adapter records the
accepted packet's trusted local facts (exact attempt provenance fields,
native packet id, native channel, owning connection generation). When a
native routing callback later names that packet id, this registry pairs
the two and states — explicitly — whether the paired fact could enter
the core observation contract.

Design constraints:

* **Bounded.** The registry holds at most *capacity* entries for at
  most *retention_seconds* (monotonic). Expired acceptance facts cannot
  match delayed results. Bounded lifetime ID tombstones prevent reuse;
  once capacity is exhausted, new IDs are refused until adapter restart.
  This limits diagnostics only and never affects native sending.
* **Session-fenced.** Each entry carries the native connection
  generation that owned the send. A callback observed under a different
  generation (reconnect, replacement client, restart) cannot match it.
  Consequence: delayed evidence that arrives after a reconnect or
  process restart is discarded. This map is process-local and is not
  durable historical correlation.
* **Fail closed.** A result observed before its acceptance is recorded,
  a reused packet id, or two disagreeing outcomes for one id are all
  counted and refused. Nothing is buffered for later submission and no
  native packet is ever independently submitted or replayed.
* **No positive emission.** Even a perfectly correlated acknowledgment
  cannot qualify as delivered evidence (see
  :mod:`medre.adapters.meshtastic.routing_observations`). This registry
  therefore never constructs a core observation; it exists to prove,
  with counters, that every adversarial shape fails closed.

All methods are thread-safe: the SDK delivers callbacks on its reader
thread while sends complete on the event loop.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from medre.adapters.meshtastic.routing_observations import (
    NativeRoutingResult,
    RejectedNativeRouting,
    classify_native_ack_source,
    positive_evidence_rejection,
)

# Conservative fallbacks when the caller does not wire configuration.
_DEFAULT_CAPACITY: int = 1024
_DEFAULT_RETENTION_SECONDS: float = 300.0

CorrelationOutcome = Literal["ack", "nak", "duplicate"]


@dataclass(frozen=True, slots=True)
class AcceptedSendFacts:
    """Trusted local facts for one accepted native send.

    These fields come from MEDRE's own queue item and handoff result —
    not from the native event. ``connection_generation`` is the session
    generation that owned the transport during the send.
    """

    native_message_id: str
    native_channel_id: str | None
    connection_generation: int
    event_id: str | None
    delivery_plan_id: str | None
    outbox_id: str | None
    attempt_number: int | None


@dataclass(frozen=True, slots=True)
class CorrelatedRoutingResult:
    """A native routing result paired with its original accepted send.

    ``source_meaning`` is always a *rejection* reason: no correlated
    fact qualifies as positive delivery evidence under the current
    contract. ``facts`` is ``None`` for a duplicate result whose
    original entry has already been consumed; it is never reconstructed
    from the newer callback.
    """

    outcome: CorrelationOutcome
    facts: AcceptedSendFacts | None
    native: NativeRoutingResult
    ack_source: str
    source_meaning: str


@dataclass(frozen=True, slots=True)
class UncorrelatedRoutingResult:
    """A routing result that could not be safely paired, with the reason."""

    reason: str


def _native_id_key(native_message_id: str) -> int | None:
    """Return the integer correlation key for a native id string.

    Accepts decimal strings (including ``"0"`` — zero is an explicit
    value, not a fallback) and returns ``None`` for anything that can
    never equal a callback request id.
    """
    stripped = native_message_id.strip()
    if not stripped or not stripped.isdigit():
        return None
    try:
        return int(stripped)
    except ValueError:  # pragma: no cover - isdigit excludes this
        return None


class MeshtasticSendCorrelation:
    """Bounded, session-fenced map of native packet ids to accepted sends."""

    def __init__(
        self,
        *,
        capacity: int = _DEFAULT_CAPACITY,
        retention_seconds: float = _DEFAULT_RETENTION_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise ValueError("capacity must be an int")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if isinstance(retention_seconds, bool) or not isinstance(
            retention_seconds, (int, float)
        ):
            raise ValueError("retention_seconds must be a number")
        if not math.isfinite(retention_seconds) or retention_seconds <= 0:
            raise ValueError("retention_seconds must be > 0")
        self._capacity = capacity
        self._retention_seconds = float(retention_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._closed = False
        # integer request id → (facts, inserted monotonic time)
        self._entries: dict[int, tuple[AcceptedSendFacts, float]] = {}
        self._order: list[int] = []
        # ids that must never resolve again (reused or conflicted)
        self._ambiguous: set[int] = set()
        self._resolved: dict[int, tuple[str, int, str | None]] = {}
        # Bounded lifetime tombstones prevent late results from matching reused IDs.
        self._known: set[int] = set()
        self.capacity_refusals = 0
        # counters
        self.recorded = 0
        self.unmatchable_ids = 0
        self.evicted = 0
        self.matched_acks = 0
        self.matched_naks = 0
        self.duplicates = 0
        self.conflicting = 0
        self.unmatched = 0
        self.stale_generation = 0
        self.rejected_normalization = 0
        self.after_close = 0
        self.expired = 0

    # -- Acceptance side (event loop) ---------------------------------------

    def record(self, facts: AcceptedSendFacts) -> bool:
        """Record one accepted send's trusted local facts.

        Returns ``True`` when the facts were retained under a matchable
        native id. A non-numeric native id can never equal a callback
        request id and is counted instead of guessed into a key.
        Recording the same native id twice marks it permanently
        ambiguous: reuse makes every future result for it unresolvable.
        """
        key = _native_id_key(facts.native_message_id)
        now = self._clock()
        with self._lock:
            if key is None:
                self.unmatchable_ids += 1
                return False
            if self._closed:
                self.after_close += 1
                return False
            self._enforce_bounds(now)
            if key in self._known:
                self._ambiguous.add(key)
                self._entries.pop(key, None)
                self._resolved.pop(key, None)
                if key in self._order:
                    self._order.remove(key)
                return False
            if len(self._known) >= self._capacity:
                self.capacity_refusals += 1
                return False
            self._known.add(key)
            self._entries[key] = (facts, now)
            self._order.append(key)
            self.recorded += 1
            self._enforce_bounds(now)
            return True

    def _enforce_bounds(self, now: float) -> None:
        """Expire acceptance facts while retaining bounded lifetime ID tombstones."""
        cutoff = now - self._retention_seconds
        remaining: list[int] = []
        for native_id in self._order:
            entry = self._entries.get(native_id)
            if entry is not None and entry[1] < cutoff:
                del self._entries[native_id]
                self.expired += 1
                self.evicted += 1
            else:
                remaining.append(native_id)
        self._order = remaining
        while len(self._order) > self._capacity:
            oldest = self._order.pop(0)
            if self._entries.pop(oldest, None) is not None:
                self.evicted += 1

    # -- Callback side (SDK reader thread) -----------------------------------

    def observe(
        self,
        routing: NativeRoutingResult | RejectedNativeRouting,
        *,
        connection_generation: int | None = None,
        own_node_id: str | None = None,
    ) -> CorrelatedRoutingResult | UncorrelatedRoutingResult:
        """Pair one native routing result with its original accepted send.

        Returns a correlated result only when the request id names an
        exactly recorded, unambiguous, generation-matching acceptance.
        Every other shape is refused with a counted reason. The
        correlated result never carries emittable evidence; its
        ``source_meaning`` explains why. ``own_node_id`` is the local
        node's id: an acknowledgment emitted by the local node itself
        classifies as the firmware's local implicit acknowledgment
        rather than a third party.
        """
        if isinstance(routing, RejectedNativeRouting):
            with self._lock:
                self.rejected_normalization += 1
            return UncorrelatedRoutingResult(
                reason=f"normalization rejected: {routing.reason}"
            )
        key = routing.request_id
        with self._lock:
            if self._closed:
                self.after_close += 1
                return UncorrelatedRoutingResult(reason="correlation registry closed")
            self._enforce_bounds(self._clock())
            if key in self._ambiguous:
                self.conflicting += 1
                return UncorrelatedRoutingResult(
                    reason="native packet id was reused; the result is ambiguous"
                )
            previous = self._resolved.get(key)
            facts_entry = self._entries.get(key)
            retained_scope = (
                (facts_entry[0].connection_generation, facts_entry[0].native_channel_id)
                if facts_entry is not None
                else previous[1:] if previous is not None else None
            )
            if retained_scope is not None:
                expected_generation, expected_channel = retained_scope
                if (
                    type(connection_generation) is not int
                    or connection_generation != expected_generation
                ):
                    self.stale_generation += 1
                    return UncorrelatedRoutingResult(
                        reason="result was observed under a retired connection generation"
                    )
                if routing.channel_index is not None and expected_channel != str(
                    routing.channel_index
                ):
                    self.unmatched += 1
                    return UncorrelatedRoutingResult(
                        reason="native channel does not match acceptance"
                    )
            if facts_entry is None:
                if previous is None:
                    self.unmatched += 1
                    return UncorrelatedRoutingResult(
                        reason="no retained acceptance names this native packet id"
                    )
                if previous[0] == routing.outcome:
                    self.duplicates += 1
                    source = classify_native_ack_source(
                        routing, own_node_id=own_node_id
                    )
                    return CorrelatedRoutingResult(
                        outcome="duplicate",
                        facts=None,
                        native=routing,
                        ack_source=source,
                        source_meaning=positive_evidence_rejection(source),
                    )
                self.conflicting += 1
                return UncorrelatedRoutingResult(
                    reason="conflicting outcomes reported for one native packet id"
                )
            facts, _inserted_at = facts_entry
            del self._entries[key]
            self._order.remove(key)
            outcome: CorrelationOutcome = routing.outcome
            self._resolved[key] = (
                routing.outcome,
                facts.connection_generation,
                facts.native_channel_id,
            )
            if routing.outcome == "ack":
                self.matched_acks += 1
            else:
                self.matched_naks += 1
            source = classify_native_ack_source(routing, own_node_id=own_node_id)
            return CorrelatedRoutingResult(
                outcome=outcome,
                facts=facts,
                native=routing,
                ack_source=source,
                source_meaning=positive_evidence_rejection(source),
            )

    # -- Lifecycle ------------------------------------------------------------

    def clear(self) -> None:
        """Drop every retained correlation and close the registry.

        After close, later callbacks are counted and refused. A stopped
        adapter never correlates again; a fresh registry is created on
        the next start.
        """
        with self._lock:
            self._closed = True
            self._entries.clear()
            self._order.clear()
            self._ambiguous.clear()
            self._resolved.clear()
            self._known.clear()

    # -- Diagnostics ----------------------------------------------------------

    def snapshot(self) -> dict[str, int | bool]:
        """Return counter values for adapter diagnostics."""
        with self._lock:
            return {
                "active": len(self._entries),
                "retained_ids": len(self._known),
                "capacity_refusals": self.capacity_refusals,
                "capacity": self._capacity,
                "retention_seconds": int(self._retention_seconds),
                "recorded": self.recorded,
                "unmatchable_ids": self.unmatchable_ids,
                "evicted": self.evicted,
                "expired": self.expired,
                "matched_acks": self.matched_acks,
                "matched_naks": self.matched_naks,
                "duplicates": self.duplicates,
                "conflicting": self.conflicting,
                "unmatched": self.unmatched,
                "stale_generation": self.stale_generation,
                "rejected_normalization": self.rejected_normalization,
                "after_close": self.after_close,
                "closed": self._closed,
            }

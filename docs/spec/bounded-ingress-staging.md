# Bounded reader-thread ingress staging

> **Authority:** Authoritative specification for bounded, loss-honest staging
> of Meshtastic relayable ingress between the SDK reader thread and the
> canonical inbound pipeline.

The Meshtastic SDK delivers inbound packets on its own reader thread. Staging
one coroutine-scheduling submission per relayable packet outside core's
bounded admission gate creates producer-side pressure the admission gate
cannot see. This page defines the staging contract for that boundary.

## Staging bound

The adapter MUST stage relayable inbound packets through a finite
pre-scheduling reservation pool sized by `max_pending_ingress_callbacks`.
The setting MUST be a positive integer; zero, negative, boolean, and
non-integer values MUST be rejected by configuration validation. There MUST
NOT be an unbounded mode. The default MUST be a finite value aligned with
core's admission default; the shipped default (``100``) is provisional
pending a measured-budget qualification round and MUST NOT be documented as
a throughput guarantee.

Reservation acquisition MUST NOT block the SDK reader thread: a reader
thread MUST never park waiting for a staging slot. An acquire that cannot
obtain a slot MUST refuse the packet immediately.

## Classification boundary

Only relayable text ingress MUST be bounded. ACK/NAK responses, native
routing-correlation callbacks, observations, and other non-relay
classifications MUST NOT consume staging slots and MUST NOT be throttled,
delayed, or discarded by staging pressure. The bound MUST apply immediately
after the relay gate and startup-backlog suppression gate, before node
enrichment and codec decoding, so producer-refused packets avoid those costs.
A reservation MUST be returned on decoding errors and if scheduling cannot
proceed because the owner loop is unavailable.

## Overflow semantics

A refused packet MUST NOT be scheduled, MUST NOT be admitted, and MUST NOT
consume dedup state or native identity. The refusal MUST be explicit,
loss-honest accounting: the adapter MUST increment a `producer_overflow`
diagnostics counter, report an `overflow` producer-pressure fact with
reason `callback_capacity` through the optional
`AdapterContext.report_producer_pressure` sink when present. The first refusal
of a session MUST emit a content-free warning, and later warnings SHOULD be
rate-limited to at most one per five seconds with a cumulative refused count.
Every refusal MUST still increment the counter and report pressure even when
its warning is suppressed. Warnings MUST NOT include message text, sender
identity, or key material. The refusal MUST NOT imply admission, authorise retry or resend, or route
the packet through any side queue or adapter-local store.

## Reported outcomes

The adapter MUST report pre-admission staging facts through the optional
producer-pressure sink using the closed core vocabulary:

- `scheduled` — a packet was staged toward admission after a reservation
  was acquired. Count 1 per staged packet.
- `overflow` — the reservation was refused. Count 1 per refused packet with
  reason `callback_capacity`.
- `cancelled_before_admission` — a staged submission was cancelled before
  its publication attempt began, for example by stop-drain. Stop-drain SHOULD
  report one coalesced report naming the cancelled count instead of one
  report per future.

Reports MUST be content-free (adapter id, outcome, count, machine-token
reason) and MUST carry the reporting adapter's own id. The sink is
optional: contexts without it MUST retain identical staging and refusal
behavior, and sink failures MUST NOT disturb the reader thread, the
publish path, or shutdown.

## Reservation lifecycle

A reservation MUST be released exactly once when its staged publication
completes, fails, or is cancelled. Stopping the adapter MUST cancel staged
futures and release their reservations. Reservations MUST be session-exact:
a stop or a connection-generation change (reconnect) MUST retire all held
reservations, and a new session MUST begin from full capacity without
inheriting stale slots. Releases captured by a retired pool MUST NOT grant
capacity to a newer pool. The callback-after-stop fence MUST remain
intact: a reader-thread callback arriving after stop MUST neither schedule
a submission nor consume a reservation.

A delayed callback MUST NOT rewind the reservation pool to an older connection
generation or replenish an exhausted current-generation budget. Each callback
MUST capture its reservation pool with its session snapshot so restart cannot
redirect a retired reader's reservation to the replacement pool.

Cancellation of a running publisher or its cross-thread future MUST NOT be
interpreted as evidence that admission never began. Stop-drain MUST report
`cancelled_before_admission` only for submissions whose publication attempt is
known not to have started. Cancellation after that boundary is ambiguous and
MUST NOT be projected into that outcome, even if publish has not returned.

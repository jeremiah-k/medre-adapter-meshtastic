# Bound Meshtastic reader-thread ingress staging with reported overflow

Relayable Meshtastic ingress is bounded **before** scheduling: the SDK
reader thread takes a non-blocking reservation from a finite per-session
pool sized by the `max_pending_ingress_callbacks` setting (default ``100``,
provisional pending a measured-budget qualification round; finite-only,
non-positive values are rejected) before staging a packet. A reader thread
never parks waiting for a slot.

A refused packet is not scheduled, not admitted, and consumes no dedup
state or native identity. The loss is explicit: it increments the
`producer_overflow` diagnostics counter, is logged once without message
content or sender identity, and is reported as an `overflow`
producer-pressure fact with reason `callback_capacity` through the
optional core `report_producer_pressure` sink. Staged packets report
`scheduled`; stop-drain cancellations report one coalesced
`cancelled_before_admission` count. Contexts without the optional sink
behave identically minus the reports.

Reservations release on completion, error, cancellation, and stop, and are
session-exact: stop and reconnect retire all held slots and the next
session starts from full capacity. ACK/NAK and routing-correlation
callbacks bypass the bound entirely. The pre-cap measurements in
`docs/dev/ingress-pressure.md` remain as history; the reader-thread burst
tests pin the bounded contract instead of unbounded growth.

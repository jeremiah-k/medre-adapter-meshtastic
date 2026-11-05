# Meshtastic reader-thread ingress pressure

`tests/test_meshtastic_ingress_pressure.py` reproduces a bounded **synthetic**
callback burst against the real adapter in `connection_type="fake"`. A blocked
canonical publisher holds all submissions so they cannot complete before the
staging high-water mark is observed. Calls are made from a separate thread,
matching the SDK's synchronous reader callback ownership. Each emitted frame
has a distinct native ID and passes the actual classifier/codec path.

Run the boundary tests without a radio or network:

```sh
MEDRE_PRESSURE_REPORT_DIR=./pressure-results pytest \
  tests/test_meshtastic_ingress_pressure.py -q
```

## Pre-cap measurements (historical)

The measurements below were taken **without any staging bound**, when the
source created one `run_coroutine_threadsafe` future per eligible packet
before downstream async admission. With 32 and 256 distinct blocked callbacks,
respectively 32 and 256 futures remained outstanding; every event published
after release, with none retained after drain. A separate test stopped while a
publication was held and confirmed that its underlying publisher task exits
through cancellation, even after the future tracking set is cleared. Those
runs showed linear scheduler pressure: the reader thread had no synchronous
flow control, and an unbounded pending-future set was the measured result.

## Bounded staging

Relayable ingress is bounded **before** scheduling. A finite per-session
reservation pool sized by `max_pending_ingress_callbacks` (default ``100``)
holds one slot per staged packet, and the reader thread stages a packet only
when a slot is free. Capacity is checked after relay/backlog classification but
before node enrichment and codec decoding:

- Acquisition is non-blocking. The SDK reader thread never parks waiting for
  a slot.
- A refusal means the packet is not scheduled, not admitted, and consumes no
  dedup state or native identity. The refusal is explicit loss: it is counted
  in the `producer_overflow` diagnostics counter, reported as an `overflow`
  producer-pressure report with reason `callback_capacity` through the
  optional `report_producer_pressure` sink. The first refusal of each session
  emits a content-free warning; later warnings are limited to at most one per
  five seconds, showing the cumulative count. Every refusal remains counted
  and reported, even when its warning is suppressed. There is no retry, side
  queue or admission implication — the packet is gone.
- A staged packet reports `scheduled` (count 1) through the same sink. When
  stop-drain cancels submissions before their publication attempt begins,
  one coalesced `cancelled_before_admission` report names only submissions
  known not to have entered publication. Running cancellation is ambiguous.
- The reservation is released on completion, error, cancellation, and stop.
  Reservations are session-exact: `stop()` and a reconnect (connection
  generation change) retire all held slots, and a new session starts from
  full capacity.
- ACK/NAK, observation, and routing-correlation callbacks bypass the bound
  entirely; only relayable text ingress is throttled.
- Contexts without the optional sink (older core, direct tests) behave
  identically minus the reports.

The default of ``100`` is **provisional**, aligned with the core admission
default. It is pending a measured-budget qualification round and is not a
throughput guarantee. The value is finite-only: non-positive values are
rejected by configuration validation, and there is no unbounded option.

The outbound paced queue's capacity is a separate contract.

Delayed reader callbacks cannot rewind the reservation generation, and each
callback captures the pool belonging to its session snapshot. Missing optional
report types on compatible older cores leave the local bound and counters
functional. Cancellation of a running publisher is not negative admission proof;
it does not contribute to `cancelled_before_admission`.

The bound covers tracked cross-thread submissions per reservation generation.
A cancelled future can become terminal before its underlying coroutine exits,
and reconnect installs fresh capacity. A process-wide limit on surviving,
cancellation-resistant publishers is not established by this staging bound.

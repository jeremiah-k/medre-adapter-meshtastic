# Meshtastic reader-thread ingress pressure

`tests/test_meshtastic_ingress_pressure.py` reproduces a bounded **synthetic**
callback burst against the real adapter in `connection_type="fake"`. A blocked
canonical publisher holds all submissions so they cannot complete before the
staging high-water mark is observed. Calls are made from a separate thread,
matching the SDK's synchronous reader callback ownership. Each emitted frame
has a distinct native ID and passes the actual classifier/codec path.
`tests/test_meshtastic_sustained_ingress.py` extends that suite with the
sustained-ingress qualification record below: above-cap bursts, a sustained
imbalance cycle, stop/restart under full staging, and memory/latency evidence.
`tests/test_meshtastic_process_lifetime_ingress.py` extends the record across
restart cycles in one process: exact per-cycle staging ledgers, retired-pool
isolation under late releasers, and whole-process memory guards.

Run the boundary tests without a radio or network:

```sh
MEDRE_PRESSURE_REPORT_DIR=./pressure-results pytest \
  tests/test_meshtastic_ingress_pressure.py \
  tests/test_meshtastic_sustained_ingress.py \
  tests/test_meshtastic_process_lifetime_ingress.py -q
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
default. The measured qualification record below justifies the bound's
loss-honest behavior, not a throughput budget; the default is not a
throughput guarantee. The value is finite-only: non-positive values are
rejected by configuration validation, and there is no unbounded option.

The outbound paced queue's capacity is a separate contract.

## Above-cap qualification (measured record)

Recorded 2026-11-04 on the synthetic SDK callback tier
(``connection_type="fake"``, default cap ``100``, no radio, network, or SDK
install; all numbers content-free). The sustained-ingress suite runs in about
1.5 seconds, and ``MEDRE_PRESSURE_REPORT_DIR`` exports one content-free JSON
record per run.

- Above-cap bursts at 1x, 4x, and 16x of the cap (100, 400, and 1600
  relayable packets with publication held): exactly 100 futures staged,
  exactly N−100 refused, and the accounting reconciled exactly — the
  ``producer_overflow`` counter equals N−100, overflow reports sum to N−100
  with reason ``callback_capacity``, and scheduled reports sum to 100.
  Nothing was published for a refused packet. The staged 100 delivered in
  full and the future set drained to zero on release. A repeated burst in
  the same session reused full capacity: no slot leaks across cycles.
- Sustained imbalance: 2,400 submitted callbacks across 20 lock-step cycles
  (120 produced per cycle against 100 slots, 16-slot release batches,
  condition-variable driven with no wall-clock sleeps) ended with 404
  staged/published and 1,996 explicit refusals. At every cycle boundary the
  invariants held exactly: submitted == staged + refused, staged ≤ 100, and
  staged == published + in-flight.
- Stop under saturation: stopping with a held publisher and full staging
  cancelled every staged submission and left the retired pool fully
  re-acquirable; the report counted only futures actually cancelled
  pre-start (zero here — every held publisher had started, and running
  cancellation stays ambiguous per the spec). A wedged-loop variant in
  which all 100 staged submissions were deterministically queued-unstarted
  produced exactly one coalesced ``cancelled_before_admission`` report with
  count 100, and a full stop afterwards double-counted nothing. The
  restarted session staged an above-cap burst under the identical contract
  from full capacity.
- Memory and reader-thread acquire latency (``tracemalloc`` peak of the held
  saturated state): the 100-burst peaked at ~0.94 MB, the 1600-burst at
  ~1.11 MB — about 1.2x growth for a 16x burst, asserted sublinear in CI
  (pending work tracks the cap, not the burst). The maximum observed
  reader ``acquire`` latency was 0.016–0.031 ms across runs against a
  100 ms bound: the acquire never parks, and this is not a throughput SLA.
  ``ru_maxrss`` is recorded in the run JSON as context only and is never
  asserted.

What this record proves: bounded staging under a 16x over-cap burst,
loss-honest accounting at every boundary, leak-free capacity reuse within a
session and across restart, and a reader thread that never parks. What it
does NOT establish: any throughput or delivery-latency guarantee, and no
claim about the real SDK, hardware, or radio paths — those stay in their
named gated tiers (``local_integration``, ``meshtastic_sdk``, ``live``,
``hardware``, ``soak``).

Delayed reader callbacks cannot rewind the reservation generation, and each
callback captures the pool belonging to its session snapshot. Missing optional
report types on compatible older cores leave the local bound and counters
functional. Cancellation of a running publisher is not negative admission proof;
it does not contribute to `cancelled_before_admission`.

The bound covers tracked cross-thread submissions per reservation generation.
A cancelled future can become terminal before its underlying coroutine exits,
and reconnect installs fresh capacity. A process-wide limit on surviving,
cancellation-resistant publishers is not established by this staging bound.

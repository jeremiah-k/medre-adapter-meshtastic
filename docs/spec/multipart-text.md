# Claim-retaining multipart text

`max_text_parts` MUST be an integer from 1 through 128, rejecting bool. Its default
1 retains single-packet clipping; values above 1 opt long ordinary messages into
core's exact immutable radio planner. This field MUST follow all existing
positional configuration fields. Short messages, zero-byte clipping and native
or descriptive reactions MUST retain their single-packet behavior.

The renderer MUST preserve the complete authored body, whitespace and Unicode.
Every packet MUST repeat the applied attribution prefix and numbered marker
within the effective configured UTF-8 budget. Context MUST NOT enlarge that
configured budget for a planned message. Exceeding the part cap MUST reject the
whole plan before native use. Proven native reply IDs MAY remain on each planned
packet; an unproven whole-message reference MUST retain readable fallback.

## Queue ownership and handoff

Core MUST persist the entire plan before admitting packets. Planned packet
results carry its one-use `before_native_send` callback and exact attempt
provenance. The adapter MUST refuse a complete plan delivered directly; its first
packet cannot stand in for whole-message delivery.

Planned packets MUST use the existing bounded outbound queue and serialized send
owner. The adapter MUST wait for each packet's actual local handoff while core
retains its current `in_progress` claim and lease renewal. Queue admission alone
MUST NOT return success or release that claim. No new canonical database, journal
or queued-row claim reconstruction is introduced. Ordinary messages MUST retain
their existing deferred completion/failure callback contract.

All queue consumers MUST serialize dequeue, pacing and handoff. The interval
MUST be measured from completion of the preceding queued transport invocation,
so a slow grant or SDK call cannot consume the next message's gap. Guarded
packets MUST perform at most one native invocation; neither SDK nor queue retries
may replay a consumed grant. Ordinary SDK retry policy remains unchanged.

Callbacks, futures and cancellation fences MUST remain runtime-only, outside
radio payloads, canonical events and durable metadata. Packet success/failure
MUST settle the waiting core dispatch rather than emit deferred parent feedback.
No individual native packet ID or ACK may complete the multipart parent.

## Cancellation and evidence

Pending cancellation MUST remove its packet from the queue and release waiter
ownership. Shutdown, disconnection and interrupted queue/result processing MUST
settle guarded waiters; ordinary pending work MUST retain its existing lifecycle
policy. A cancelled SDK worker that has not begun native use MUST be fenced.
A native operation already executing remains uncertain: cancelling a coroutine
MUST NOT be described as terminating that native thread.

The adapter session and SDK client/generation MUST be checked around the storage
grant. The worker MUST recheck cancellation and that exact client immediately
before native use, including structured reply sends. A consumed grant MUST NOT
be redirected to a replacement session or client.

Unlike MeshCore's ID-less channel acknowledgement, a guarded Meshtastic handoff
MUST retain the SDK's actual uint32 packet ID, including zero. Missing, malformed or
out-of-range IDs MUST remain unconfirmed and stop subsequent packets. Core's
`radio-local-handoff.schema.json` describes the common retained reference shape;
this provider enforces the stricter packet-ID domain at runtime. Confirmation
remains local transport, never independently observed recipient delivery.

Only a complete set of committed packet handoffs may aggregate. The parent MUST
omit a scalar native message ID rather than select an arbitrary fragment as a
reply target. Partial or uncertain messages remain held for inspection after
restart; automatic partial resumption and full-plan native ACK promotion are
separate work. The existing read-only radio-attempt inspector reports those facts
without recovering or resending them.

## Qualification

Deterministic tests exercise the actual renderer, queue, adapter and session with
controlled SDK calls and file-backed core SQLite. Fake/SDK CI MUST qualify the
candidate against core's merged guarded-handoff contract. Physical two-radio
receipts and mixed-topology interruption/restart remain distinct gates; no RF
claim follows from local queue statistics or a successful SDK return.

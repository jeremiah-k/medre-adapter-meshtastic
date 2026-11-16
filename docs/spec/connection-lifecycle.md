# Meshtastic connection lifecycle

## Initial connection

The session MUST run native interface construction outside the asyncio event
loop. The positive integer sync_timeout_ms setting (default 30000) bounds the
initial asynchronous wait in milliseconds and supplies the SDK connection and
configuration phase timeouts. TCP socket connection uses the smaller of that
budget and ten seconds.

An unavailable transport or an expired initial wait MUST leave the adapter
started and degraded, with recovery supervision active. Other adapters can then
start, and routes involving this instance remain eligible. Invalid configuration,
missing native dependencies, and initial callback setup errors remain startup
failures.

## Recovery ownership

Expiry of the initial wait MUST NOT launch a competing constructor. The session
MUST retain and adopt that attempt if it succeeds, or enter capped lifetime
retries after it fails. Transport recovery MUST stop when session shutdown begins.

A client completing after its async owner is cancelled MUST be closed instead
of adopted, and late constructor errors MUST be consumed. Stopping before the
recovery supervisor first runs MUST retain the same cleanup guarantee.

Python cancellation cannot forcibly terminate a blocking SDK thread. The native
SDK phase limits bound ordinary attempts; a malfunctioning dependency that
ignores them can delay executor or process teardown. The startup deadline is an
async wait budget, not a guarantee that every native phase has terminated.

## Health and delivery

Health MUST report degraded while disconnected and healthy after the connection
is restored, subject to existing queue-pressure diagnostics. Recovery MUST NOT
turn a queued or failed delivery into confirmed RF receipt. Core retry and native
handoff evidence retain their existing meanings and limits.

## Native construction during interpreter shutdown

Cancellation MUST mark a pending constructor result as abandoned independently
of its asyncio wrapper. A client returned after abandonment MUST be closed in
the native thread, even when event-loop shutdown has cancelled that wrapper.
A result published before cancellation MUST be retired exactly once by the
retained disposer. Neither path may activate a stopped session.

An outstanding native constructor MUST be reported when session shutdown leaves
it running. Coroutine cancellation does not terminate the SDK's synchronous
constructor; interpreter exit may await its timeout. Hard process-exit bounds
require an SDK-supported cancellation mechanism or an owned process boundary.

### Cooperative SDK construction cancellation

For SDK constructors that explicitly declare keyword-capable `cancelEvent`, the session passes
its invocation-owned abandonment event. Cancelling the construction coroutine
sets this event before late-result retirement. The event MUST NOT be retained as
session-wide cancellation state or reused for another attempt. SDKs without this
parameter retain the late-client disposal path. Native operations that do not
observe the event can still delay interpreter exit; the host does not claim to
terminate a thread.

## Recovery cadence and native diagnostics

The default reconnect delay MUST start at ten seconds and grow exponentially to
300 seconds, matching the long-outage recovery cadence used by MMRelay. Existing
`reconnect_backoff_initial_seconds` and `reconnect_backoff_max_seconds` settings
remain operator-controlled. Jitter MUST remain between the configured initial
value and cap. Recovery MUST continue until connected or stopped.

The adapter MUST retain its actionable reconnect warnings. Its declared SDK
logging defaults suppress duplicate warning-level BLE connection tracebacks;
ERROR records remain visible. Explicit `logging.overrides` for `meshtastic.ble`
or `meshtastic.ble_interface` MUST take precedence, allowing DEBUG diagnostics.
This log policy MUST NOT convert an unavailable host controller into a permanent
adapter failure or change Bluetooth power or pairing state.

## Reconnect receive watermark

A radio can hold undelivered packets in its FROMRADIO queue across a transport
drop and hand the whole queue to the next successful connection. Judged against
the original startup baselines, that queued pre-reconnect traffic could relay as
fresh. The adapter therefore records a receive-ready watermark — a wall-clock
epoch and a monotonic arm time — for every connection generation the session
installs, before that client can subscribe or dispatch receive callbacks.

The wall-clock `rxTime` gate judges each packet against the watermark of its own
captured generation instead of the original startup epoch, with
`startup_backlog_suppress_seconds` (default 5.0; `0` disables) as the cutoff
tolerance. The semantics per timestamp class are explicit:

- A valid `rxTime` older than the current generation's cutoff is suppressed as
  queue backlog, permanently within that generation.
- A valid `rxTime` at or after the cutoff is published immediately, including
  while the monotonic window below is open; a reconnect never spends a blind
  content-independent drain on live packets.
- A missing or malformed `rxTime` cannot prove age. During the short monotonic
  bootstrap window armed on every receive-ready installation after the first,
  such packets are dropped and counted; after lazy expiry of the window they
  relay, because their age cannot be judged. This bounds the unprovable-case
  loss to a few seconds per reconnect episode instead of leaving queue backlog
  a permanent blind spot.
- A future `rxTime` is never suppressed. The local radio is trusted
  infrastructure; a timestamp ahead of the host clock is treated as clock skew,
  not stale backlog. Honest limit: queue backlog on a radio whose clock runs
  ahead of the host by more than the tolerance is indistinguishable from live
  traffic by `rxTime` and relays.
- A radio clock trailing the host by more than the tolerance can have genuinely
  fresh packets suppressed. `startup_backlog_suppress_seconds` doubles as the
  skew tolerance; the same failure class exists at the initial connection.

Repeated reconnects record a fresh watermark and arm a fresh window per
generation; loss stays bounded per episode. Failed connection attempts advance
the generation without producing receive-ready watermarks that any deliverable
packet can reference; the next successful installation re-arms its own
baseline. A pending degraded start installs no client and spends neither the
watermark nor any window. Stop and start reset the watermark history and
evidence counters with the rest of the connect-window state.

The bootstrap window is fixed at five seconds and shares the startup drain's
operator opt-out: `startup_packet_drain_seconds == 0` disables the window as
well as the initial drain, keeping the connect-window drop family on one
switch. No new configuration key is introduced. The initial-connect startup
drain itself is unchanged: armed once before the first receive subscription,
content-independent, and deliberately not re-armed on reconnects.

Each window expiry emits one INFO completion line with the window's drop count;
each in-window drop logs at DEBUG with the suppression log format. Diagnostics
add `receive_ready_generation` (latest watermark generation), and
`reconnect_backlog_packets_dropped` (cumulative unprovable-age drops), and
`reconnect_receive_bootstrap_seconds` (the fixed window length). A late hook
from a retired adapter lifecycle MUST NOT record a watermark or arm a window.

## Native health observations

The adapter MUST report completed native health changes through the runtime's
optional `AdapterContext.report_health` sink when available. Explicit probes and
native notifications MUST use the same readiness calculation. Reports MUST be
bound to the started adapter lifetime; stop and replacement MUST retire their
callbacks. Observer exceptions MUST NOT interrupt native lifecycle or delivery.
Contexts without the optional sink MUST retain explicit-probe compatibility.

Connection loss and owned client replacement MUST update retained health.
Queue transitions into and out of critical/full pressure MUST also update it;
connectivity alone MUST NOT hide critical queue pressure. Native health MUST NOT
imply independent RF reception or change paced submission ownership.

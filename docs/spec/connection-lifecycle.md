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

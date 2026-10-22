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

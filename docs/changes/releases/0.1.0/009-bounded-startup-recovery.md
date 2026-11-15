# Bound offline radio startup and retain recovery

Run native client construction outside the event loop and use sync_timeout_ms
(default 30000) as the initial wait budget and SDK phase timeout. Unavailable
radios start degraded, retain their routes and recover through capped retries
without restarting MEDRE. Keep a timed-out attempt owned instead of opening a
competing connection, and dispose late clients during cancellation or shutdown.

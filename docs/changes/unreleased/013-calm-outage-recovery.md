# Calm reconnect cadence and BLE diagnostics

Default reconnect backoff now spans ten seconds to five minutes. Configured
limits remain supported and jitter cannot shorten the initial interval. Native
BLE warning tracebacks are opt-in through logger overrides; the adapter retains
its own actionable reconnect summaries and SDK ERROR records remain visible.

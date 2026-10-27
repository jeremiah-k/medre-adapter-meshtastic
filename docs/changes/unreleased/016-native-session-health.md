# Publish Meshtastic session and queue health

The adapter reports connection loss and replacement-session readiness through
the optional runtime health sink. Queue pressure changes use the same health
calculation, so draining clears pressure degradation while a disconnected radio
remains degraded. Native generations and adapter lifetimes fence retired
callbacks. Notifications do not add probes, alter pacing or confirm RF delivery.

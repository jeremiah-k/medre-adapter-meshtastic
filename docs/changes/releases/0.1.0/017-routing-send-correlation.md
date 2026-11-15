# Native routing callbacks are classified, never treated as delivery

Inbound `ROUTING_APP` ACK/NAK packets pass through strict source-shaped
normalization and are paired against bounded, connection-generation-fenced
facts recorded for accepted sends. The pairing is classification only:
the supported send path never sets `want_ack`, so no routing result
reachable from this adapter qualifies as remote-service or end-to-end
delivery evidence, and nothing is submitted to core as a delivery
observation. Correlation facts surface under the `send_correlation`
diagnostics key with capacity, retention, and rejection counters.

Retain bounded packet-ID tombstones for the adapter lifetime, including after
resolution and expiry. Refuse new diagnostic correlations when that bound is
full; native sends continue normally. Require the original connection generation
and reject a mismatched native channel without consuming the acceptance.
Structured sends use the same captured client that supplies their provenance.

# Meshtastic reconnect receive watermark

After a transparent reconnect, the Meshtastic radio may empty its FROMRADIO
queue of pre-reconnect packets. Previously those packets were judged against
the original startup baselines: the 15-second startup drain was long expired
and the `rxTime` cutoff still used the first receive-ready epoch, so queued
pre-reconnect traffic could relay as fresh traffic.

The adapter now records a receive-ready watermark per connection generation.
The `rxTime` gate judges each packet against its own generation's watermark,
and a fixed five-second, generation-scoped window drops packets that arrive
without a usable `rxTime` right after a reconnect (they cannot prove they are
not queue backlog). Packets with a usable, provably fresh `rxTime` relay
immediately — a reconnect never spends a blind content-independent drain on
live packets.

Operator impact:

- Queued pre-reconnect backlog no longer relays as fresh.
- Live traffic immediately after a reconnect is unchanged.
- Packets without a usable `rxTime` arriving within five seconds of a
  reconnect are dropped; afterwards they relay as before.
- No new configuration key. The existing `startup_packet_drain_seconds: 0`
  opt-out also disables the reconnect window; `startup_backlog_suppress_seconds`
  remains the `rxTime` cutoff tolerance (and doubles as the radio clock-skew
  tolerance).
- New diagnostics keys: `receive_ready_generation`,
  `reconnect_backlog_packets_dropped`, `reconnect_receive_bootstrap_seconds`.

The released 0.1.0 schema snapshots and their freeze digests are unchanged.

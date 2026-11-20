# Adapter startup info

Core's runtime duck-types an optional `startup_info()` callable on a
built adapter, collecting the returned scalar facts at the adapter's
ready transition. Core owns the timing and layout of the rendered banner.
The full provider contract, redaction policy, cache behavior, and
presentation rules are owned by the core specification;
this page fixes only what this adapter supplies.

## Facts

The provider MUST format facts the adapter already holds and MUST NOT
perform device or radio I/O, acquire resources, or mutate session
state. A synchronous callable is safe on any worker thread: the node
address, the node name, the device release facts, and the LoRa link
settings are atomic snapshots of session state held under the
session's client lock. Refresh and client replacement use that same
lock; a replacement client cannot inherit the previous client's node
address, node name, firmware release, board model, or LoRa settings.

The first row carries the node identity when held: the session's own
node address (`!` plus lowercase hex of the radio's `my_node_num`),
gathered from the connected radio's `myInfo` after client activation,
followed by the local node's name (label `name`) when held — the
LongName and ShortName combine into one fact rendered as
`Long (SHORT)`, either alone when only one is held, sourced from the
client's held node DB entry for the local node (`getMyUser`, i.e.
`nodesByNum[my_node_num]["user"]`, which the radio streams during the
initial config exchange). An entry with neither name held, or a client
without the node DB surface, offers no name fact.

The second row carries the configured endpoint: the connection type
and its endpoint detail — TCP host and port combine into one fact
rendered as `host:port` (bracketed IPv6 when a port is present; host
alone when no port is configured),
serial device, or BLE address — followed by the held device release
facts: the radio's firmware release (label `firmware`) and board
model (label `board`). The third row carries the radio's LoRa link
settings, and is offered only when at least one setting meaningfully
differs from its derived default; the release facts share the endpoint
row while the LoRa settings get a dedicated row.

The LoRa row leads with the link's identity and shape: the modem
preset (label `preset`, only while the radio is actually using a
preset, i.e. `use_preset` is set — with manual bandwidth and spreading
factors the preset is meaningless and omitted), the hop limit (label
`hop limit`), and the frequency slot (label `frequency slot`, the
radio's `channel_num`, only when set — zero means the firmware derives
the channel number from the channel name hash). The override pair
follows: the frequency override (label `frequency override`, rendered
in MHz, only when the radio overrides the region plan) immediately
before the coding rate override (label `coding rate`, rendered as the
`4/N` ratio, for valid manual rates or when the configured rate raises
that of a known preset). Zero, out-of-range values, and equal/lower
preset rates are omitted. Unknown presets cannot establish an override.
Frequency values retain the precision of the held number without
padding whole MHz values. These are configured settings, not independent
proof of RF parameters applied by a particular firmware version. The
region closes the row (label `region`, rendered whenever the radio's
LoRa config is held — an unset region is itself the operationally
relevant fact that the radio cannot transmit). Enum fields render as
their pinned-SDK protobuf enum names (e.g. `US`, `LONG_FAST`), never
as raw integers.

The node name, device release, and LoRa facts are connect-flow state
capture, not new device I/O: the radio delivers its own `NodeInfo`
(the node DB entry carrying the user's LongName/ShortName), its
`DeviceMetadata` (firmware version and hardware model), and its
`LoRaConfig` (copied by the SDK receive pipeline into the local node's
config cache) during the same initial config exchange that delivers
`myInfo`; the client constructor returns only after that exchange
completes, and the session stores the facts atomically with the node
identity under the client lock at every activation. Facts that are
not held MUST be omitted rather than probed: a radio that has not
(yet) delivered its config exchange leaves the name, release, and
LoRa facts absent (including protobuf's default-valued accessor for an
absent `LocalConfig.lora` field), and the adapter never requests them
with a dedicated admin message. The adapter still never gathers channel
counts, so it does not offer them; channel counts on the session are
an unpopulated placeholder and MUST NOT be reported as device facts.

## Decoupling

The provider MUST return plain JSON-like data shaped by the contract.
Adapter source MUST NOT import `medre.core` symbols for startup info;
core validates, redacts, orders, and caps the facts at its render
boundary. No secret-named configuration surfaces in the offered facts:
credentials live in operator configuration and radio identity files,
not in the reported endpoint fields.

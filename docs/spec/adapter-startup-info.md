# Adapter startup info

Core's runtime duck-types an optional `startup_info()` callable on a
built adapter and renders the returned scalar facts at the adapter's
ready transition. The full provider contract, redaction policy, cache
behavior, and presentation rules are owned by the core specification;
this page fixes only what this adapter supplies.

## Facts

The provider MUST format facts the adapter already holds and MUST NOT
perform device or radio I/O, acquire resources, or mutate session
state. A synchronous callable is safe on any worker thread: the node
address and the device release facts are atomic snapshots of session
state held under the session's client lock. Refresh and client
replacement use that same lock; a replacement client cannot inherit the
previous client's node address, firmware release, or board model.

The first row carries the node identity when held: the session's own
node address (`!` plus lowercase hex of the radio's `my_node_num`),
gathered from the connected radio's `myInfo` after client activation.
The second row carries the configured endpoint: the connection type and
its endpoint detail (TCP host and port, serial device, or BLE address),
followed by the held device release facts — the radio's firmware
release (label `firmware`) and board model (label `board`) — after the
endpoint detail. The provider chooses two rows, so the release facts
share the endpoint row.

The device release facts are connect-flow state capture, not new
device I/O: the radio delivers its `DeviceMetadata` (firmware version
and hardware model) during the same initial config exchange that
delivers `myInfo`, and the session stores it atomically with the node
identity under the client lock at every activation. Facts that are not
held MUST be omitted rather than probed: a radio that has not (yet)
delivered its metadata leaves the release facts absent, and the
adapter never requests them with a dedicated admin message. The
adapter still never gathers channel or hop state, so it does not offer
them; channel counts on the session are an unpopulated placeholder and
MUST NOT be reported as device facts.

## Decoupling

The provider MUST return plain JSON-like data shaped by the contract.
Adapter source MUST NOT import `medre.core` symbols for startup info;
core validates, redacts, orders, and caps the facts at its render
boundary. No secret-named configuration surfaces in the offered facts:
credentials live in operator configuration and radio identity files,
not in the reported endpoint fields.

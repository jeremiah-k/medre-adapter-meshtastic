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
address is an atomic snapshot of session state held under the session's
client lock. Refresh and client replacement use that same lock; a
replacement client cannot inherit the previous client's node address.

The first row carries the node identity when held: the session's own
node address (`!` plus lowercase hex of the radio's `my_node_num`),
gathered from the connected radio's `myInfo` after client activation.
The second row carries the configured endpoint: the connection type and
its endpoint detail (TCP host and port, serial device, or BLE address).

Facts that are not held MUST be omitted rather than probed. The adapter
never gathers board model, firmware version, channel, or hop state, so
it does not offer them; channel counts on the session are an unpopulated
placeholder and MUST NOT be reported as device facts.

## Decoupling

The provider MUST return plain JSON-like data shaped by the contract.
Adapter source MUST NOT import `medre.core` symbols for startup info;
core validates, redacts, orders, and caps the facts at its render
boundary. No secret-named configuration surfaces in the offered facts:
credentials live in operator configuration and radio identity files,
not in the reported endpoint fields.

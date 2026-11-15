# Declare Meshtastic device-attached state for stopped snapshots

Register a provider-owned, SDK-free stopped-state declaration through the
`AdapterSpec.stopped_state_declaration` reference. Registration uses the core's
native setup and stopped-state contracts directly; it does not silently omit
metadata when a core installation lacks those contracts.

The declaration selects **zero host files**: Meshtastic radio identity and
firmware configuration reside on the radio, not in a locally capturable
adapter ledger. It reports `device_attached` coverage (or `stateless_local`
for fake connectivity) and **unsupported local exclusion**; it does not
acquire or reuse a native ownership primitive. A host-file snapshot neither
captures radio state nor proves that the selected radio is exclusively owned.
Operators must verify the configured TCP endpoint or the selected serial/BLE
device and its ownership independently. Ordinary power cycling generally
preserves stored radio settings; factory reset or destructive reflash may not.

The stopped-state consumer and its trust boundary live in
`medre.maintenance.stopped_declare` in core.

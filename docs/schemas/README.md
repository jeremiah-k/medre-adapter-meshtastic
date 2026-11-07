# Meshtastic native contract freeze

`v0.1.0.freeze.json` records the pre-first-tag 0.1.0 schema baseline.
The compatibility tests reject unreviewed schema additions and edits; changes
after the first release require an explicit compatibility/version decision.
The schema is owned by the Meshtastic adapter, not MMRelay or MEDRE core.
This release guard does not require MMRelay to install the Meshtastic adapter.

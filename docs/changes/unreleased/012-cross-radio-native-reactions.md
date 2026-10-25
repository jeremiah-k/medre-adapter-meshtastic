# Preserve native reactions between Meshtastic instances

Meshtastic reactions now retain native tapback fields when crossing configured
radio instances, including asymmetric channel maps. Destination-native packet
IDs come from durable delivery feedback and relation enrichment. Source-radio
numeric hints are not reused when destination mapping is absent. Other-platform
reactions retain descriptive MMRelay formatting.

Reactions rendered before destination handoff keep readable fallback even when
the mapping arrives while they are queued; later reactions can use that mapping.

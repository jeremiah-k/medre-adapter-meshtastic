# Remove the retired mmrelay compatibility config surface

The Meshtastic adapter no longer defines the `mmrelay_compatibility` config
field. It had no readers in this adapter: the legacy flat `meshtastic_*`
Matrix-event embedding it described was retired with the Matrix adapter's
interop surface. Deployments must drop the `mmrelay_compatibility` key from
Meshtastic adapter configuration; constructing `MeshtasticConfig` with that
key now fails.

Behavior is unchanged: canonical attribution prefixes, cross-platform
descriptive reactions, native tapback replies, the codec's relation
metadata, and the trust boundaries that keep foreign MMRelay participants
ordinary and untrusted all stay as they were.

# Meshtastic relay attribution

An omitted or null `radio_relay_prefix` selects automatic attribution. Matrix
sources use the first five Unicode code points of the full display name followed
by `"[M]: "`, matching MMRelay's `display5` default. Missing display names fall back
to the source sender identity. Other platforms use the generic short sender
followed by `": "` and MUST NOT receive a Matrix marker.

A configured string MUST take precedence; `radio_relay_prefix: ""` MUST disable
the prefix. Custom `{sender_short}` templates retain their existing account-name
semantics. Prefix presentation MUST NOT rewrite the canonical sender metadata.

The entire composed prefix, reply/reaction text and message MUST fit the target's
`max_text_bytes` budget (227 by default). Truncation MUST preserve valid UTF-8;
it MUST NOT split an encoded code point. Matrix display-name abbreviation uses
characters; final wire truncation uses bytes. Native tapbacks remain native
without an added visible text prefix.


## Reactions across radio instances

Reactions sourced from any configured Meshtastic instance MUST retain native
Meshtastic tapback semantics when a numeric destination-native target is known.
The source and destination adapter IDs need not match. Transport classification
MUST use the provider-owned instance configuration, not an adapter-name prefix
or wire metadata.

Across Meshtastic instances, replies and reactions MUST use the destination's
native target mapping. A source codec's meshtastic_reply_id hint MUST NOT be
reused as an unmapped destination packet ID. Missing destination mappings MUST
fall back to readable text without native relation fields. Explicit fallback
strategies MUST NOT emit native tapback fields. Reactions from other transports
retain MMRelay-compatible descriptive rendering.

The mapping is sampled when the reaction is rendered. A reaction queued before
native handoff establishes the original's destination mapping MUST retain its
readable fallback even if that mapping appears before the queued send executes.
It MUST NOT be automatically upgraded or submitted a second time. A subsequent
reaction MAY use a destination mapping established by that later time. Waiting
for mappings or upgrading a completed fallback requires a separate durable
relation-delivery policy; it is not an implicit renderer retry.

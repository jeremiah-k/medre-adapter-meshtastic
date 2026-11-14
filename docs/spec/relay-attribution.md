# Meshtastic relay attribution

An omitted or null `relay_prefix` selects automatic attribution. Matrix
sources use the automatic template `{sender5}[M]: ` — the first five
Unicode code points of the full display name followed by `"[M]: "`.
Missing display names fall back to the source sender identity. Other
platforms use the generic short sender followed by `": "` and MUST NOT
receive a Matrix marker.

A configured string MUST take precedence; `relay_prefix: ""` MUST disable
the prefix. Custom `{sender_short}` templates retain their existing account-name
semantics. Prefix presentation MUST NOT rewrite the canonical sender metadata.

Radio displays are the constrained-display affordance of this transport:
the label variables `sender`, `sender_short`, and `origin_label` accept
the shared `{nameN}` truncation suffix, rendering the first N Unicode
code points of the variable's value. The suffix counts code points,
never bytes, and N MUST be at least 1 (`{sender0}` is not a truncation
variable). A numeric-suffixed name whose stem is not declared
truncatable for this transport passes through unchanged — roomy
transports declare no truncatable names. The shared relay-prefix
formatter specification defines the full contract.

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
retain descriptive rendering.

A descriptive cross-transport reaction body MUST quote a bounded, single-line
preview of the reacted-to text: whitespace-flattened, at most 40 characters
with a trailing ellipsis when longer, sourced from canonical relation
enrichment (`original_text`, then `fallback_text`). When the reacted-to text
is genuinely unresolvable, the body MUST say `an earlier message` instead of
quoting an empty string; the literal shape `to ""` MUST NOT reach the wire.
The complete described body, including the attribution prefix and the
exact reaction key, remains subject to the target's `max_text_bytes` budget.

The mapping is sampled when the reaction is rendered. A reaction queued before
native handoff establishes the original's destination mapping MUST retain its
readable fallback even if that mapping appears before the queued send executes.
It MUST NOT be automatically upgraded or submitted a second time. A subsequent
reaction MAY use a destination mapping established by that later time. Waiting
for mappings or upgrading a completed fallback requires a separate durable
relation-delivery policy; it is not an implicit renderer retry.

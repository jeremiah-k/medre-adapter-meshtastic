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

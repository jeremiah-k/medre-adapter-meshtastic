# Degrade the described reaction preview to honest wording

Described cross-transport reactions delivered to Meshtastic quote a bounded,
single-line preview of the reacted-to text. When the target text is genuinely
unresolvable — the source reaction's native ref does not resolve to a stored
canonical event, or the destination's quote read is denied so relation
enrichment only sees the metadata-only envelope — the body now says
`an earlier message` instead of quoting an empty string. The literal shape
`to ""` no longer reaches the wire in any delivery strategy.

Preview population was already bounded (40 characters, whitespace-flattened)
and resolvable-target reactions are unchanged; only the unresolvable-target
composition changes. The complete described body keeps passing the target's
`max_text_bytes` truncation machinery, and mesh-origin native tapback
ingress is untouched.

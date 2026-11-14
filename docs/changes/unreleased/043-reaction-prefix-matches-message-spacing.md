# Described reactions reuse the ordinary-message relay prefix

Described cross-transport reactions delivered to Meshtastic (for example,
Matrix → Meshtastic) previously stripped spaces from the sender label
before `{nameN}` truncation, so the same Matrix sender rendered
`BobS[M]: reacted 👍 to …` on the reaction path but `Bob S[M]: hello` on
ordinary messages. The reaction prefix now composes through the same
builder as the message prefix with no whitespace handling, matching the
MMRelay reference, which calls its shared prefix builder for both paths.

Separator-space insertion between the prefix and `reacted` for custom
templates that do not end in whitespace is unchanged, mesh-origin native
tapbacks are unchanged, and the described body's bounded single-line
preview rules are unchanged.

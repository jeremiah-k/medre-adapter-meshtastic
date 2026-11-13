# Unified relay-prefix template surface

The Meshtastic adapter's relay-prefix configuration is the shared
`relay_prefix` key, and the automatic Matrix attribution is expressed as
the `{sender5}[M]: ` template through the shared relay-prefix formatter.

Radio displays are this transport's constrained-display affordance: the
label variables `sender`, `sender_short`, and `origin_label` accept
`{nameN}` truncation suffixes in custom templates, rendering the first N
Unicode code points (N at least 1). `{sender3}` now works in a configured
`relay_prefix`. A numeric suffix on an undeclared variable passes through
unchanged, per the shared formatter contract. Automatic attribution
renders byte-identically to the previous built-in output.

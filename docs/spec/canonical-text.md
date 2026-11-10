# Canonical message text

Text rendering MUST select source text through core's `select_message_body`.
A payload with `formatting` and a string `body` uses that body; otherwise a
legacy `text` alias takes precedence over `body`. A malformed explicit body or
selected value MUST fail before native submission. Invalid optional formatting
MUST NOT decorate a different compatibility alias. Relation fallbacks follow
selection; prefixes, native identity, budgets and delivery evidence remain
adapter-owned. This change requires the candidate core text-selection revision.

Candidate qualification MUST use an explicit immutable core revision. Ordinary
CI MUST retain core main as its baseline; dependency merges precede consumers.

Native queue draining MUST reject malformed retained payloads or text as
permanent failures before calling the session. Such records MUST NOT consume
transient retry attempts. Reaction keys MUST NOT bypass canonical body validation;
native reply and tapback authority still follows the proven destination mapping.

## Explicit reaction keys

Structured reactions MUST carry an explicit nonempty text key. Native tapback
text supplies that key exactly; malformed or empty tapbacks MUST be dropped
and MUST NOT decode into a fabricated symbol. Renderers MUST preserve the
relation key and MUST NOT infer it from payload text or emoji aliases.

Symbolic public portnums MUST resolve through the installed SDK's protocol
enum when available, with the static fallback used when the SDK is absent.
The public fact MUST retain the same numeric value as a numeric packet callback.

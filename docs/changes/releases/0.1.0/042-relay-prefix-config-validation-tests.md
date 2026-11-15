# Relay-prefix config validation test coverage

The `MeshtasticConfig.validate()` relay-prefix branch now has explicit
config-validation coverage: bool (true and false), int, and float
`relay_prefix` values are pinned to raise `MeshtasticConfigError` with
the exact "must be a str" message shape, while `None` (automatic
attribution), the empty string (disabled), and a custom template are
pinned as accepted and preserved. Test-only change; no runtime behavior
or schema changes.

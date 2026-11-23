# medre-adapter-meshtastic

The Meshtastic adapter for MEDRE.

## Install

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) to install
MEDRE and this adapter in the same persistent tool environment:

```bash
uv tool install medre
medre adapters install meshtastic
```

On Windows, the package-changing command prints a uv command to run **after**
`medre.exe` exits, rather than replacing an executable still in use.
Adapter installation defaults to package-index requirements; existing saved
Git requests and catalogs are preserved. For fake smoke, guided route
creation, locked uv deployment projects, private Git sources, local checkouts,
and the same-environment requirement, see the
[MEDRE Getting Started guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/getting-started.md)
and the
[adapter-source guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/adapter-installation.md).
Neither MEDRE nor the adapter installs packages during runtime startup.

## Configure

Save a private `connections.yaml` with one real TCP instance. The common case
needs the connection type and the node host; the default TCP port is shown in
the setup guide's options:

```yaml
adapters:
  meshtastic:
    radio-a:
      adapter_kind: real
      connection_type: tcp
      host: meshtastic.local
routes: {}
```

Add another adapter instance (a second radio, or a different transport), then
build routes with guided configuration. Environment overrides use
`MEDRE_ADAPTER__<TOKEN>__<FIELD>`, for example
`MEDRE_ADAPTER__RADIO_A__SERIAL_PORT` for a serial node. For every option,
extra instances, and troubleshooting, see the
[Meshtastic setup guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/meshtastic.md).

## Verify

Confirm the installed adapter and the saved configuration resolve:

```bash
medre adapters check --config connections.yaml
```

The setup guide's
[Verify](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/meshtastic.md#verify)
section shows the config check, a healthy start, health states, and delivery
semantics.

## Outbound pacing

The default message_delay_seconds is 2.2 seconds, leaving a margin above
current firmware's two-second phone-originated text limit. Shorter configured
intervals can cause RATE_LIMIT_EXCEEDED even when the SDK returns a packet ID.
Use a lower interval only with separately qualified firmware. A local transport
handoff is not proof of RF reception; verify delivery on an independent node.
The firmware rule is implemented in
[PhoneAPI.cpp](https://github.com/meshtastic/firmware/blob/master/src/mesh/PhoneAPI.cpp).

## Default relay attribution

Matrix messages sent to Meshtastic default to `Alice[M]: message`, using the
first five display-name characters. The prefix shares the 227-byte UTF-8 budget
with the body. Other source platforms retain generic short-name attribution.
Set `relay_prefix: ""` to disable it or supply a custom generic template;
omitted/null selects the source-aware default.

Templates use generic variables such as `{sender}` and `{sender_short}`.
Because radio displays are constrained, the label variables `sender`,
`sender_short`, and `origin_label` also accept truncation suffixes:
`{sender3}` renders the first three Unicode code points of the sender
label (N must be at least 1). A numeric suffix on any other variable
passes through unchanged.

## Offline startup and recovery

An offline radio no longer holds up the runtime for the SDK's multi-minute
connection defaults. The existing sync_timeout_ms setting defaults to 30000:
MEDRE waits up to 30 seconds for that instance's initial connection, then starts
it degraded and proceeds with the remaining adapters. Each offline instance has
its own budget; startup still visits adapter instances in order.

Routes involving a started, degraded radio remain available. Recovery retains
one connection attempt at a time and retries with capped backoff until the radio
returns or MEDRE stops. Use `medre diagnostics --refresh-health` to inspect the
live connection state. A connection returning after shutdown is closed.

Set sync_timeout_ms on the Meshtastic instance to change the initial wait and
native SDK phase limits. It must be a positive integer in milliseconds. Invalid
configuration and missing dependencies still fail startup. The async deadline
does not forcibly terminate a blocking SDK thread; see the
[connection lifecycle contract](docs/spec/connection-lifecycle.md) for ownership
and shutdown limits. Native handoff still requires independent RF qualification.

## Development

See [contributing](CONTRIBUTING.md) for the shared Trunk quality gate and
uv-based editable development workflow. Maintainer material — packaging and
provenance, ownership and imports, offline samples and diagnostics, tests,
fake smoke sources, coordinated CI, and the adapter contract — lives in
[adapter maintenance](docs/dev/adapter-maintenance.md).

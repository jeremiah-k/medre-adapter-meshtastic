# Meshtastic native contract freeze

Git attributes disable line-ending translation for this schema tree so frozen SHA-256
digests and installed wheel resources remain identical on Windows and Unix.

Source distributions include this directory. Wheels include the same schema
bundle, freeze manifest, and examples under `medre/adapters/meshtastic/_schemas`.
Installed consumers can read it with
`importlib.resources.files("medre.adapters.meshtastic").joinpath("_schemas")`.

The schema's `$id` uses this repository's `raw.githubusercontent.com` namespace,
matching core's identifier ownership policy. It replaces the earlier `medre.dev`
identifier before the first tag; editor mappings should use the included
schema's `$id`. The identifier does not require network retrieval.

`v0.1.0.freeze.json` records the pre-first-tag 0.1.0 schema baseline.
The compatibility tests reject unreviewed schema additions and edits; changes
after the first release require an explicit compatibility/version decision.
The schema is owned by the Meshtastic adapter, not MMRelay or MEDRE core.
This release guard does not require MMRelay to install the Meshtastic adapter.

A producer can explicitly publish the already-observed source portnum under
`meshtastic.public_facts.portnum`. MEDRE's transport-neutral bridge context
copies only this bounded, opted-in projection, not raw packets or keys. The
`MeshtasticSourceObservation` schema variant describes an embedding consumer
(such as MMRelay) that has native source identity, display labels and public
facts but does not instantiate the Meshtastic adapter or claim to have decoded
a full SDK packet. It is distinct from the complete native packet shape.

## Public portnum facts

The source-owned `public_facts.portnum` MUST carry the numeric Meshtastic
protocol port number when known. Internal classifier labels MUST NOT become
public portnum values. Unknown or malformed values MUST NOT be guessed.

## Git checkout line endings

A main-to-candidate Git checkout may retain unchanged CRLF files. Packaging
MUST emit the exact frozen bytes. LF reconstruction is permitted only when
its digest matches the freeze manifest; every other byte change MUST fail
the build. Fresh checkouts retain the explicit `-text` attribute policy.

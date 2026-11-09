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

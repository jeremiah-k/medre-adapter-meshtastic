# Share rendering policy

Meshtastic now uses shared relation descriptions, exact canonical reaction keys and destination-scoped target bindings. Descriptive reaction prefixes preserve the same sender-label spacing as messages. Unsupported prefix placeholders fail validation, and native reaction keys that exceed the packet budget are rejected.

Only reaction-only events can become native key-only tapbacks. Ordinary messages carrying reaction context retain authored text, and relation-free presence, edit and delete events keep their kind-specific descriptions.

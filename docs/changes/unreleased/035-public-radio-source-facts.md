# Explicit public source facts for relay contexts

The Meshtastic codec now selects portnum for a bounded public source-fact
projection rather than leaking the decoded packet. The frozen native contract
also includes a minimal source-facts observation for consumers that own their
own radio session, such as MMRelay. This snapshot is not a packet
classification or proof of sender authenticity.

Public portnum facts use the protocol integer, independently of internal
classification labels such as `text_message` or `detection_sensor`.

Resolve SDK-declared symbolic portnums as well as the static fallback names so
explicitly promoted text retains its public protocol number.

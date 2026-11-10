# Decode explicitly promoted text portnums

Decode non-chat packets selected for relay by explicit promotion policy when
they carry nonempty text. Preserve the source portnum in native metadata and
public facts, including detection-sensor port 10 with both configured opt-ins.
Deferred or dropped non-chat packets do not gain text decode eligibility.

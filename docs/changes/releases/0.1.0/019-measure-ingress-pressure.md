# Measure Meshtastic SDK callback scheduling pressure

Add deterministic reader-thread callback burst tests at 32 and 256 pending
publications and a shutdown probe that observes publisher cancellation and
completion independently of future bookkeeping. Optional sanitized JSON
counters provide repeatable high-water data. No runtime ingestion policy,
configuration, SDK contract, durable send authority or queue bound changes.
The measurements do not establish hardware throughput or a safe lossy cap.

# Qualify sustained staging at and above the bounded cap

The Meshtastic adapter's bounded reader-thread ingress staging now carries
its measured qualification record. New sustained-ingress tests
(``tests/test_meshtastic_sustained_ingress.py``) pin, against the real
adapter in fake mode with the default ``max_pending_ingress_callbacks`` of
``100``: above-cap bursts at 1x/4x/16x stage exactly the cap and refuse
exactly N−100 with reconciled producer-pressure reports and no publication
for refused packets; repeated bursts in the same session reuse full capacity
without slot leaks; a 2,400-callback produce/release imbalance keeps exact
invariants at every cycle boundary (submitted == staged + refused,
staged ≤ cap, staged == published + in-flight); stop under saturation
cancels all staged work, reports only pre-start cancellations (one coalesced
``cancelled_before_admission`` count for deterministically queued-unstarted
submissions), and leaves the retired pool fully released; a restarted
session immediately stages an above-cap burst under the identical contract.

Content-free memory and reader-latency evidence extends the
``MEDRE_PRESSURE_REPORT_DIR`` record: ``tracemalloc`` peak grows 1.21x for a
16x burst (pending work tracks the cap, not the burst; the sublinear bound
is asserted in CI), and the reader-thread reservation acquire stays
non-blocking with microsecond-scale observed maxima. This is a loss-honesty
and boundedness record, not a throughput guarantee; real-SDK, hardware, and
radio claims remain in their named gated tiers. The shipped default stays
provisional and finite-only.

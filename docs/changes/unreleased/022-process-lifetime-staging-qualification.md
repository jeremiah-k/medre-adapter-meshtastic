# Qualify staging across process-lifetime restart cycles

Bounded reader-thread ingress staging now carries a process-lifetime
qualification record alongside the single-session one. New restart-cycle tests
(``tests/test_meshtastic_process_lifetime_ingress.py``) run warm-up and
twenty-five measured restart cycles in one process against one adapter
instance in fake mode: every cycle drives an above-cap burst with publication
held, so each stop retires a staging pool stranded at full capacity.

Each cycle pins exact accounting: the replacement session starts from exactly
full capacity, stages exactly the cap, and refuses exactly the surplus with
per-cycle producer-pressure totals reconciled through a fresh sink; every stop
returns pending futures, admission trackers, and background tasks to exact
zero by count and leaves the retired pool behaviorally full; late one-shot
releasers fired after the replacement session starts release into the retired
pool only; refusal accounting is frozen between sessions, and every retired
pool across the whole run still holds exactly its full capacity. A
whole-process ``tracemalloc`` guard keeps retained bytes under both a doubling
of the post-warm-up baseline and a fixed extra budget for the entire run, with
exact counters carrying the primary evidence. The bounded staging
specification gains the matching process-lifetime qualification section.

This is a boundedness and loss-honesty record on the synthetic SDK tier:
synthetic frames run through the real classifier and codec. It makes no
real-SDK, hardware, or throughput claim, and no capacity budget beyond the
staging bound itself.

# Deliver complete Meshtastic text through the paced queue

`max_text_parts` permits 2–128 numbered packets for long ordinary messages;
the default remains 1. The shared planner retains authored text, attribution
and Unicode within every packet budget. Exceeding the cap rejects the whole plan.

Planned packets wait for native handoff through the existing queue while core
retains its current claim. Cancellation, stopped/replaced clients, missing packet
IDs and ambiguous native calls stop remaining parts without replaying consumed
grants. Individual packets emit no deferred parent completion. Ordinary messages
retain their existing deferred behavior and native retry policy.

Queue consumers now serialize through one owner and pace from completion,
preventing slow storage/SDK work from consuming the next transmission's gap.
Partially accepted messages remain held after restart; independent recipient ACK
aggregation and physical qualification remain separate. Package version stays
0.1.0 and no canonical storage shape changes are introduced.

Queued mapping handoffs retain an explicit native `packet_id` of zero even
when a second `id` alias is present; the secondary ID cannot replace the
packet's authoritative identity.

An SDK result conversion failure settles its guarded waiter and clears only
that completed invocation's cancellation capture, preserving unrelated queued
work and avoiding false cancellation feedback during shutdown.

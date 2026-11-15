"""Evidence-only helpers for the live pair ingress case."""

from __future__ import annotations

import asyncio
import re
import time


def submitted_batch(records: list[dict], texts: list[str]) -> tuple[list[int], str]:
    """Separate native submissions from unsolicited received packets."""
    assert texts, "peer batch must contain at least one submission"
    sent = [item for item in records if "sent_id" in item or "sent_text" in item]
    assert [item.get("sent_text") for item in sent] == texts, (
        "peer submission count/order/text differs from requested batch"
    )
    ids: list[int] = []
    for item in sent:
        pid = item.get("sent_id")
        assert type(pid) is int and 0 < pid <= 0xFFFFFFFF, "peer send not accepted"
        ids.append(pid)
    assert len(set(ids)) == len(ids), "peer reused a native packet id"
    senders = {item.get("sender_id") for item in sent}
    assert len(senders) == 1, "peer did not report one native sender id"
    sender = next(iter(senders))
    assert isinstance(sender, str) and re.fullmatch(r"![0-9a-f]{8}", sender), (
        "peer did not report a valid native sender id"
    )
    return ids, sender


async def assert_admitted_batch(
    storage,
    texts: list[str],
    packet_ids: list[int],
    sender: str,
    *,
    timeout: float = 60,
) -> None:
    """Correlate every packet to distinct durable identity and ingress-owned body.

    The caller owns a fresh REPLAY-scoped runtime. Do not use this helper
    for LIVE routed egress: that worker owns content and may release it.
    """
    assert len(texts) == len(packet_ids) and len(set(packet_ids)) == len(packet_ids)
    refs = {}
    deadline = time.monotonic() + timeout
    while True:
        for pid in packet_ids:
            if pid not in refs:
                ref = await storage.resolve_native_ref("mt_radio", "0", str(pid))
                if ref:
                    refs[pid] = ref
        if len(refs) == len(packet_ids):
            break
        remaining = deadline - time.monotonic()
        assert remaining > 0, (
            f"native packets not durably admitted: {set(packet_ids) - refs.keys()}"
        )
        await asyncio.sleep(min(0.25, remaining))
    assert len(set(refs.values())) == len(packet_ids), "distinct packets deduplicated"
    for text, pid in zip(texts, packet_ids, strict=True):
        ref = refs[pid]
        identity = await storage.get(ref)
        assert identity is not None and identity.event_id == ref
        assert identity.source_adapter == "mt_radio"
        assert identity.source_channel_id == "0"
        assert identity.source_transport_id == sender
        assert identity.payload == {}, "identity projection exposed content"
        read = await storage.read_event_for_ingress(ref)
        assert read.status == "retained" and read.event is not None, (
            f"authorized ingress read denied for packet {pid}"
        )
        assert read.event.event_id == ref
        assert read.event.source_transport_id == sender
        assert read.event.payload.get("body") == text, f"body mismatch for packet {pid}"

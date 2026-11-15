"""No hardware is needed to test the batch evidence oracle."""

from types import SimpleNamespace

import pytest
from medre_meshtastic_tests.helpers.pair_ingress import (
    assert_admitted_batch,
    submitted_batch,
)

_SENDER = "!12345678"
_TEXTS = ["unicode ✓", "line\nb", "same", "same"]
_IDS = [1, 2, 3, 4]


def _sent():
    return [
        {"sent_id": pid, "sent_text": text, "sender_id": _SENDER}
        for pid, text in zip(_IDS, _TEXTS, strict=True)
    ]


def test_batch_ignores_receives_but_keeps_submission_order():
    rows = _sent()
    rows.insert(2, {"id": 50, "text": "ambient packet"})
    assert submitted_batch(rows, _TEXTS) == (_IDS, _SENDER)


@pytest.mark.parametrize(
    "failure",
    ["missing", "extra", "duplicate", "wrong-text", "sender", "zero", "boolean"],
)
def test_batch_rejects_incomplete_or_ambiguous_submissions(failure):
    rows = _sent()
    if failure == "missing":
        rows.pop()
    elif failure == "extra":
        rows.append(rows[-1].copy())
    elif failure == "duplicate":
        rows[-1]["sent_id"] = rows[-2]["sent_id"]
    elif failure == "wrong-text":
        rows[-1]["sent_text"] = "different"
    elif failure == "sender":
        rows[-1]["sender_id"] = None
    elif failure == "zero":
        rows[-1]["sent_id"] = 0
    else:
        rows[-1]["sent_id"] = True
    with pytest.raises(AssertionError):
        submitted_batch(rows, _TEXTS)


class Storage:
    def __init__(self, failure=None):
        self.failure = failure
        self.reads = []

    async def resolve_native_ref(self, adapter, channel, pid):
        assert (adapter, channel) == ("mt_radio", "0")
        if pid == "4":
            if self.failure == "missing":
                return None
            if self.failure == "dedup":
                return "3"
        return pid

    async def get(self, ref):
        return SimpleNamespace(
            event_id=ref,
            source_adapter="mt_radio",
            source_channel_id="0",
            source_transport_id=_SENDER,
            payload={},
        )

    async def read_event_for_ingress(self, ref):
        self.reads.append(ref)
        if ref == "4" and self.failure == "denied":
            return SimpleNamespace(status="denied", event=None)
        text = _TEXTS[int(ref) - 1]
        if ref == "4" and self.failure == "body":
            text = "wrong body"
        return SimpleNamespace(
            status="retained",
            event=SimpleNamespace(
                event_id=ref, source_transport_id=_SENDER, payload={"body": text}
            ),
        )


async def test_every_packet_uses_authorized_body_read():
    storage = Storage()
    await assert_admitted_batch(storage, _TEXTS, _IDS, _SENDER, timeout=0)
    assert storage.reads == ["1", "2", "3", "4"]


@pytest.mark.parametrize("failure", ["missing", "dedup", "denied", "body"])
async def test_batch_rejects_missing_admission_dedup_and_denied_or_wrong_body(failure):
    with pytest.raises(AssertionError):
        await assert_admitted_batch(Storage(failure), _TEXTS, _IDS, _SENDER, timeout=0)

"""Queued work and unrelated destinations cannot prove native handoff."""

from types import SimpleNamespace

import pytest
from medre_meshtastic_tests.helpers.pair_receipts import await_sent_receipt


def receipt(status, sequence=1, *, target="mt_radio", route="lab_egress"):
    return SimpleNamespace(
        status=status, sequence=sequence, target_adapter=target, route_id=route
    )


class Storage:
    def __init__(self, batches):
        self.batches = iter(batches)
        self.calls = 0

    async def list_receipts_for_event(self, event_id):
        assert event_id == "event"
        self.calls += 1
        return next(self.batches)


async def wait(storage, timeout=1):
    return await await_sent_receipt(
        storage,
        "event",
        target_adapter="mt_radio",
        route_id="lab_egress",
        timeout=timeout,
        interval=0,
    )


async def test_refreshes_queued_receipt_after_native_completion():
    queued = receipt("queued")
    sent = receipt("sent", 2)
    storage = Storage([[queued], [queued, sent]])
    assert await wait(storage) is sent
    assert storage.calls == 2


async def test_latest_selected_destination_must_be_sent():
    old = receipt("sent")
    queued = receipt("queued", 2)
    sent = receipt("sent", 3)
    storage = Storage([[old, queued], [old, queued, sent]])
    assert await wait(storage) is sent
    assert storage.calls == 2


@pytest.mark.parametrize(
    "records",
    [
        [],
        [receipt("queued")],
        [receipt("sent", target="other")],
        [receipt("sent", route="other")],
    ],
)
async def test_missing_or_wrong_handoff_evidence_fails(records):
    with pytest.raises(AssertionError, match="no sent receipt"):
        await wait(Storage([records]), timeout=0)

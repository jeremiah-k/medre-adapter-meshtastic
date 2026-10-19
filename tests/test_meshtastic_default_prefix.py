"""Compact default attribution and final wire-byte budgeting."""

from datetime import datetime, timezone
from dataclasses import replace
import pytest
from medre.core.events import CanonicalEvent, EventMetadata, NativeMetadata
from medre.core.rendering.renderer import RenderingContext
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig


def message(native, sender="!9f098838", text="Testing from medre"):
    return CanonicalEvent(
        event_id="prefix-proof",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(timezone.utc),
        source_adapter="source",
        source_transport_id=sender,
        source_channel_id="3",
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"body": text},
        metadata=EventMetadata(native=NativeMetadata(data=native)),
    )


@pytest.fixture
def renderer(monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_sender_label": "Tad Chilly",
            "source_sender_short_label": "tadchilly",
            "source_sender_id": "@tadchilly:example.org",
            "source_platform": "matrix",
        },
    )
    return MeshtasticRenderer(configs={"target": MeshtasticConfig("target")})


async def render(renderer, body="Testing from Matrix"):
    return await renderer.render(
        message({}, sender="@tadchilly:example.org", text=body),
        RenderingContext(
            target_adapter="target",
            target_platform="meshtastic",
            target_channel="3",
            delivery_strategy="direct",
        ),
    )


async def test_default_uses_five_display_characters_and_matrix_marker(renderer):
    result = await render(renderer)
    assert result.payload["text"] == "Tad C[M]: Testing from Matrix"
    assert result.metadata["relay_prefix_rendered"] == "Tad C[M]: "
    assert result.payload["channel_index"] == 3


async def test_default_prefix_counts_against_unicode_wire_budget(renderer):
    renderer._configs["target"] = replace(
        renderer._configs["target"], max_text_bytes=30
    )
    result = await render(renderer, "🌍" * 30)
    assert result.payload["text"] == "Tad C[M]: " + "🌍" * 5
    assert len(result.payload["text"].encode("utf-8")) <= 30
    assert result.truncated


@pytest.mark.parametrize(
    "template,expected",
    [
        ("", "Testing from Matrix"),
        ("{sender_short}: ", "tadchilly: Testing from Matrix"),
    ],
)
async def test_explicit_prefix_and_disable_override_automatic(
    renderer, template, expected
):
    renderer._configs["target"] = replace(
        renderer._configs["target"], radio_relay_prefix=template
    )
    assert (await render(renderer)).payload["text"] == expected


async def test_other_platforms_do_not_get_a_matrix_marker(renderer, monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_platform": "xmpp",
            "source_sender_short_label": "Alice",
        },
    )
    assert (await render(renderer)).payload["text"] == "Alice: Testing from Matrix"

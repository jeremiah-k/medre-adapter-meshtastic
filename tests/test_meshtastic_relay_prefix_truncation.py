"""Truncation suffixes on the unified ``relay_prefix`` template surface.

The automatic Matrix attribution is the shared ``{sender5}[M]: `` template,
and custom ``relay_prefix`` templates accept ``{nameN}`` first-N-code-point
truncation for the label variables declared by this transport. Radio
displays are the constrained-display affordance that motivates truncation.
"""

from datetime import datetime, timezone

import pytest
from medre.adapters.meshtastic.renderer import MeshtasticRenderer
from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.core.events import CanonicalEvent, EventMetadata, NativeMetadata
from medre.core.rendering.renderer import RenderingContext


def message(sender="!9f098838", text="Testing from Matrix"):
    return CanonicalEvent(
        event_id="prefix-truncation",
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
        metadata=EventMetadata(native=NativeMetadata(data={})),
    )


@pytest.fixture
def renderer(monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_sender_label": "Bob Smith",
            "source_sender_short_label": "bobsmith",
            "source_sender_id": "@bobsmith:example.org",
            "source_platform": "matrix",
        },
    )
    return MeshtasticRenderer(configs={"target": MeshtasticConfig(adapter_id="target")})


async def render(renderer, relay_prefix=None, body="Testing from Matrix"):
    renderer._configs["target"] = MeshtasticConfig(
        adapter_id="target", relay_prefix=relay_prefix
    )
    return await renderer.render(
        message(sender="@bobsmith:example.org", text=body),
        RenderingContext(
            target_adapter="target",
            target_platform="meshtastic",
            target_channel="3",
            delivery_strategy="direct",
        ),
    )


# ---------------------------------------------------------------------------
# Automatic default: byte-identical template rendering
# ---------------------------------------------------------------------------


async def test_automatic_matrix_default_is_the_truncation_template(renderer):
    result = await render(renderer)
    assert result.payload["text"] == "Bob S[M]: Testing from Matrix"
    assert result.metadata["relay_prefix_rendered"] == "Bob S[M]: "
    assert result.metadata["relay_prefix_template"] == "{sender5}[M]: "


async def test_automatic_matrix_default_with_short_name(renderer, monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_sender_label": "Ana",
            "source_sender_short_label": "ana",
            "source_sender_id": "@ana:example.org",
            "source_platform": "matrix",
        },
    )
    result = await render(renderer)
    assert result.payload["text"] == "Ana[M]: Testing from Matrix"


async def test_automatic_non_matrix_default_keeps_short_sender(renderer, monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_platform": "xmpp",
            "source_sender_short_label": "Alice",
        },
    )
    result = await render(renderer)
    assert result.payload["text"] == "Alice: Testing from Matrix"


async def test_none_selects_automatic_and_empty_string_disables(renderer):
    automatic = await render(renderer, relay_prefix=None)
    assert automatic.payload["text"] == "Bob S[M]: Testing from Matrix"
    disabled = await render(renderer, relay_prefix="")
    assert disabled.payload["text"] == "Testing from Matrix"
    assert "relay_prefix_rendered" not in disabled.metadata


# ---------------------------------------------------------------------------
# Custom templates: declared truncation suffixes
# ---------------------------------------------------------------------------


async def test_custom_template_truncates_sender_by_code_points(renderer):
    result = await render(renderer, relay_prefix="{sender3}[M]: ")
    assert result.payload["text"] == "Bob[M]: Testing from Matrix"


async def test_truncation_counts_code_points_not_bytes(renderer, monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_sender_label": "\U0001f47eIngrid",
            "source_sender_short_label": "ingrid",
            "source_sender_id": "@ingrid:example.org",
            "source_platform": "matrix",
        },
    )
    result = await render(renderer, relay_prefix="{sender2}: ")
    assert result.payload["text"] == "\U0001f47eI: Testing from Matrix"
    assert len("\U0001f47e".encode("utf-8")) == 4


async def test_value_shorter_than_suffix_renders_whole_value(renderer, monkeypatch):
    monkeypatch.setattr(
        "medre.adapters.meshtastic.renderer.project_source_fields",
        lambda *a, **kw: {
            "source_sender_label": "Ana",
            "source_sender_short_label": "ana",
            "source_sender_id": "@ana:example.org",
            "source_platform": "matrix",
        },
    )
    result = await render(renderer, relay_prefix="{sender5}[M]: ")
    assert result.payload["text"] == "Ana[M]: Testing from Matrix"


async def test_origin_label_truncation_suffix(renderer):
    renderer._configs["target"] = MeshtasticConfig(
        adapter_id="target", relay_prefix="{origin_label2}: "
    )
    result = await renderer.render(
        message(text="hello"),
        RenderingContext(
            target_adapter="target",
            target_platform="meshtastic",
            target_channel="3",
            delivery_strategy="direct",
            source_origin_label="East Relay",
        ),
    )
    assert result.payload["text"] == "Ea: hello"


async def test_sender_short_truncation_suffix(renderer):
    result = await render(renderer, relay_prefix="{sender_short4}: ")
    assert result.payload["text"] == "bobs: Testing from Matrix"


# ---------------------------------------------------------------------------
# Undeclared or invalid suffixes pass through unchanged
# ---------------------------------------------------------------------------


async def test_zero_suffix_is_not_a_truncation_variable(renderer):
    result = await render(renderer, relay_prefix="{sender0}: ")
    assert result.payload["text"] == "{sender0}: Testing from Matrix"
    assert result.metadata["relay_prefix_unknown_variables"] == ("sender0",)


async def test_undeclared_stem_with_suffix_stays_literal(renderer):
    result = await render(renderer, relay_prefix="{sender_id3}: ")
    assert result.payload["text"] == "{sender_id3}: Testing from Matrix"
    assert result.metadata["relay_prefix_unknown_variables"] == ("sender_id3",)


async def test_plain_variables_are_unaffected_by_declaration(renderer):
    result = await render(renderer, relay_prefix="{sender}: ")
    assert result.payload["text"] == "Bob Smith: Testing from Matrix"

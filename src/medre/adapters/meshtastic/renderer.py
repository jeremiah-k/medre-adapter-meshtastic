"""Meshtastic renderer for target-specific event rendering.

The :class:`MeshtasticRenderer` converts canonical events into
Meshtastic-ready content payloads (dicts with ``text`` and
``channel_index``).

The renderer is initialised with a **required** mapping of adapter IDs to
:class:`~medre.config.adapters.meshtastic.MeshtasticConfig` instances.
At render time the config for *target_adapter* is resolved from this
mapping — there is no fallback or default.  An empty mapping raises
:class:`ValueError` at construction; an unknown *target_adapter* raises
:class:`KeyError` at render time.

When the resolved config contains a non-empty ``relay_prefix``,
the renderer prepends a formatted prefix to the message text.  The prefix
template uses the shared named-placeholder grammar with generic variables:

* ``{sender}`` — primary sender label (generic, preferred).
* ``{sender_short}`` — abbreviated sender label.
* ``{sender_id}`` — sender node/transport ID.
* ``{origin_label}`` — source origin label (from route context or adapter
  registry).
* ``{platform}`` — source platform name.
* ``{channel}`` — source channel/room.
* ``{route_id}`` — route identifier.

Radio displays are constrained, so the label variables ``sender``,
``sender_short``, and ``origin_label`` additionally accept the shared
``{nameN}`` truncation suffix — ``{sender3}`` renders the first three
Unicode code points of the sender label (N >= 1).  A numeric-suffixed
undeclared name is rejected during validation and rendering.  The automatic Matrix default is the
``{sender5}[M]: `` template.

This renderer is owned by the Meshtastic adapter package and is registered
with the rendering pipeline.

Selection is via the rendering pipeline's platform registry: when the
pipeline populates the adapter's platform as ``"meshtastic"``, the renderer
matches on that platform string directly.

Text messages, replies, and reactions use bounded single-packet rendering by
default. Explicit ``max_text_parts > 1`` opts long ordinary messages into the
shared exact radio planner. Native tapbacks and descriptive reactions remain
single packets. Prefix, numbering and UTF-8 budgets apply independently to
every planned packet; exceeding the part cap rejects the complete plan.

**Cross-platform reaction rendering.**
When a reaction originates outside the configured Meshtastic instances
(e.g. Matrix → Meshtastic), the renderer emits descriptive text
rather than a native Meshtastic emoji tapback:

* ``{prefix}reacted {emoji} to "{abbreviated_text}"``
* The prefix preserves the same sender label and casing as ordinary
  messages. A separator space is added only when a nonempty prefix does
  not already end in whitespace.
* When the reacted-to text is genuinely unresolvable (no enriched
  ``original_text``/``fallback_text``), the
  quoted preview degrades to the honest wording ``an earlier message``
  instead of quoting an empty string.
* ``reply_id`` is set when the original message has a known Meshtastic
  packet ID bound to the destination adapter and channel,
  allowing the descriptive text to be sent as a structured reply.
* ``emoji`` is **not** set to ``1`` — the payload is plain text, not a
  native tapback.

Native Meshtastic-originated reactions continue to use ``emoji=1`` +
``reply_id`` for proper tapback round-tripping.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, Mapping

from medre.adapters.metadata import project_source_fields
from medre.core.events import CanonicalEvent, EventKind
from medre.core.events.text import select_message_body
from medre.core.rendering.attribution import (
    TRUNCATABLE_RELAY_LABELS,
    PrefixFormatterResult,
    RelayAttribution,
    resolve_relay_attribution,
    format_relay_prefix,
)
from medre.core.rendering.radio_text import plan_radio_text
from medre.core.rendering.relations import (
    describe_reaction,
    reaction_key,
    destination_native_message_id,
)
from medre.core.rendering.text_helpers import extract_event_text, truncate_text_bytes
from medre.core.rendering.renderer import (
    RenderingContext,
    RenderingResult,
)

if TYPE_CHECKING:
    from medre.config.adapters.meshtastic import MeshtasticConfig


# Label variables that accept the ``{nameN}`` first-N-code-points
# truncation suffix in relay-prefix templates.  Radio displays are the
# constrained-display affordance that motivates truncation on this
# transport; identifier-like variables (``sender_id``, ``platform``,
# ``channel``, ``route_id``) are deliberately excluded.

# Automatic Matrix attribution expressed as a template: the first five
# Unicode code points of the display name followed by the Matrix marker.
_AUTOMATIC_MATRIX_TEMPLATE = "{sender5}[M]: "


class MeshtasticRenderer:
    """Renderer for Meshtastic transport targets.

    Produces content dicts with ``text`` and ``channel_index``.

    **Target-aware rendering.** The renderer is initialised with a
    mapping of adapter IDs to :class:`~medre.config.adapters.meshtastic.MeshtasticConfig`
    instances.  At render time the config for *target_adapter* is resolved
    from this mapping.  This allows multi-radio setups where each adapter
    has different ``max_text_bytes`` and ``relay_prefix`` values.

    An empty *configs* mapping raises :class:`ValueError`.  An unknown
    *target_adapter* at render time raises :class:`KeyError`.

    Selection is via the pipeline's platform registry.
    """

    name: str = "meshtastic"
    """Renderer identifier used by the rendering pipeline for platform
    registry matching (``ctx.target_platform`` comparison)."""

    _PLATFORM: str = "meshtastic"
    """Internal platform identifier for matching via ``target_platform``."""

    def __init__(
        self,
        *,
        configs: Mapping[str, MeshtasticConfig],
        source_attribution: dict[str, Any] | None = None,
    ) -> None:
        if not configs:
            raise ValueError(
                "MeshtasticRenderer requires at least one adapter config. "
                "Pass a non-empty configs mapping."
            )
        self._configs: dict[str, MeshtasticConfig] = dict(configs)
        self._source_attribution: dict[str, Any] = dict(source_attribution or {})

    # ------------------------------------------------------------------
    # Capability check
    # ------------------------------------------------------------------

    # Event kinds that have a natural plain-text representation for
    # Meshtastic radio transports.
    _SUPPORTED_KINDS: frozenset[str] = frozenset(
        {
            EventKind.MESSAGE_TEXT,
            EventKind.MESSAGE_CREATED,
            EventKind.MESSAGE_EDITED,
            EventKind.MESSAGE_DELETED,
            EventKind.MESSAGE_REACTED,
            EventKind.PRESENCE_CHANGED,
            EventKind.PLUGIN_CUSTOM,
        }
    )

    def can_render(
        self,
        event: CanonicalEvent,
        ctx: RenderingContext,
    ) -> bool:
        """Return ``True`` when *ctx.target_platform* is ``"meshtastic"``
        and the event kind is supported.

        Parameters
        ----------
        event:
            The canonical event to check.
        ctx:
            Frozen rendering context with target identity and delivery
            metadata.

        Returns
        -------
        bool
            Whether this renderer handles events for the given adapter.
        """
        return (
            ctx.target_platform == self._PLATFORM
            and event.event_kind in self._SUPPORTED_KINDS
        )

    # ------------------------------------------------------------------
    # Prefix formatting
    # ------------------------------------------------------------------

    def _format_prefix_for(
        self,
        event: CanonicalEvent,
        relay_prefix: str | None,
        *,
        source_origin_label: str | None = None,
        source_attribution: RelayAttribution | None = None,
    ) -> PrefixFormatterResult:
        """Format a prefix template using adapter projection and the shared
        generic builder.

        Delegates to :func:`~medre.adapters.metadata.project_source_fields`
        for source-identity projection and
        :func:`~medre.core.rendering.attribution.build_relay_attribution` for
        generic envelope assembly.

        Available template variables:

        * ``{sender}`` — sender label (generic, preferred).
        * ``{sender_short}`` — sender short label (generic, preferred).
        * ``{sender_id}`` — sender node/transport ID.
        * ``{origin_label}`` — source origin label.
        * ``{platform}`` — source platform name.
        * ``{channel}`` — source channel/room.
        * ``{route_id}`` — route identifier.

        The label variables ``sender``, ``sender_short``, and
        ``origin_label`` accept the shared ``{nameN}`` truncation suffix:
        ``{sender3}`` renders the first three Unicode code points of the
        sender label (N >= 1).  A numeric suffix on an undeclared name is
        rejected during configuration validation and rendering.

        ``{meshnet_name}`` is not a known variable and is rejected.

        Falls back to empty strings for any unavailable variables.
        Never renders the literal text ``"None"``.

        Parameters
        ----------
        event:
            The canonical event whose source metadata is used for formatting.
        relay_prefix:
            The prefix template string.  ``None`` selects the automatic
            attribution template; an empty string disables the prefix.
        source_origin_label:
            Route label, including an explicitly empty override. Shared
            resolution then considers the supplied snapshot and source config.

        Returns
        -------
        PrefixFormatterResult
            Frozen result with rendered prefix string and diagnostic
            metadata.
        """
        if relay_prefix == "":
            return PrefixFormatterResult(
                rendered_prefix="",
                template_used=relay_prefix,
                variables_used=(),
                missing_variables=(),
                unknown_variables=(),
                formatting_error=None,
            )

        src_attr_cfg = self._source_attribution.get(event.source_adapter)
        # Project source identity from native metadata via dispatch.
        native_data: dict[str, object] = {}
        if event.metadata and event.metadata.native:
            native_data = dict(event.metadata.native.data)

        platform_hint = (
            getattr(src_attr_cfg, "platform", None) if src_attr_cfg else None
        )
        projected = project_source_fields(
            native_data,
            source_adapter=event.source_adapter,
            source_transport_id=event.source_transport_id,
            platform_hint=platform_hint,
        )

        attr = resolve_relay_attribution(
            event,
            source_origin_label=source_origin_label,
            configured_origin_label=getattr(src_attr_cfg, "origin_label", None),
            source_attribution=source_attribution,
            projected_fields=projected,
        )

        if relay_prefix is None:
            sender = (
                attr.source_sender_label
                or attr.source_sender_short_label
                or attr.source_sender_id
                or event.source_transport_id
                or event.source_adapter
            )
            if not sender.isprintable():
                sender = (
                    attr.source_sender_id
                    or event.source_transport_id
                    or event.source_adapter
                )
            if attr.source_platform == "matrix":
                # The automatic default is the shared truncation template:
                # five Unicode code points of the display name; the final
                # UTF-8 wire budget is enforced after composition.
                attr = replace(attr, source_sender_label=sender)
                relay_prefix = _AUTOMATIC_MATRIX_TEMPLATE
            else:
                attr = replace(
                    attr,
                    source_sender_short_label=attr.source_sender_short_label or sender,
                )
                relay_prefix = "{sender_short}: "
        result = format_relay_prefix(
            relay_prefix, attr, truncatable=TRUNCATABLE_RELAY_LABELS
        )
        if result.formatting_error:
            raise ValueError(f"Invalid relay_prefix: {result.formatting_error}")
        return result

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    async def render(
        self,
        event: CanonicalEvent,
        ctx: RenderingContext,
    ) -> RenderingResult:
        """Render a canonical event into a Meshtastic content payload.

        The rendered payload includes:

        * ``text``: extracted text from the event payload, with the
          configured ``relay_prefix`` prepended if set.
        * ``channel_index``: the adapter's ``default_channel``, overridden
          only when *ctx.target_channel* is a valid numeric value.

        **Target-aware config resolution.** The renderer resolves the
        config for *ctx.target_adapter* from the ``configs`` mapping
        supplied at construction.  If the adapter is not found, a
        :class:`KeyError` is raised — there is no fallback.

        **Fallback-text mode** — when ``ctx.delivery_strategy`` is
        ``"fallback_text"``, relation semantics are degraded into plain
        text within the native Meshtastic payload.  Native relation
        fields (``reply_id``, ``emoji``) are suppressed; relation
        context is expressed as readable text instead.  All Meshtastic
        payload ownership is preserved: ``channel_index``, prefix rules, and UTF-8 byte-safe truncation.

        **Direct mode** — a single relation may use destination-bound
        numeric packet IDs; native Meshtastic tapbacks additionally require
        an explicit reaction key and a Meshtastic source. Unbound replies,
        unbound reactions and mixed relations are described using the shared
        core policy. Cross-platform reactions remain described messages and
        may carry a separately bound reply_id, never an emoji flag. Numeric
        metadata alone cannot establish destination binding.

        UTF-8 byte-budget truncation is applied after final radio text
        rendering (including prefix, reply, and reaction formatting).
        The byte budget defaults to 227 (``MeshtasticConfig.max_text_bytes``)
        and is configurable per adapter instance.  Note: the budget applies
        to text only; ``reply_id``/``emoji`` protobuf fields are encoded
        separately and consume up to ~8 bytes of the 233-byte wire limit.
        Operators sending relation-heavy traffic should consider lowering
        ``max_text_bytes`` to ~219-225.

        Parameters
        ----------
        event:
            The canonical event to render.
        ctx:
            Frozen rendering context with target identity, delivery
            strategy, capability metadata, and text budgets.

        Returns
        -------
        RenderingResult
            The rendered Meshtastic content dict wrapped in a result.
        """
        # Unpack context.
        target_adapter = ctx.target_adapter
        target_channel = ctx.target_channel
        delivery_strategy = ctx.delivery_strategy
        is_fallback = delivery_strategy == "fallback_text"

        # Resolve target-adapter-specific config.
        try:
            adapter_config = self._configs[target_adapter]
        except KeyError:
            raise KeyError(
                f"No MeshtasticConfig registered for target_adapter "
                f"{target_adapter!r}. Known adapters: "
                f"{sorted(self._configs.keys())}"
            ) from None

        prefix = adapter_config.relay_prefix
        max_text_bytes = min(
            adapter_config.max_text_bytes,
            (
                ctx.max_text_bytes
                if ctx.max_text_bytes is not None
                else adapter_config.max_text_bytes
            ),
        )

        # Resolve channel index: adapter config default, overridden only by
        # a valid numeric target_channel.  Invalid/non-numeric values keep
        # the adapter's default_channel (do not silently force 0).
        channel_index = adapter_config.default_channel
        if target_channel is not None:
            try:
                channel_index = int(target_channel)
            except (ValueError, TypeError):
                pass

        content: dict[str, object] = {
            "channel_index": channel_index,
        }

        # -- Structured reply / reaction rendering ----------------------------
        wire_prefix = ""
        is_structured_reaction = False
        is_descriptive_reaction = False
        prefix_result: PrefixFormatterResult = PrefixFormatterResult(
            rendered_prefix="",
            template_used="",
            variables_used=(),
            missing_variables=(),
            unknown_variables=(),
            formatting_error=None,
        )

        body = select_message_body(event.payload)
        # Native reply/tapback is available for one relation only. Mixed
        # relations degrade together rather than silently dropping context or
        # attaching a native tapback to authored message text.
        rel = event.relations[0] if len(event.relations) == 1 else None
        reply_id = None
        if rel is not None and not is_fallback:
            native_id = destination_native_message_id(
                rel, target_adapter, str(channel_index)
            )
            if native_id is not None:
                try:
                    numeric_id = int(native_id)
                except ValueError:
                    pass
                else:
                    if 0 < numeric_id <= 0xFFFFFFFF:
                        reply_id = numeric_id
        if (
            rel is not None
            and rel.relation_type == "reaction"
            and event.event_kind == EventKind.MESSAGE_REACTED
        ):
            key = reaction_key(rel)
            if self._is_meshtastic_source(event) and reply_id is not None:
                content.update(text=key, reply_id=reply_id, emoji=1)
                is_structured_reaction = True
            else:
                content["text"] = describe_reaction(rel)
                is_descriptive_reaction = True
                if reply_id is not None:
                    content["reply_id"] = reply_id
        elif rel is not None and rel.relation_type == "reply" and reply_id is not None:
            content.update(text=body, reply_id=reply_id)
        else:
            content["text"] = extract_event_text(event)

        prefix_result = self._format_prefix_for(
            event,
            prefix,
            source_origin_label=ctx.source_origin_label,
            source_attribution=ctx.source_attribution,
        )
        if not is_structured_reaction and prefix_result.rendered_prefix:
            wire_prefix = prefix_result.rendered_prefix
            if is_descriptive_reaction and not wire_prefix[-1].isspace():
                wire_prefix += " "
            content["text"] = wire_prefix + str(content["text"])

        # -- UTF-8 byte-budget truncation after final rendering ------
        final_text = str(content.get("text", ""))
        if is_structured_reaction and len(final_text.encode("utf-8")) > max_text_bytes:
            raise ValueError("Native reaction key exceeds destination byte budget")
        radio_plan = None
        if (
            adapter_config.max_text_parts > 1
            and max_text_bytes > 0
            and event.event_kind == "message.created"
            and not is_structured_reaction
            and not is_descriptive_reaction
            and len(final_text.encode("utf-8")) > max_text_bytes
        ):
            radio_plan = plan_radio_text(
                final_text[len(wire_prefix) :],
                prefix=wire_prefix,
                max_text_bytes=max_text_bytes,
                max_parts=adapter_config.max_text_parts,
            )
            truncated_text = radio_plan.parts[0].wire_text
            was_truncated = False
            original_bytes = len(final_text.encode("utf-8"))
            rendered_bytes = sum(part.wire_bytes for part in radio_plan.parts)
        else:
            truncated_text, was_truncated, original_bytes, rendered_bytes = (
                truncate_text_bytes(final_text, max_text_bytes)
            )
        content["text"] = truncated_text

        metadata: dict[str, object] = {
            "renderer": self.name,
            "original_length": len(final_text),
            "rendered_length": len(truncated_text),
            "original_text_bytes": original_bytes,
            "rendered_text_bytes": rendered_bytes,
            "max_text_bytes": max_text_bytes,
            "truncated": was_truncated,
        }
        if radio_plan is not None:
            metadata["radio_part_count"] = len(radio_plan.parts)
            metadata["radio_plan_fingerprint"] = radio_plan.fingerprint
            metadata["rendered_text_chars"] = sum(
                len(part.wire_text) for part in radio_plan.parts
            )
            metadata["rendered_length"] = metadata["rendered_text_chars"]
        formatted_prefix = prefix_result.rendered_prefix
        if prefix != "":
            # Normalized keys (consistent across all renderers)
            metadata["relay_prefix_template"] = prefix_result.template_used
            metadata["relay_prefix_rendered"] = formatted_prefix
            metadata["relay_prefix_variables_used"] = prefix_result.variables_used
            metadata["relay_prefix_missing_variables"] = prefix_result.missing_variables
            metadata["relay_prefix_unknown_variables"] = prefix_result.unknown_variables
            metadata["relay_prefix_formatting_error"] = prefix_result.formatting_error
        if is_descriptive_reaction:
            metadata["descriptive_reaction"] = True
        if is_fallback:
            metadata["delivery_strategy"] = delivery_strategy

        return RenderingResult(
            event_id=event.event_id,
            target_adapter=target_adapter,
            target_channel=target_channel,
            payload=content,
            metadata=metadata,
            truncated=was_truncated,
            radio_plan=radio_plan,
            fallback_applied="strategy_fallback_text" if is_fallback else None,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _is_meshtastic_source(self, event: CanonicalEvent) -> bool:
        """Recognize Meshtastic source instances from the provider-owned config map.

        Runtime assembly supplies every enabled Meshtastic instance, so an LF
        radio and an LT radio share native reaction semantics despite distinct
        IDs. External sources retain descriptive reaction rendering.
        Wire metadata and adapter-name spelling do not establish transport type.
        """
        return event.source_adapter in self._configs


def build_meshtastic_renderer(
    *,
    runtime_configs: Mapping[str, Any],
    all_runtime_configs: Mapping[str, Mapping[str, Any]],
    source_attribution: Mapping[str, Any],
) -> MeshtasticRenderer | None:
    """Build the registered Meshtastic renderer for runtime assembly."""
    del all_runtime_configs
    from medre.config.adapters.meshtastic import MeshtasticConfig

    configs: dict[str, MeshtasticConfig] = {}
    for rtc in runtime_configs.values():
        if not getattr(rtc, "enabled", False):
            continue
        adapter_id = rtc.adapter_id
        config = rtc.config
        if config is None:
            config = MeshtasticConfig(adapter_id=adapter_id, relay_prefix="")
        configs[adapter_id] = config
    if not configs:
        return None
    return MeshtasticRenderer(
        configs=configs,
        source_attribution=dict(source_attribution),
    )

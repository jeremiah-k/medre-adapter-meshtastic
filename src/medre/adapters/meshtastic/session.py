"""Meshtastic session lifecycle boundary.

:class:`MeshtasticSession` owns the raw Meshtastic transport lifecycle:
client construction, connection establishment, inbound-packet callback
registration, lifetime reconnection supervision, TCP liveness probing, and
graceful teardown.

The adapter delegates all client ownership to this session object.
The session owns raw transport; the adapter owns semantic conversion.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from collections.abc import Awaitable
from typing import Any, cast

from medre.adapters.meshtastic.compat import HAS_MESHTASTIC
from medre.adapters.meshtastic.errors import (
    MeshtasticConnectionError,
    MeshtasticSendError,
)
from medre.config.adapters.meshtastic import MeshtasticConfig

_logger = logging.getLogger(__name__)

# Exponential reconnect jitter.  Backoff base/cap are adapter configuration.
_BACKOFF_JITTER_FRACTION: float = 0.25

# Delay before the first TCP liveness probe after startup.  This is deliberately
# internal: operators configure the steady-state interval/timeout, while MEDRE
# avoids probing during the SDK's initial configuration exchange.
_LIVENESS_INITIAL_DELAY_SECONDS: float = 5.0

# Maximum transient retry attempts for outbound send.
_MAX_SEND_RETRIES: int = 3


def _normalize_emoji_flag(emoji: object) -> int | None:
    """Validate and normalize an emoji flag for Meshtastic structured send.

    Returns ``1``, ``None``, or raises ``MeshtasticSendError``.
    """
    if emoji is None:
        return None
    if isinstance(emoji, bool):
        return 1 if emoji else None
    if isinstance(emoji, int):
        if emoji in (0, 1):
            return 1 if emoji == 1 else None
        raise MeshtasticSendError(
            f"invalid Meshtastic emoji flag for structured send: {emoji!r}",
            transient=False,
        )
    if isinstance(emoji, str):
        stripped = emoji.strip()
        if stripped in ("0", "1"):
            return 1 if stripped == "1" else None
    raise MeshtasticSendError(
        f"invalid Meshtastic emoji flag for structured send: {emoji!r}",
        transient=False,
    )


@dataclass(frozen=True)
class MeshtasticSessionDiagnostics:
    """Read-only snapshot of session operational state.

    No secrets, private keys, raw protobuf dumps, or sensitive radio
    identifiers beyond what is public.
    """

    connected: bool
    reconnecting: bool
    reconnect_attempts: int
    last_packet_time: float | None
    node_id: str | None
    channel_count: int
    transient_delivery_failures: int
    permanent_delivery_failures: int
    stale_receive_callbacks: int
    stale_disconnect_callbacks: int
    last_error: str | None
    reconnect_total_attempts: int = 0
    liveness_enabled: bool = False
    liveness_probe_successes: int = 0
    liveness_probe_failures: int = 0
    liveness_consecutive_failures: int = 0
    last_liveness_probe_time: float | None = None
    last_liveness_success_time: float | None = None
    last_liveness_error: str | None = None


class MeshtasticSession:
    """Transport-owned session boundary for Meshtastic connections.

    Owns the raw client interface and manages its full lifecycle:
    creation, callback registration, inbound message forwarding,
    transport-specific liveness/recovery supervision, and graceful teardown.

    Parameters
    ----------
    config:
        Validated :class:`~medre.config.adapters.meshtastic.MeshtasticConfig`.
    adapter_id:
        The adapter identifier (for logging).
    platform:
        Platform name (always ``"meshtastic"``).
    logger:
        Optional :class:`logging.Logger`.  When ``None`` a module-level
        fallback logger is used.
    """

    __slots__ = (
        "__weakref__",
        "_config",
        "_adapter_id",
        "_platform",
        "_client",
        "_client_state_lock",
        "_connection_generation",
        "_message_callback",
        "_logger",
        "_started",
        "_subscribed",
        "_subscribed_connection_lost",
        "_stop_requested",
        "_loop",
        # Reconnect state
        "_reconnecting",
        "_reconnect_attempts",
        "_reconnect_total_attempts",
        "_reconnect_task",
        "_startup_connect_task",
        "_liveness_task",
        # Diagnostics
        "_last_packet_time",
        "_node_id",
        "_channel_count",
        "_transient_delivery_failures",
        "_permanent_delivery_failures",
        "_stale_receive_callbacks",
        "_stale_disconnect_callbacks",
        "_liveness_probe_successes",
        "_liveness_probe_failures",
        "_liveness_consecutive_failures",
        "_last_liveness_probe_time",
        "_last_liveness_success_time",
        "_last_liveness_error",
        "_last_error",
    )

    def __init__(
        self,
        config: MeshtasticConfig,
        adapter_id: str,
        platform: str,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._adapter_id = adapter_id
        self._platform = platform
        self._client: Any = None
        self._client_state_lock = threading.RLock()
        self._connection_generation: int = 0
        self._message_callback: Callable[[dict[str, Any]], None] | None = None
        self._logger: logging.Logger = logger if logger is not None else _logger
        self._started: bool = False
        self._subscribed: bool = False
        self._subscribed_connection_lost: bool = False
        self._stop_requested: bool = False
        self._loop: asyncio.AbstractEventLoop | None = None
        # Reconnect state
        self._reconnecting: bool = False
        self._reconnect_attempts: int = 0
        self._reconnect_total_attempts: int = 0
        self._reconnect_task: asyncio.Task[Any] | None = None
        self._startup_connect_task: asyncio.Task[Any] | None = None
        self._liveness_task: asyncio.Task[Any] | None = None
        # Diagnostics
        self._last_packet_time: float | None = None
        self._node_id: str | None = None
        self._channel_count: int = 0
        self._transient_delivery_failures: int = 0
        self._permanent_delivery_failures: int = 0
        self._stale_receive_callbacks: int = 0
        self._stale_disconnect_callbacks: int = 0
        self._liveness_probe_successes: int = 0
        self._liveness_probe_failures: int = 0
        self._liveness_consecutive_failures: int = 0
        self._last_liveness_probe_time: float | None = None
        self._last_liveness_success_time: float | None = None
        self._last_liveness_error: str | None = None
        self._last_error: str | None = None

    # -- Properties -----------------------------------------------------------

    @property
    def connected(self) -> bool:
        """``True`` when the active SDK client reports a live connection.

        Older/fake clients may not expose ``isConnected``; for those, client
        ownership plus the started lifecycle remains the compatibility signal.
        """
        with self._client_state_lock:
            client = self._client
            started = self._started
            reconnecting = self._reconnecting
        if client is None or not started or reconnecting:
            return False
        connected_event = getattr(client, "isConnected", None)
        is_set = getattr(connected_event, "is_set", None)
        if callable(is_set):
            try:
                return bool(is_set())
            except Exception:
                return False
        return True

    @property
    def connection_generation(self) -> int:
        """Generation of the active SDK client ownership state."""
        with self._client_state_lock:
            return self._connection_generation

    def is_connection_generation_current(self, generation: int) -> bool:
        """Return whether *generation* still names the active client state."""
        with self._client_state_lock:
            return (
                not self._stop_requested and generation == self._connection_generation
            )

    def _activate_client(self, client: Any) -> None:
        """Install a new SDK client and advance the connection generation."""
        with self._client_state_lock:
            self._client = client
            self._connection_generation += 1

    def _invalidate_client(self) -> Any:
        """Detach the active SDK client and invalidate callbacks that captured it."""
        with self._client_state_lock:
            client = self._client
            self._client = None
            self._node_id = None
            self._connection_generation += 1
            return client

    @property
    def reconnecting(self) -> bool:
        """``True`` when the session is in a reconnect backoff phase."""
        return self._reconnecting

    @property
    def reconnect_attempts(self) -> int:
        """Number of consecutive reconnect attempts in the current cycle."""
        return self._reconnect_attempts

    @property
    def last_packet_time(self) -> float | None:
        """Monotonic time of last received packet, or ``None``."""
        return self._last_packet_time

    @property
    def node_id(self) -> str | None:
        """The session's own node ID in the format matching ``fromId`` in inbound
        packets (typically ``"!" + lowercase_hex(myNodeNum)``, e.g. ``"!a1b2c3d4"``).

        Populated from ``interface.myInfo.myNodeNum`` after every successful
        connect (and re-connect).  If ``myInfo`` is not yet available at
        connect time, the ``_on_receive`` callback lazily refreshes on each
        inbound packet until the value is obtained.  Returns ``None`` when
        the client is not connected or ``myInfo`` is not yet available.
        """
        return self._node_id

    @property
    def channel_count(self) -> int:
        """Count of known channels, if available."""
        return self._channel_count

    @property
    def transient_delivery_failures(self) -> int:
        """Number of transient outbound send failures."""
        return self._transient_delivery_failures

    @property
    def permanent_delivery_failures(self) -> int:
        """Number of permanent outbound send failures."""
        return self._permanent_delivery_failures

    @property
    def last_error(self) -> str | None:
        """Description of the most recent error, if any."""
        return self._last_error

    @property
    def client(self) -> Any:
        """The underlying client interface, or ``None``."""
        return self._client

    def get_node_info(self, node_id: str) -> dict[str, str] | None:
        """Look up a node's longname and shortname from the SDK client.

        Returns a plain dict with ``longname`` and ``shortname`` keys, or
        ``None`` when the node is unknown or the client is unavailable.

        Parameters
        ----------
        node_id:
            The Meshtastic node ID to look up (e.g. ``"!abcdef12"``).

        Returns
        -------
        dict[str, str] | None
            ``{"longname": ..., "shortname": ...}`` or ``None``.
        """
        if self._client is None:
            return None
        client_nodes = getattr(self._client, "nodes", None)
        if not isinstance(client_nodes, dict):
            return None
        node_info = client_nodes.get(node_id)
        if not isinstance(node_info, dict):
            return None
        user_info = node_info.get("user")
        if not isinstance(user_info, dict):
            return None
        longname = str(user_info.get("longName", "") or "")
        shortname = str(user_info.get("shortName", "") or "")
        if not longname and not shortname:
            return None
        result: dict[str, str] = {}
        if longname:
            result["longname"] = longname
        if shortname:
            result["shortname"] = shortname
        return result

    # -- Lifecycle ------------------------------------------------------------

    async def start(
        self,
        message_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        """Wait within the startup budget, then retain degraded recovery ownership.

        Native construction runs off-loop. Expiry retains the same attempt;
        ordinary connection refusals enter capped lifetime retries. Configuration
        and callback setup errors remain startup failures.

        Parameters
        ----------
        message_callback:
            Callback invoked with raw packet dicts for inbound messages.

        Raises
        ------
        MeshtasticConnectionError
            If ``mtjk`` is not installed and connection_type is not ``"fake"``,
            before a native connection attempt can be made.
        """
        if self._started:
            self._logger.warning(
                "MeshtasticSession.start() called while already running"
            )
            return

        if self._startup_connect_task is not None:
            raise RuntimeError("Meshtastic connection setup is already in progress")
        self._stop_requested = False
        self._reconnect_attempts = 0
        self._reconnect_total_attempts = 0
        self._reconnecting = False
        self._last_error = None
        self._liveness_probe_successes = 0
        self._liveness_probe_failures = 0
        self._liveness_consecutive_failures = 0
        self._last_liveness_probe_time = None
        self._last_liveness_success_time = None
        self._last_liveness_error = None
        self._stale_receive_callbacks = 0
        self._stale_disconnect_callbacks = 0
        self._message_callback = message_callback
        self._loop = asyncio.get_running_loop()

        conn = self._config.connection_type

        if conn == "fake":
            self._client = None
            self._node_id = None
        else:
            if not HAS_MESHTASTIC:
                raise MeshtasticConnectionError(
                    "mtjk not installed; pip install 'medre-adapter-meshtastic'"
                )
            # Validation failures are operator errors, not offline transports.
            self._config.validate()
            pending = asyncio.create_task(
                self._create_client_offloop(),
                name=f"meshtastic-start:{self._adapter_id}",
            )
            self._startup_connect_task = pending
            try:
                async with asyncio.timeout(self._config.sync_timeout_ms / 1000):
                    # wait() retains ownership without cancelling the attempt.
                    # Python 3.14 shield() logs late failures independently of
                    # the recovery supervisor that retrieves them.
                    await asyncio.wait((pending,))
                    client = pending.result()
                self._startup_connect_task = None
            except asyncio.CancelledError:
                await self._cancel_startup_attempt(pending)
                raise
            except TimeoutError:
                self._last_error = (
                    "Initial connection did not complete within startup budget"
                )
                self._logger.warning(
                    "MeshtasticSession %s initial connection exceeded %.1fs; "
                    "continuing degraded with recovery active",
                    self._adapter_id,
                    self._config.sync_timeout_ms / 1000,
                )
                # Retain this attempt. Starting another constructor here would
                # compete for the same radio while the SDK call is still live.
                self._reconnecting = True
                self._reconnect_task = asyncio.create_task(
                    self._reconnect_loop(pending),
                    name=f"meshtastic-reconnect:{self._adapter_id}",
                )
                client = None
            except MeshtasticConnectionError as exc:
                self._startup_connect_task = None
                self._last_error = str(exc)
                self._logger.warning(
                    "MeshtasticSession %s initial connection unavailable; "
                    "continuing degraded with recovery active: %s",
                    self._adapter_id,
                    exc,
                )
                self._start_reconnect_task()
                client = None
            except BaseException:
                self._startup_connect_task = None
                raise

            if client is not None:
                self._activate_client(client)
                try:
                    self._subscribe_callbacks()
                    self._refresh_node_id()
                except Exception:
                    self._subscribed = False
                    try:
                        close_fn = getattr(self._client, "close", None)
                        if close_fn is not None:
                            close_fn()
                    except Exception:
                        pass
                    self._invalidate_client()
                    raise

        self._started = True
        if self._tcp_liveness_enabled:
            self._liveness_task = asyncio.create_task(
                self._liveness_loop(),
                name=f"meshtastic-liveness:{self._adapter_id}",
            )
        self._logger.info(
            "MeshtasticSession %s started (mode=%s)",
            self._adapter_id,
            conn,
        )

    async def stop(self, timeout: float = 5.0) -> None:
        """Disconnect from the Meshtastic node.  Idempotent.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for a clean shutdown.
        """
        if not self._started and self._startup_connect_task is None:
            return
        pending_start = self._startup_connect_task

        # Signal stop to prevent reconnect loops
        self._stop_requested = True
        self._reconnecting = False
        # Reset reconnect counter so diagnostics are truthful after stop.
        self._reconnect_attempts = 0

        # Cancel liveness before reconnect/client teardown so a probe cannot
        # race shutdown and schedule fresh recovery work.
        if self._liveness_task is not None:
            if not self._liveness_task.done():
                self._liveness_task.cancel()
                try:
                    await asyncio.wait_for(self._liveness_task, timeout=timeout)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            self._liveness_task = None

        # Cancel reconnect task if running
        if self._reconnect_task is not None:
            if not self._reconnect_task.done():
                self._reconnect_task.cancel()
                try:
                    await asyncio.wait_for(self._reconnect_task, timeout=timeout)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            self._reconnect_task = None

        # A supervisor cancelled before its first step never awaits its
        # pending constructor. Retain independent ownership so this path still
        # installs the late-result disposer and consumes any eventual failure.
        if pending_start is not None:
            await self._cancel_startup_attempt(pending_start)

        self._unsubscribe_callbacks()

        client = self._invalidate_client()
        if client is not None:
            try:
                close_fn = getattr(client, "close", None)
                if close_fn is not None:
                    close_fn()
            except Exception:
                pass

        self._started = False
        self._loop = None
        self._logger.info("MeshtasticSession %s stopped", self._adapter_id)

    # -- Outbound send --------------------------------------------------------

    async def send(
        self,
        packet_dict: dict[str, Any],
        *,
        before_native_send: Callable[[], Awaitable[None]] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> object | None:
        """Send via the client; guarded packets permit exactly one native invocation.

        Guarded callbacks run after queue pacing. Cancellation and exact client
        generation are checked around the grant and inside the SDK worker.
        Ordinary packets retain their existing bounded retry policy.

        When *packet_dict* contains a ``reply_id`` key, the method uses
        the protobuf ``_sendPacket`` path to build a structured
        ``MeshPacket`` with ``reply_id`` and optional ``emoji`` fields.
        Missing protobuf modules or ``_sendPacket`` raise
        :class:`MeshtasticSendError` with ``transient=False``.

        When no ``reply_id`` is present the existing ``sendText`` path is
        used with bounded transient retry.

        Parameters
        ----------
        packet_dict:
            Dict with at least ``text`` and ``channel_index`` keys.
            May include ``reply_id`` (int) and ``emoji`` (int) for
            structured reply / reaction sends.
        before_native_send:
            Runtime-only one-use core grant. Its presence disables SDK retries.
        is_cancelled:
            Thread-safe cancellation predicate for the guarded packet owner;
            checked again in the worker immediately before native use.

        Returns
        -------
        object | None
            Opaque SDK packet object or mapping, normalized by the queue owner;
            ``None`` in fake mode. Guarded packets require a retained native ID.

        Raises
        ------
        MeshtasticSendError
            On permanent send failure or after max transient retries.
        """
        if before_native_send is not None:
            return await self._send_guarded(
                packet_dict, before_native_send, is_cancelled
            )
        if is_cancelled is not None:
            raise MeshtasticSendError(
                "cancellation fence requires a guarded send", transient=False
            )
        if self._client is None:
            # Fake mode — no real send
            return None

        text = str(packet_dict.get("text", ""))
        channel_index = packet_dict.get("channel_index", 0)
        reply_id = packet_dict.get("reply_id")
        emoji = packet_dict.get("emoji")

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_SEND_RETRIES + 1):
            try:
                if reply_id is not None:
                    result = await self._send_structured(
                        text, channel_index, reply_id, emoji
                    )
                else:
                    result = await asyncio.to_thread(
                        self._client.sendText,
                        text,
                        channelIndex=channel_index,
                    )
                return cast(object, result)
            except asyncio.CancelledError:
                raise
            except MeshtasticSendError as exc:
                if not exc.transient:
                    self._permanent_delivery_failures += 1
                    self._last_error = f"Permanent send failure: {exc}"
                    raise
                # Transient MeshtasticSendError — retry
                last_exc = exc
                self._transient_delivery_failures += 1
                self._last_error = f"Transient send failure (attempt {attempt}): {exc}"
                self._logger.warning(
                    "MeshtasticSession %s send failure " "(attempt %d/%d): %s",
                    self._adapter_id,
                    attempt,
                    _MAX_SEND_RETRIES,
                    exc,
                )
                if attempt < _MAX_SEND_RETRIES:
                    await asyncio.sleep(0.1 * attempt)
            except ConnectionError as exc:
                last_exc = exc
                self._transient_delivery_failures += 1
                self._last_error = f"Transient send failure (attempt {attempt}): {exc}"
                self._logger.warning(
                    "MeshtasticSession %s transient send failure "
                    "(attempt %d/%d): %s",
                    self._adapter_id,
                    attempt,
                    _MAX_SEND_RETRIES,
                    exc,
                )
                if attempt < _MAX_SEND_RETRIES:
                    await asyncio.sleep(0.1 * attempt)
            except OSError as exc:
                last_exc = exc
                self._transient_delivery_failures += 1
                self._last_error = f"Transient send failure (attempt {attempt}): {exc}"
                self._logger.warning(
                    "MeshtasticSession %s transient send failure "
                    "(attempt %d/%d): %s",
                    self._adapter_id,
                    attempt,
                    _MAX_SEND_RETRIES,
                    exc,
                )
                if attempt < _MAX_SEND_RETRIES:
                    await asyncio.sleep(0.1 * attempt)
            except (ValueError, TypeError) as exc:
                # Non-transient: raise immediately
                self._permanent_delivery_failures += 1
                self._last_error = f"Permanent send failure: {exc}"
                raise MeshtasticSendError(
                    f"Permanent send failure: {exc}",
                    transient=False,
                ) from exc
            except Exception as exc:
                last_exc = exc
                self._transient_delivery_failures += 1
                self._last_error = f"Send failure (attempt {attempt}): {exc}"
                self._logger.warning(
                    "MeshtasticSession %s send failure " "(attempt %d/%d): %s",
                    self._adapter_id,
                    attempt,
                    _MAX_SEND_RETRIES,
                    exc,
                )
                if attempt < _MAX_SEND_RETRIES:
                    await asyncio.sleep(0.1 * attempt)

        # All retries exhausted
        self._permanent_delivery_failures += 1
        self._last_error = f"Send failed after {_MAX_SEND_RETRIES} attempts: {last_exc}"
        raise MeshtasticSendError(
            f"Send failed after {_MAX_SEND_RETRIES} attempts: {last_exc}"
        ) from last_exc

    async def _send_guarded(
        self,
        packet: dict[str, Any],
        before_native_send: Callable[[], Awaitable[None]],
        is_cancelled: Callable[[], bool] | None,
    ) -> object | None:
        """Consume one paced grant and invoke only its captured native client once.

        Recheck cancellation and client generation in the SDK worker as well as
        around the SQLite await. A consumed grant never enters native retry.
        """
        text = packet.get("text")
        if not isinstance(text, str):
            raise MeshtasticSendError("guarded text must be a string", transient=False)
        with self._client_state_lock:
            client, generation = self._client, self._connection_generation

        def check_current() -> None:
            """Fence a cancelled waiter or retired SDK before the native invocation."""
            if is_cancelled is not None and is_cancelled():
                raise MeshtasticSendError("guarded send cancelled", transient=False)
            if not self.connected:
                raise MeshtasticSendError(
                    "session unavailable before guarded send", transient=True
                )
            with self._client_state_lock:
                if (
                    self._stop_requested
                    or self._client is not client
                    or self._connection_generation != generation
                ):
                    raise MeshtasticSendError(
                        "guarded send client is no longer current", transient=True
                    )

        check_current()
        await before_native_send()
        try:
            check_current()
            if packet.get("reply_id") is not None:
                return cast(
                    object,
                    await self._send_structured(
                        text,
                        packet.get("channel_index", 0),
                        packet["reply_id"],
                        packet.get("emoji"),
                        client=client,
                        before_native_call=check_current,
                    ),
                )

            def native_send() -> Any:
                """The worker checks its exact owner just before entering the SDK."""
                check_current()
                return client.sendText(
                    text, channelIndex=packet.get("channel_index", 0)
                )

            return cast(object, await asyncio.to_thread(native_send))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise MeshtasticSendError(
                "guarded native send did not produce a confirmed handoff",
                transient=False,
            ) from exc

    async def _send_structured(
        self,
        text: str,
        channel_index: int,
        reply_id: int,
        emoji: int | None,
        *,
        client: Any = None,
        before_native_call: Callable[[], None] | None = None,
    ) -> Any:
        """Send a structured message via protobuf ``_sendPacket``.

        Builds a ``MeshPacket`` with ``Data`` payload containing
        ``TEXT_MESSAGE_APP``, the text payload, ``reply_id``, and
        optional ``emoji=1``.

        Raises
        ------
        MeshtasticSendError
            With ``transient=False`` when protobuf modules or
            ``_sendPacket`` are unavailable.
        """
        if isinstance(reply_id, bool):
            raise MeshtasticSendError(
                f"invalid Meshtastic reply_id for structured send: {reply_id!r}",
                transient=False,
            )

        try:
            from meshtastic.protobuf import mesh_pb2, portnums_pb2
        except ImportError as exc:
            raise MeshtasticSendError(
                f"meshtastic protobuf modules not available: {exc}",
                transient=False,
            ) from exc

        client = self._client if client is None else client
        _send_packet = getattr(client, "_sendPacket", None)
        if _send_packet is None:
            raise MeshtasticSendError(
                "client does not expose _sendPacket for structured send",
                transient=False,
            )

        text_portnum = getattr(portnums_pb2, "TEXT_MESSAGE_APP", None)
        if text_portnum is None:
            portnum_enum = getattr(portnums_pb2, "PortNum", None)
            text_portnum = getattr(portnum_enum, "TEXT_MESSAGE_APP", None)
        if text_portnum is None:
            raise MeshtasticSendError(
                "meshtastic TEXT_MESSAGE_APP protobuf enum is unavailable",
                transient=False,
            )

        try:
            reply_id_int = int(reply_id)
        except (TypeError, ValueError) as exc:
            raise MeshtasticSendError(
                f"invalid Meshtastic reply_id for structured send: {reply_id!r}",
                transient=False,
            ) from exc

        data = mesh_pb2.Data()
        data.portnum = text_portnum
        data.payload = text.encode("utf-8")
        try:
            data.reply_id = reply_id_int
        except AttributeError as exc:
            raise MeshtasticSendError(
                "structured Meshtastic send requires Data.reply_id support",
                transient=False,
            ) from exc
        emoji_flag = _normalize_emoji_flag(emoji)
        if emoji_flag == 1:
            try:
                data.emoji = 1
            except AttributeError as exc:
                raise MeshtasticSendError(
                    "structured Meshtastic reaction requires Data.emoji support",
                    transient=False,
                ) from exc

        mesh_packet = mesh_pb2.MeshPacket()
        mesh_packet.decoded.CopyFrom(data)
        mesh_packet.channel = channel_index
        generate_packet_id = getattr(client, "_generatePacketId", None)
        if callable(generate_packet_id):
            try:
                mesh_packet.id = cast(int, generate_packet_id())
            except Exception:
                pass

        send_kwargs: dict[str, Any] = {"wantAck": False}
        try:
            import meshtastic

            broadcast_addr = getattr(meshtastic, "BROADCAST_ADDR", None)
            signature = inspect.signature(_send_packet)
            accepts_destination = "destinationId" in signature.parameters or any(
                param.kind is inspect.Parameter.VAR_KEYWORD
                for param in signature.parameters.values()
            )
            if broadcast_addr is not None and accepts_destination:
                send_kwargs["destinationId"] = broadcast_addr
        except Exception:
            pass

        if before_native_call is None:
            result = await asyncio.to_thread(_send_packet, mesh_packet, **send_kwargs)
        else:

            def guarded_native() -> Any:
                """Refuse a stale SDK or cancelled waiter before structured native use."""
                before_native_call()
                return _send_packet(mesh_packet, **send_kwargs)

            result = await asyncio.to_thread(guarded_native)
        if result is not None:
            return result
        # _sendPacket may return None even after successfully sending
        # (SDK mutates the packet, setting id via _generatePacketId).
        # Fall back to the mesh_packet so the caller can still extract
        # the packet ID via getattr(obj, "id", None).
        packet_id = getattr(mesh_packet, "id", None)
        if packet_id:
            return mesh_packet
        return None

    # -- Diagnostics ----------------------------------------------------------

    def diagnostics(self) -> MeshtasticSessionDiagnostics:
        """Return a read-only snapshot of session state.

        Never exposes secrets, private keys, raw protobuf dumps, or
        sensitive radio identifiers beyond what is public.
        """
        return MeshtasticSessionDiagnostics(
            connected=self.connected,
            reconnecting=self._reconnecting,
            reconnect_attempts=self._reconnect_attempts,
            reconnect_total_attempts=self._reconnect_total_attempts,
            liveness_enabled=self._tcp_liveness_enabled,
            liveness_probe_successes=self._liveness_probe_successes,
            liveness_probe_failures=self._liveness_probe_failures,
            liveness_consecutive_failures=self._liveness_consecutive_failures,
            last_liveness_probe_time=self._last_liveness_probe_time,
            last_liveness_success_time=self._last_liveness_success_time,
            last_liveness_error=self._last_liveness_error,
            last_packet_time=self._last_packet_time,
            node_id=self._node_id,
            channel_count=self._channel_count,
            transient_delivery_failures=self._transient_delivery_failures,
            permanent_delivery_failures=self._permanent_delivery_failures,
            stale_receive_callbacks=self._stale_receive_callbacks,
            stale_disconnect_callbacks=self._stale_disconnect_callbacks,
            last_error=self._last_error,
        )

    # -- Client creation (protected, overridable for testing) -----------------

    def _create_client(self, *, cancel_event: threading.Event | None = None) -> Any:
        """Create a Meshtastic interface client based on config.

        Uses the real ``meshtastic`` library interfaces.

        Returns
        -------
        object
            A Meshtastic interface instance.

        Raises
        ------
        MeshtasticConnectionError
            If the client cannot be created.
        """

        def cancellation_kwargs(factory: Callable[..., Any]) -> dict[str, Any]:
            """Opt in only when the SDK declares keyword-capable cancelEvent."""
            if cancel_event is None:
                return {}
            try:
                parameter = inspect.signature(factory).parameters.get("cancelEvent")
            except (TypeError, ValueError):
                return {}
            if parameter is None or parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
                return {}
            return {"cancelEvent": cancel_event}

        try:
            conn = self._config.connection_type
            if conn == "tcp":
                if self._config.host is None:
                    raise RuntimeError("config.host must be set for TCP connection")

                from meshtastic.tcp_interface import TCPInterface

                return TCPInterface(
                    **cancellation_kwargs(TCPInterface),
                    timeout=self._config.sync_timeout_ms / 1000,
                    connectTimeout=min(10.0, self._config.sync_timeout_ms / 1000),
                    hostname=self._config.host,
                    portNumber=(
                        self._config.port if self._config.port is not None else 4403
                    ),
                )
            elif conn == "serial":
                if self._config.serial_port is None:
                    raise RuntimeError(
                        "config.serial_port must be set for serial connection"
                    )

                from meshtastic.serial_interface import SerialInterface

                return SerialInterface(
                    **cancellation_kwargs(SerialInterface),
                    devPath=self._config.serial_port,
                    timeout=self._config.sync_timeout_ms / 1000,
                )
            elif conn == "ble":
                if self._config.ble_address is None:
                    raise RuntimeError(
                        "config.ble_address must be set for BLE connection"
                    )

                from meshtastic.ble_interface import (
                    BLEInterface,  # no py.typed / pyi stubs
                )

                return BLEInterface(
                    **cancellation_kwargs(BLEInterface),
                    address=self._config.ble_address,
                    timeout=self._config.sync_timeout_ms / 1000,
                )
            else:
                raise MeshtasticConnectionError(
                    f"Unsupported connection_type: {conn!r}"
                )
        except (MeshtasticConnectionError, ImportError):
            # Missing transport modules need an installation fix; retrying them
            # as an offline radio would leave a permanently degraded instance.
            raise
        except Exception as exc:
            raise MeshtasticConnectionError(
                f"Failed to create {self._config.connection_type} client: {exc}"
            ) from exc

    # -- Callback subscription ------------------------------------------------

    def _subscribe_callbacks(self) -> None:
        """Subscribe to Meshtastic pubsub callbacks for inbound packets.

        Subscribes to ``meshtastic.receive`` for inbound packets and
        ``meshtastic.connection.lost`` for automatic reconnect triggering.

        Raises
        ------
        MeshtasticConnectionError
            If callback registration fails.
        """
        try:
            from pubsub import pub

            pub.subscribe(self._on_receive, "meshtastic.receive")
        except Exception as exc:
            raise MeshtasticConnectionError(
                f"Failed to subscribe to meshtastic.receive: {exc}"
            ) from exc
        self._subscribed = True

        try:
            from pubsub import pub

            pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        except Exception as exc:
            self._logger.warning(
                "MeshtasticSession %s: failed to subscribe to "
                "meshtastic.connection.lost (reconnect auto-trigger disabled): %s",
                self._adapter_id,
                exc,
            )
            # Non-fatal: receive subscription already succeeded, session
            # is usable.  Reconnect can still be triggered manually via
            # notify_connection_lost().
            return
        self._subscribed_connection_lost = True

    def _unsubscribe_callbacks(self) -> None:
        """Unsubscribe from Meshtastic pubsub callbacks."""
        if self._subscribed:
            try:
                from pubsub import pub

                pub.unsubscribe(self._on_receive, "meshtastic.receive")
            except Exception:
                pass
            self._subscribed = False

        if self._subscribed_connection_lost:
            try:
                from pubsub import pub

                pub.unsubscribe(self._on_connection_lost, "meshtastic.connection.lost")
            except Exception:
                pass
            self._subscribed_connection_lost = False

    def _refresh_node_id(self) -> None:
        """Populate self._node_id from interface.myInfo.myNodeNum when available.

        Called after every successful connect (and re-connect) and lazily
        from ``_on_receive`` when ``_node_id`` is still ``None`` (late
        myInfo).  Safe to call multiple times; refreshes from current
        interface state.
        """
        with self._client_state_lock:
            self._node_id = None
            client = self._client
            if client is None:
                return
            my_info = getattr(client, "myInfo", None)
            node_num = getattr(my_info, "myNodeNum", None)
            if isinstance(node_num, int) and node_num >= 0:
                self._node_id = f"!{node_num:08x}"

    def _on_receive(self, packet: dict[str, Any], interface: Any = None) -> None:
        """Pubsub callback for inbound packets from the active SDK client.

        Late callbacks from a client replaced during reconnect are ignored.
        This matters because pypubsub delivery can race unsubscribe/close and
        a stale interface must never inject packets into the new session.
        """
        # The SDK invokes this callback from its reader thread.  Hold the
        # ownership lock through validation and state mutation only, then
        # dispatch outside it: packet processing runs classification, codec
        # decode, and a run_coroutine_threadsafe hop, and every other lock
        # user — including the event loop via connected()/stop() — would
        # block for that whole duration.  A replacement racing the dispatch
        # is already rejected downstream: the adapter re-reads
        # connection_generation inside the callback and revalidates the
        # session and generation before publishing.
        with self._client_state_lock:
            if self._stop_requested:
                return
            active_client = self._client
            if interface is not None and interface is not active_client:
                self._stale_receive_callbacks += 1
                self._logger.debug(
                    "MeshtasticSession %s ignored packet from stale interface",
                    self._adapter_id,
                )
                return

            self._last_packet_time = time.monotonic()
            if self._node_id is None and active_client is not None:
                self._refresh_node_id()
            callback = self._message_callback

        # Dispatch outside the ownership lock.  The adapter revalidates the
        # captured connection generation before it publishes.
        if callback is not None:
            callback(packet)

    # -- Reconnection ---------------------------------------------------------

    def _on_connection_lost(self, interface: Any = None, **kwargs: Any) -> None:
        """Pubsub callback for ``meshtastic.connection.lost``.

        Called from the Meshtastic SDK reader thread when the transport
        detects a connection loss (e.g. TCP disconnect, serial unplug,
        BLE gatt error).  Delegates to :meth:`notify_connection_lost`
        which handles thread-safe reconnect scheduling.

        Parameters
        ----------
        interface:
            The Meshtastic interface that lost its connection.  Events from
            replaced interfaces are ignored as stale reconnect callbacks.
        **kwargs:
            Additional pubsub keyword arguments (ignored).
        """
        # Guard SDK-reader-thread callbacks with the same ownership lock used
        # for client replacement, then carry the captured generation onto the
        # event loop before scheduling reconnect work.
        with self._client_state_lock:
            if self._stop_requested or not self._started:
                return
            if interface is not None and interface is not self._client:
                self._stale_disconnect_callbacks += 1
                self._logger.debug(
                    "MeshtasticSession %s ignored disconnect from stale interface",
                    self._adapter_id,
                )
                return
            generation = self._connection_generation
        self.notify_connection_lost(expected_generation=generation)

    def notify_connection_lost(
        self,
        *,
        expected_generation: int | None = None,
        reason: str = "Connection lost",
    ) -> None:
        """Called when a connection loss is detected.

        Schedules the lifetime reconnect loop on the session's event loop.
        Thread-safe: may be called from the SDK reader thread or from
        any async context.
        """
        with self._client_state_lock:
            generation = self._connection_generation
            if expected_generation is not None and expected_generation != generation:
                self._stale_disconnect_callbacks += 1
                return
            if self._stop_requested or self._reconnecting:
                return
            self._node_id = None
            self._last_error = reason
            loop = self._loop
        self._logger.warning(
            "MeshtasticSession %s connection lost: %s", self._adapter_id, reason
        )

        # Schedule the reconnect task on the session's event loop.  Pass the
        # generation captured in the SDK thread and revalidate it on the loop so
        # a replacement client cannot inherit an old disconnect notification.
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._start_reconnect_task, generation)
        else:
            self._logger.warning(
                "MeshtasticSession %s: cannot schedule reconnect; "
                "event loop unavailable",
                self._adapter_id,
            )

    def _start_reconnect_task(self, expected_generation: int | None = None) -> None:
        """Create the reconnect task (must run on the event loop thread).

        Separated from :meth:`notify_connection_lost` so that
        ``call_soon_threadsafe`` can schedule just the task creation
        (cheap, non-blocking) while the reconnect loop itself runs as
        an async task.

        Idempotent: if a reconnect task is already running or scheduled,
        the duplicate notification is silently dropped.
        """
        with self._client_state_lock:
            if (
                expected_generation is not None
                and expected_generation != self._connection_generation
            ):
                self._stale_disconnect_callbacks += 1
                return
            if self._stop_requested or self._reconnecting:
                return
            if self._reconnect_task is not None and not self._reconnect_task.done():
                return
            self._reconnecting = True
            self._reconnect_task = asyncio.ensure_future(self._reconnect_loop())

    @property
    def _tcp_liveness_enabled(self) -> bool:
        """Whether active TCP liveness probing is enabled for this session."""
        return (
            self._config.connection_type == "tcp"
            and self._config.tcp_liveness_interval_seconds > 0
        )

    async def _liveness_loop(self) -> None:
        """Periodically prove TCP round-trip liveness and trigger recovery."""
        initial_delay = min(
            _LIVENESS_INITIAL_DELAY_SECONDS,
            self._config.tcp_liveness_interval_seconds,
        )
        try:
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)
            while not self._stop_requested:
                with self._client_state_lock:
                    client = self._client
                    generation = self._connection_generation
                    reconnecting = self._reconnecting
                if client is not None and not reconnecting:
                    try:
                        await self._probe_tcp_liveness(client, generation)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if self.is_connection_generation_current(generation):
                            self._liveness_probe_failures += 1
                            self._liveness_consecutive_failures += 1
                            self._last_liveness_error = str(exc)
                            self.notify_connection_lost(
                                expected_generation=generation,
                                reason=f"TCP liveness probe failed: {exc}",
                            )
                    else:
                        if self.is_connection_generation_current(generation):
                            now = time.monotonic()
                            self._liveness_probe_successes += 1
                            self._liveness_consecutive_failures = 0
                            self._last_liveness_success_time = now
                            self._last_liveness_error = None
                await asyncio.sleep(self._config.tcp_liveness_interval_seconds)
        except asyncio.CancelledError:
            if not self._stop_requested:
                raise

    async def _probe_tcp_liveness(self, client: Any, generation: int) -> None:
        """Issue one bounded local metadata request through the active mtjk client.

        ``isConnected`` and cached node state cannot distinguish a half-open TCP
        connection.  This request requires an ACK/response from the local radio.
        The request callback may run on an SDK thread, so completion is handed
        back to the session event loop thread-safely.  A timed-out response
        handler is explicitly retired through the audited mtjk request-runtime
        seam to avoid accumulating callbacks.
        """
        if not self.is_connection_generation_current(generation):
            return
        from meshtastic.protobuf import admin_pb2

        loop = asyncio.get_running_loop()
        completed: asyncio.Future[None] = loop.create_future()
        self._last_liveness_probe_time = time.monotonic()

        def on_response(_packet: dict[str, Any]) -> None:
            def resolve() -> None:
                if not completed.done():
                    completed.set_result(None)

            try:
                loop.call_soon_threadsafe(resolve)
            except RuntimeError:
                pass

        local_node = getattr(client, "localNode", None)
        send_admin = getattr(local_node, "_send_admin", None)
        if not callable(send_admin):
            raise MeshtasticConnectionError(
                "Pinned mtjk localNode._send_admin liveness seam is unavailable"
            )

        request = admin_pb2.AdminMessage()
        request.get_device_metadata_request = True

        request_id_holder: dict[str, int | None] = {"id": None}
        dropped = False

        def retire_response_handler() -> None:
            """Drop the registered handler exactly once, once we have its id."""
            nonlocal dropped
            request_id = request_id_holder["id"]
            if dropped or not (isinstance(request_id, int) and request_id > 0):
                return
            request_runtime = getattr(client, "_request_wait_runtime", None)
            drop_handler = getattr(request_runtime, "drop_response_handler", None)
            if not callable(drop_handler):
                return
            dropped = True
            try:
                drop_handler(request_id)
            except Exception:
                self._logger.debug(
                    "MeshtasticSession %s failed to retire liveness handler %s",
                    self._adapter_id,
                    request_id,
                    exc_info=True,
                )

        async def _send_request() -> Any:
            # Runs in a worker thread: the synchronous admin send must not
            # stall the event loop for its whole duration.
            packet = await asyncio.to_thread(
                send_admin,
                request,
                wantResponse=True,
                onResponse=on_response,
            )
            if packet is None:
                raise MeshtasticConnectionError(
                    "mtjk did not start the TCP liveness admin request"
                )
            request_id_holder["id"] = getattr(packet, "id", None)
            return packet

        # A shielded worker task: on timeout the send keeps running to
        # completion, and its done callback retires the handler even though
        # this coroutine already returned TimeoutError to the supervisor.
        worker = asyncio.ensure_future(_send_request())
        try:
            # One deadline covers request initiation and response wait: a
            # send that wedges must not defer recovery past the configured
            # liveness timeout.
            async with asyncio.timeout(self._config.tcp_liveness_timeout_seconds):
                # Use mtjk's Node admin transport rather than raw MeshInterface.sendData.
                # The Node seam owns admin-channel selection, PKI encryption, cached
                # session-passkey attachment, and response matching.
                await asyncio.shield(worker)
                # The response handler must stay registered until the
                # response resolves — retiring it here would orphan a
                # healthy in-flight response and force a reconnect on every
                # healthy probe.
                await asyncio.shield(completed)
        finally:
            # Retire after the response resolves, or — when startup or the
            # wait timed out and the worker is still finishing — retire via
            # its done callback so the late packet id is still reaped.
            if worker.done():
                retire_response_handler()
            else:
                worker.add_done_callback(lambda _task: retire_response_handler())

    async def _cancel_startup_attempt(self, pending: asyncio.Task[Any]) -> None:
        """Cancel an attempt and retire a result completed before cancellation.

        An already-completed task cannot be cancelled. Its interface still
        belongs to startup until adoption clears the ownership field.
        """
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        if self._startup_connect_task is not pending:
            return
        self._startup_connect_task = None
        if pending.cancelled():
            return
        try:
            client = pending.result()
        except Exception:
            return
        if client is self._client:
            return
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:
                self._logger.warning(
                    "MeshtasticSession %s failed to close unadopted startup client",
                    self._adapter_id,
                    exc_info=True,
                )

    async def _create_client_offloop(self) -> Any:
        """Create the SDK client in a worker thread, off the event loop.

        The constructor is a blocking SDK call (TCP/serial/BLE connect);
        running it on the loop would stall queue processing, health checks,
        and shutdown for its whole duration — once per lifetime retry.

        If this await is cancelled while a constructor is still running, the
        worker keeps going to completion and any client it produced is
        disposed here instead of being adopted by a session that already
        tore down.
        """
        created: dict[str, Any] = {}
        ownership = threading.Lock()
        abandoned = threading.Event()
        native_done = threading.Event()

        def dispose(client: Any) -> None:
            """Release a native result without depending on an asyncio task."""
            close_fn = getattr(client, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    self._logger.warning(
                        "MeshtasticSession %s failed disposing cancelled connection",
                        self._adapter_id,
                        exc_info=True,
                    )

        def _build() -> Any:
            try:
                create = self._create_client
                try:
                    parameter = inspect.signature(create).parameters.get("cancel_event")
                except (TypeError, ValueError):
                    parameter = None
                if parameter is not None and parameter.kind in (
                    inspect.Parameter.KEYWORD_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    client = create(cancel_event=abandoned)
                else:
                    # Private overrides and injected factories may keep the
                    # legacy signature. Late-result retirement still owns them.
                    client = create()
                with ownership:
                    if not abandoned.is_set():
                        created["client"] = client
                        return client
                # The executor wrapper may already have been cancelled.
                # Disposal must live in the native thread too.
                dispose(client)
                return None
            finally:
                native_done.set()

        worker = asyncio.ensure_future(asyncio.to_thread(_build))
        try:
            # Cancellation must not reach the native worker. Explicit ownership
            # also avoids shield() reporting handled late failures on 3.14.
            await asyncio.wait((worker,))
            return worker.result()
        except asyncio.CancelledError:
            # Atomic with publication in _build: a late client cannot escape
            # both the thread disposer and the wrapper completion callback.
            with ownership:
                abandoned.set()
            if not native_done.is_set():
                self._logger.warning(
                    "MeshtasticSession %s native connection setup is still running; "
                    "process exit may wait for its SDK timeout. Any late client "
                    "will be closed without reactivating the session.",
                    self._adapter_id,
                )

            def _dispose_late(task: asyncio.Future[Any]) -> None:
                """Retire a published result even if loop shutdown cancels its wrapper."""
                try:
                    task.result()
                except (Exception, asyncio.CancelledError):
                    pass
                with ownership:
                    client = created.pop("client", None)
                if client is not None:
                    dispose(client)

            worker.add_done_callback(_dispose_late)
            raise

    def _reconnect_delay(self, attempt: int) -> float:
        """Bound jitter between the configured initial delay and maximum backoff."""
        initial = self._config.reconnect_backoff_initial_seconds
        cap = self._config.reconnect_backoff_max_seconds
        if initial >= cap:
            delay = cap
        else:
            # Once the exponent would exceed the cap there is no value in
            # computing an ever-larger integer/float power.
            max_doublings = max(0, int(math.ceil(math.log2(cap / initial))))
            exponent = min(max(0, attempt - 1), max_doublings)
            delay = min(initial * (2.0**exponent), cap)
        jitter = delay * _BACKOFF_JITTER_FRACTION
        return min(cap, max(initial, delay + random.uniform(-jitter, jitter)))

    async def _reconnect_loop(self, pending: asyncio.Task[Any] | None = None) -> None:
        """Adopt an in-flight startup attempt, then retry with capped backoff.

        A startup deadline does not cancel or duplicate native construction.
        Transport downtime leaves the started session degraded until recovery
        succeeds or shutdown cancels the owner and disposes any late client.
        """
        self._reconnecting = True
        self._reconnect_attempts = 0
        try:
            # Detach the failed client before the first backoff interval.
            # Otherwise ``connected`` can continue to reflect a stale SDK
            # ``isConnected`` event and outbound work may target a client that
            # supervision has already declared unhealthy.
            old_client = self._invalidate_client()
            self._unsubscribe_callbacks()
            if old_client is not None:
                try:
                    close_fn = getattr(old_client, "close", None)
                    if close_fn is not None:
                        close_fn()
                except Exception:
                    self._logger.debug(
                        "MeshtasticSession %s failed to close disconnected client",
                        self._adapter_id,
                        exc_info=True,
                    )

            while not self._stop_requested:
                if pending is None:
                    self._reconnect_attempts += 1
                    self._reconnect_total_attempts += 1
                    actual_delay = self._reconnect_delay(self._reconnect_attempts)
                    self._logger.warning(
                        "MeshtasticSession %s reconnect attempt %d in %.1fs",
                        self._adapter_id,
                        self._reconnect_attempts,
                        actual_delay,
                    )
                    try:
                        await asyncio.sleep(actual_delay)
                    except asyncio.CancelledError:
                        if self._stop_requested:
                            return
                        raise
                if self._stop_requested:
                    return
                try:
                    if pending is not None:
                        initial, pending = pending, None
                        try:
                            new_client = await initial
                        finally:
                            if self._startup_connect_task is initial:
                                self._startup_connect_task = None
                    else:
                        new_client = await self._create_client_offloop()
                    self._activate_client(new_client)
                    self._subscribe_callbacks()
                    self._refresh_node_id()
                    with self._client_state_lock:
                        # Ownership passed to the fresh client. Release the
                        # reconnecting state before this task exits: a
                        # disconnect notification for this generation can
                        # arrive from the SDK reader thread from here until
                        # the finally block, and it must schedule a fresh
                        # loop instead of being dropped as a duplicate — on
                        # serial/BLE no liveness probe would ever re-detect
                        # the loss.
                        self._reconnecting = False
                        if self._reconnect_task is asyncio.current_task():
                            self._reconnect_task = None
                    if self._reconnect_attempts:
                        self._logger.info(
                            "MeshtasticSession %s reconnected after %d "
                            "consecutive attempts",
                            self._adapter_id,
                            self._reconnect_attempts,
                        )
                    else:
                        self._logger.info(
                            "MeshtasticSession %s initial connection became ready",
                            self._adapter_id,
                        )
                    self._reconnect_attempts = 0
                    self._last_error = None
                    return
                except asyncio.CancelledError:
                    if self._stop_requested:
                        return
                    raise
                except Exception as exc:
                    # A replacement may have been activated before callback
                    # subscription or node refresh failed. Retire it now rather
                    # than holding a partial client open through the next
                    # backoff interval.
                    failed_client = self._invalidate_client()
                    self._unsubscribe_callbacks()
                    with self._client_state_lock:
                        # The retry loop stays live: re-arm the state that the
                        # activation window released so liveness and notify
                        # paths keep treating recovery as in progress.
                        self._reconnecting = True
                    if failed_client is not None:
                        try:
                            close_fn = getattr(failed_client, "close", None)
                            if close_fn is not None:
                                close_fn()
                        except Exception:
                            self._logger.debug(
                                "MeshtasticSession %s failed to close partial "
                                "reconnect client",
                                self._adapter_id,
                                exc_info=True,
                            )
                    self._last_error = f"Reconnect failed: {exc}"
                    self._logger.warning(
                        "MeshtasticSession %s reconnect attempt %d failed: %s",
                        self._adapter_id,
                        self._reconnect_attempts,
                        exc,
                    )
        except asyncio.CancelledError:
            if not self._stop_requested:
                raise
        finally:
            self._reconnecting = False

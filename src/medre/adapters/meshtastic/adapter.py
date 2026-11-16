"""Meshtastic transport adapter for the MEDRE framework.

:class:`MeshtasticAdapter` connects to a Meshtastic radio node and bridges
inbound radio packets into the MEDRE canonical event stream and outbound
rendered payloads back to the radio mesh.

**Soft dependency**: all ``meshtastic`` imports are guarded behind
:mod:`~medre.adapters.meshtastic.compat`.  If ``mtjk`` is not installed
the adapter raises :class:`~medre.adapters.meshtastic.errors.MeshtasticConnectionError`
on :meth:`start` when using non-fake connection types.

Connection modes
----------------
The adapter supports four connection types configured via
:class:`~medre.config.adapters.meshtastic.MeshtasticConfig`:

``"fake"``
    No real client.  Used for testing without hardware.  Inbound
    simulation via :meth:`simulate_inbound`; outbound via :meth:`deliver`
    accepts work into the internal queue and returns a deferred hand-off
    fact.  No real radio transmission occurs.

``"tcp"``
    Connects via TCP using ``meshtastic.tcp_interface.TCPInterface(hostname, portNumber)``.

``"serial"``
    Connects via serial using ``meshtastic.serial_interface.SerialInterface(devPath)``.

``"ble"``
    Connects via BLE using ``meshtastic.ble_interface.BLEInterface(address)``.

All non-fake modes require the ``mtjk`` package.  Client creation is
delegated to the :class:`~medre.adapters.meshtastic.session.MeshtasticSession`,
which can be overridden in tests or monkeypatched with fake modules.

Lifecycle
---------
:meth:`start` and :meth:`stop` are idempotent — calling them multiple
times is safe.  The adapter tracks background :class:`asyncio.Task`
instances spawned by inbound packet callbacks and drains them on stop.

**Inbound-path lifecycle guard.** The SDK reader thread calls
:meth:`_on_packet` which schedules an async publish via
``asyncio.run_coroutine_threadsafe``.  The resulting
``concurrent.futures.Future`` is tracked in ``_inbound_futures``.
On :meth:`stop`, ``_started`` is cleared *before* draining, causing
:meth:`_on_packet` to reject late packets and :meth:`_on_packet_async`
to skip publication against a torn-down session.  Remaining inbound
futures are cancelled during :meth:`_drain_background_tasks`.

**Bounded loss-honest staging.** Relayable ingress is bounded *before*
scheduling: a non-blocking reservation from a finite per-session pool
must be acquired or the packet is refused — never scheduled, never
admitted, never deduplicated — and the loss is reported as
``producer_overflow`` diagnostics plus an ``overflow``
:class:`~medre.core.contracts.adapter.ProducerPressureReport` with
reason ``callback_capacity`` through the optional
``report_producer_pressure`` sink. Staged and cancelled-before-admission
work is reported through the same sink. ACK/NAK and routing-correlation
callbacks are never throttled.

Session boundary
----------------
The adapter delegates raw transport lifecycle to
:class:`~medre.adapters.meshtastic.session.MeshtasticSession`.
The session owns the raw client; the adapter owns semantic conversion
(classification, codec, event publishing).

Inbound evidence
----------------
The adapter maintains counters for every classification action (relay,
ignore, drop, deferred) and sub-counters for specific reasons (encrypted,
detection sensor, DM, empty text, unknown portnum, malformed).  These
are exposed via :meth:`diagnostics` and provide inbound evidence without
external dependencies.

Native routing results (ROUTING_APP ACK/NAK packets) additionally pass
through :meth:`_observe_native_routing_packet`, which pairs them against
bounded, session-fenced facts recorded for accepted sends.  The pairing
is classification only: with the supported send path never requesting
``want_ack``, no routing result reachable from this adapter qualifies as
``remote_service`` or ``end_to_end`` delivery evidence, so nothing is
ever submitted to core as a delivery observation.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from medre.core.events.canonical import CanonicalEvent

from medre.core.contracts.adapter import (
    AdapterCapabilities,
    AdapterContext,
    AdapterContract,
    AdapterInfo,
    AdapterPermanentError,
    AdapterRole,
    AdapterSendError,
)
from medre.core.contracts.delivery import (
    AdapterHandoffResult,
    DeferredHandoffCompleted,
    DeferredHandoffFailed,
)
from medre.core.events.delivery import DeliveryAttemptProvenance
from medre.core.policies.startup_backlog_suppress import (
    should_suppress_startup_backlog,
)
from medre.core.rendering.renderer import RenderingResult
from medre.core.supervision.diagnostic_contract import QUEUE_PENDING

from medre.adapters.meshtastic.codec import MeshtasticCodec
from medre.adapters.meshtastic.errors import (
    MeshtasticSendError,
)
from medre.adapters.meshtastic.event_shape import MESHTASTIC_NATIVE_SCHEMA_VERSION
from medre.adapters.meshtastic.guarded import GuardedQueuedSend
from medre.adapters.meshtastic.packet_classifier import (
    REASON_DETECTION_SENSOR,
    REASON_DIRECT_MESSAGE,
    REASON_EMPTY_TEXT,
    REASON_ENCRYPTED,
    REASON_MALFORMED,
    REASON_SELF_ECHO,
    REASON_UNKNOWN_PORTNUM,
    ClassificationResult,
    MeshtasticPacketClassifier,
)
from medre.adapters.meshtastic.queue import (
    MeshtasticOutboundQueue,
    QueueDeliveryResult,
    QueueTerminalResult,
)
from medre.adapters.meshtastic.routing_observations import (
    normalize_native_routing_result,
)
from medre.adapters.meshtastic.send_correlation import (
    AcceptedSendFacts,
    MeshtasticSendCorrelation,
)
from medre.adapters.meshtastic.session import MeshtasticSession
from medre.adapters.meshtastic.startup_backlog import extract_meshtastic_rx_time
from medre.config.adapters.meshtastic import MeshtasticConfig

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    ProducerPressureReport = None  # type: ignore[assignment,misc]


# Length of the generation-scoped monotonic bootstrap window armed on every
# receive-ready installation after the first.  Behavioral reference:
# mmrelay's ``RECONNECT_PRESTART_BOOTSTRAP_WINDOW_SECS``.  Unlike the
# content-independent startup drain, the window only drops packets that
# cannot prove their age (no usable ``rxTime``); provably fresh traffic
# relays immediately, so a reconnect never spends a blind drain on live
# packets.  Not operator-configurable: it shares the
# ``startup_packet_drain_seconds`` opt-out with the startup drain, which
# disables the connect-window drop family entirely when set to ``0``.
_RECONNECT_RECEIVE_BOOTSTRAP_SECONDS: float = 5.0

# Bounded retention of per-generation receive-ready watermarks.  Only the
# current generation can pass the ownership fence, so retired watermarks
# exist solely to keep in-flight gate evaluations coherent across the
# fence-to-gate race of a reconnect; a small history covers that race
# without unbounded growth on a flapping link.
_RECEIVE_READY_EPOCH_HISTORY: int = 8


def _is_native_routing_callback(classification: ClassificationResult) -> bool:
    """Return whether one classified inbound packet is a native routing callback.

    ROUTING_APP packets classify under the ``routing`` portnum; the
    classifier may also mark plain acknowledgment shapes with the
    ``ack`` category. Both shapes are offered to send correlation
    before the relay gate discards them from the publish path.
    """
    return classification.portnum == "routing" or classification.category == "ack"


class _IngressStagingReservations:
    """Thread-safe, non-blocking staging slots for reader-thread ingress.

    The Meshtastic SDK delivers packets on its own reader thread. That
    thread must never park waiting for a staging slot, so acquisition is
    a non-blocking semaphore take under a short lock; a refused acquire
    becomes an explicit, counted overflow instead of a blocked callback.

    A generation sync keeps reservations session-exact: when the caller
    observes a connection generation newer than the pool's, the pool is
    replaced before the acquire, so a reconnected session never inherits
    stale reservations. Releases captured from a retired pool land in
    that retired pool only and can never over-grant the fresh one.
    """

    __slots__ = ("_capacity", "_generation", "_lock", "_slots")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._generation: int | None = None
        self._lock = threading.Lock()
        self._slots = threading.Semaphore(capacity)

    @property
    def capacity(self) -> int:
        """Configured slot count; the pool is always finite."""
        return self._capacity

    def acquire(self, generation: int | None) -> Callable[[], None] | None:
        """Take one staging slot without blocking.

        Returns a one-shot releaser bound to the exact pool that issued
        the slot, or ``None`` when full or called by a retired generation.
        A newer generation resets the pool; an older generation cannot
        replenish current capacity.
        """
        with self._lock:
            if self._generation is not None and (
                generation is None or generation < self._generation
            ):
                return None
            if generation != self._generation:
                self._generation = generation
                self._slots = threading.Semaphore(self._capacity)
            if not self._slots.acquire(blocking=False):
                return None
            slots = self._slots
        return slots.release


class MeshtasticAdapter(AdapterContract):
    """Transport adapter for Meshtastic radio nodes.

    Connects to a Meshtastic node, receives radio packets, and publishes
    them as canonical events.  Outbound rendered payloads are enqueued
    for paced delivery.

    Delegates raw transport lifecycle to
    :class:`~medre.adapters.meshtastic.session.MeshtasticSession`.
    The session owns the raw client; the adapter owns semantic conversion
    (classification, codec, event publishing).

    **Inbound-path lifecycle guard.**  ``run_coroutine_threadsafe``
    Futures are tracked in ``_inbound_futures`` and cancelled on stop.
    The ``_started`` flag gates both :meth:`_on_packet` (sync, SDK
    thread) and :meth:`_on_packet_async` (async, event loop) to
    prevent late-packet processing after session teardown.  Staging is
    bounded by a finite per-session reservation pool
    (:class:`_IngressStagingReservations`); refusals are explicit,
    counted loss reported through the optional producer-pressure sink.

    Parameters
    ----------
    config:
        Validated :class:`~medre.config.adapters.meshtastic.MeshtasticConfig`.
    """

    adapter_id: str
    platform: str = "meshtastic"
    supports_radio_multipart: bool = True
    role: AdapterRole = AdapterRole.TRANSPORT

    def __init__(self, config: MeshtasticConfig) -> None:
        super().__init__()
        config.validate()
        self._config = config
        self.adapter_id = config.adapter_id
        self._capabilities = AdapterCapabilities(
            text=True,
            title=False,
            replies="native",
            threads="fallback",
            reactions="native",
            edits="unsupported",
            deletes="unsupported",
            attachments=False,
            metadata_fields=True,
            store_and_forward=False,
            direct_messages=False,
            channels=True,
            mesh_routing=True,
            max_text_chars=None,
            max_text_bytes=config.max_text_bytes,
        )
        self._session: MeshtasticSession | None = None
        self._codec = MeshtasticCodec(config.adapter_id, config)
        self._classifier = MeshtasticPacketClassifier(config)
        self._health_epoch = 0
        self._reported_health: str | None = None
        self._queue = MeshtasticOutboundQueue(
            delay_between_messages=config.message_delay_seconds,
            max_queue_size=config.queue_max_size,
            max_attempts=config.queue_send_max_attempts,
            warning_threshold_pct=config.queue_warning_threshold_pct,
            critical_threshold_pct=config.queue_critical_threshold_pct,
            on_pressure_change=lambda: self._report_native_health(self._health_epoch),
        )
        self._guarded_sends: set[GuardedQueuedSend] = set()
        self.ctx: AdapterContext | None = None
        self._started: bool = False
        # Bounded, session-fenced correlation of accepted native sends to
        # later routing callbacks. Classification only: it never emits a
        # core observation, because no native routing result reachable from
        # this adapter's supported send path qualifies as delivered evidence.
        self._send_correlation = self._new_send_correlation()
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._inbound_futures: set[concurrent.futures.Future[object]] = set()
        self._inbound_admission_started: dict[
            concurrent.futures.Future[object], threading.Event
        ] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._drain_task: asyncio.Task[Any] | None = None
        # Bounded reader-thread staging: slots held by staged-but-unfinished
        # inbound publications. Installed at construction and replaced by
        # every start(); see _IngressStagingReservations for the
        # session-generation contract.
        self._ingress_reservations = _IngressStagingReservations(
            config.max_pending_ingress_callbacks
        )

        # Classifier counters — inbound evidence
        self._classifier_packets_seen: int = 0
        self._classifier_packets_relayed: int = 0
        self._classifier_packets_ignored: int = 0
        self._classifier_packets_dropped: int = 0
        self._classifier_packets_deferred: int = 0
        self._classifier_packets_malformed: int = 0
        self._classifier_packets_encrypted_dropped: int = 0
        self._classifier_packets_detection_sensor_deferred: int = 0
        self._classifier_packets_dm_ignored: int = 0
        self._classifier_packets_empty_text_ignored: int = 0
        self._classifier_packets_unknown_portnum_deferred: int = 0
        self._classifier_packets_self_echo_ignored: int = 0
        self._inbound_published: int = 0

        # Producer-side pressure evidence — explicit, counted staging loss.
        # Cumulative across sessions, like the classifier counters.
        self._producer_overflow: int = 0
        self._last_overflow_warning_at: float = float("-inf")

        # Startup backlog suppression
        self._adapter_start_epoch: float | None = None
        self._startup_backlog_packets_seen: int = 0
        self._startup_backlog_packets_suppressed: int = 0

        # Startup packet drain — content-independent drop window armed at
        # first receive-ready connection; see _check_startup_packet_drain.
        # The same lock guards the reconnect receive-watermark state: both
        # are connect-window baselines armed from session installation
        # hooks and evaluated from two ingress paths.
        self._startup_drain_lock = threading.Lock()
        self._startup_drain_deadline_monotonic: float | None = None
        self._startup_drain_packets_dropped: int = 0

        # Reconnect receive watermark — wall-clock receive-ready epoch per
        # connection generation (see _record_connection_receive_ready) and
        # the generation-scoped monotonic bootstrap window that drops
        # packets unable to prove their age right after a reconnect; see
        # _check_reconnect_receive_bootstrap.  The window state carries its
        # generation, monotonic deadline, and the drops counted within this
        # window (for the single completion summary line).
        self._receive_ready_epochs: dict[int, float] = {}
        self._reconnect_bootstrap: tuple[int, float, int] | None = None
        self._reconnect_backlog_packets_dropped: int = 0

        # Outbound gate suppression counter
        self._outbound_gate_suppressed: int = 0

        # Cached health string from last health_check() call.
        self._last_health: str | None = None

    # -- Lifecycle ----------------------------------------------------------

    def _new_send_correlation(self) -> MeshtasticSendCorrelation:
        """Build a correlation registry sized from the outbound queue config.

        ``start()`` calls this on every (re)start: a stopped adapter's
        registry is closed by ``stop()``, and a restart must get a fresh
        one so accepted sends correlate again instead of every fact
        landing in the after-close counter.
        """
        return MeshtasticSendCorrelation(
            capacity=(
                self._config.queue_max_size
                if isinstance(self._config.queue_max_size, int)
                and self._config.queue_max_size > 0
                else 1024
            ),
            retention_seconds=self._config.reconnect_backoff_max_seconds,
        )

    async def start(self, ctx: AdapterContext) -> None:
        """Connect to the Meshtastic node and begin receiving packets.

        Idempotent: calling start on an already-started adapter is a no-op.
        An unavailable node leaves the started adapter degraded while the
        session retains connection recovery in the background.

        Parameters
        ----------
        ctx:
            Runtime context supplied by the framework.

        Raises
        ------
        MeshtasticConnectionError
            If ``mtjk`` is not installed and connection_type is not ``"fake"``.
        """
        if self._started:
            return

        # A stop() closed the previous correlation registry; this start
        # installs a fresh one so the restart correlates again.
        self._send_correlation = self._new_send_correlation()

        # Fresh staging capacity for the new session: a restart never
        # inherits reservations from the previous session's staged work.
        # Construction installed a pool too; start() replaces it so the
        # new session's generation owns full, private capacity.
        self._ingress_reservations = _IngressStagingReservations(
            self._config.max_pending_ingress_callbacks
        )
        self._last_overflow_warning_at = float("-inf")

        # Clear cached health at lifecycle boundary so diagnostics
        # never reports a stale health string from a previous session.
        self._last_health = None
        self._health_epoch += 1
        self._reported_health = None
        health_epoch = self._health_epoch

        self.ctx = ctx

        # A degraded start may return long before the radio connects. Reset
        # evidence now, but let the session arm both gates before it subscribes
        # to the first client's receive callbacks.
        self._adapter_start_epoch = None
        self._startup_backlog_packets_seen = 0
        self._startup_backlog_packets_suppressed = 0
        with self._startup_drain_lock:
            self._startup_drain_deadline_monotonic = None
            self._startup_drain_packets_dropped = 0
            self._receive_ready_epochs = {}
            self._reconnect_bootstrap = None
            self._reconnect_backlog_packets_dropped = 0

        # Create session and delegate lifecycle
        session = MeshtasticSession(
            config=self._config,
            adapter_id=self.adapter_id,
            platform=self.platform,
            logger=ctx.logger if ctx.logger else None,
            on_health_change=lambda: self._report_native_health(health_epoch),
            on_initial_connection=lambda: self._arm_startup_backlog(health_epoch),
            on_connection_ready=lambda generation, initial: (
                self._record_connection_receive_ready(
                    health_epoch, generation, initial=initial
                )
            ),
        )
        self._session = session

        # Capture the owning session here and the generation in its receive
        # callback, not after dispatch has crossed the session lock.
        def on_packet(packet: dict[str, Any], generation: int) -> None:
            self._on_packet(
                packet, source_session=session, source_generation=generation
            )

        try:
            await session.start(generation_callback=on_packet)
        except asyncio.CancelledError:
            # Clear adapter-owned fields synchronously — no await.
            self._session = None
            self.ctx = None
            self._start_time = None
            self._adapter_start_epoch = None
            self._startup_drain_deadline_monotonic = None
            self._reconnect_bootstrap = None
            raise
        except Exception:
            # Best-effort cleanup of partially-started session.
            try:
                if self._session is not None:
                    await self._session.stop(timeout=5.0)
            except Exception:
                pass
            self._session = None
            self.ctx = None
            self._start_time = None
            self._adapter_start_epoch = None
            self._startup_drain_deadline_monotonic = None
            self._reconnect_bootstrap = None
            raise

        self._loop = asyncio.get_running_loop()
        self._drain_task = asyncio.create_task(self._process_queue())

        # Record start time only after session and infrastructure are up.
        self._mark_started(ctx)
        self._started = True
        self._report_native_health(health_epoch)
        ctx.logger.info(
            "MeshtasticAdapter %s started (mode=%s)",
            self.adapter_id,
            self._config.connection_type,
        )

    async def stop(self, timeout: float = 5.0) -> None:
        """Disconnect from the Meshtastic node.

        Idempotent: calling stop on an already-stopped adapter is a no-op.
        Cancels all tracked background tasks and inbound futures before
        shutting down.

        The ``_started`` flag is cleared *before* draining so that the
        inbound-path guard in :meth:`_on_packet` rejects any late packets
        arriving from the SDK reader thread during teardown.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for a clean shutdown.
        """
        if not self._started:
            return

        # Gate new inbound scheduling *before* draining.  The SDK reader
        # thread may still call _on_packet during teardown; this flag
        # causes _on_packet to return early, preventing new
        # run_coroutine_threadsafe submissions.
        self._started = False
        self._start_time = None
        self._health_epoch += 1
        self._reported_health = None

        # Clear cached health at lifecycle boundary.
        self._last_health = None

        for guarded in tuple(self._guarded_sends):
            guarded.fail("adapter stopped before guarded queue handoff completed")

        # Cancel the queue drain background task.
        if self._drain_task is not None:
            self._drain_task.cancel()
            try:
                await asyncio.wait_for(self._drain_task, timeout=timeout)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._drain_task = None

        # Cancel all tracked background tasks and drain inbound futures.
        await self._drain_background_tasks(timeout)

        # Delegate stop to session
        if self._session is not None:
            await self._session.stop(timeout=timeout)

        self._session = None
        # Retained native ids never correlate across a stop: delayed
        # results after shutdown are counted and dropped, never guessed.
        self._send_correlation.clear()
        if self.ctx is not None:
            self.ctx.logger.info("MeshtasticAdapter %s stopped", self.adapter_id)

    async def health_check(self) -> AdapterInfo:
        """Return a snapshot of the adapter's current health.

        Composes health from session diagnostics when available.

        Returns
        -------
        AdapterInfo
            Metadata describing the adapter's state.
        """
        info = self._health_info()
        if (
            self._started
            and self._session is not None
            and self._config.connection_type != "fake"
            and not self._session.connected
            and not self._session.reconnecting
        ):
            # Preserve the explicit probe's backup recovery trigger. Native
            # diagnostic observers do not schedule transport recovery.
            self._session.notify_connection_lost()
        return info

    def _report_native_health(self, epoch: int) -> None:
        """Publish session and queue readiness without probing or owning recovery."""
        if epoch != self._health_epoch or not self._started or self.ctx is None:
            return
        info = self._health_info()
        report = getattr(self.ctx, "report_health", None)
        if report is None or self._reported_health == info.health:
            return
        try:
            report(info)
            self._reported_health = info.health
        except Exception as exc:
            self.ctx.logger.warning(
                "Meshtastic health notification failed (%s)", type(exc).__name__
            )

    def _health_info(self) -> AdapterInfo:
        """Compose the same settled connection and pressure state for probes and reports."""
        if self._started and self._session is not None:
            if self._session.connected or self._config.connection_type == "fake":
                health = (
                    "degraded"
                    if self._queue.pressure_state in ("critical", "full")
                    else "healthy"
                )
            elif self._session.reconnecting:
                health = "degraded"
            else:
                # The SDK can clear isConnected without a pubsub callback.
                # Report degradation; an explicit probe retains the existing
                # backup recovery trigger outside this observation projection.
                health = "degraded"
        elif self._session is not None and not self._started:
            # Session exists but start did not complete — subscription failure.
            health = "failed"
        else:
            health = "unknown"
        self._last_health = health
        return AdapterInfo(
            adapter_id=self.adapter_id,
            platform=self.platform,
            role=self.role,
            version="0.1.0",
            capabilities=self._capabilities,
            health=health,
        )

    # -- Outbound delivery --------------------------------------------------

    async def deliver(self, result: RenderingResult) -> AdapterHandoffResult:
        """Deliver through the paced queue, retaining core ownership for planned packets.

        A core-owned ``before_native_send`` callback selects synchronous packet
        handoff: this method waits for the SDK result while the parent remains
        in progress. Individual planned packets emit no deferred parent feedback.
        Ordinary messages retain their existing deferred admission contract.

        The *result.payload* is expected to be a Meshtastic-ready content
        dict already rendered by
        :class:`~medre.adapters.meshtastic.renderer.MeshtasticRenderer`.

        The payload is accepted into the local queue and the returned hand-off
        result has ``disposition="deferred"``.  Later transport completion or
        failure is reported through the unified delivery-feedback callback.

        .. note::

           Deferred admission requires immutable attempt provenance. Direct
           calls that do not carry ``RenderingResult.attempt_provenance`` are
           rejected before the local queue is mutated; otherwise the later
           completion/failure could not be correlated to durable lifecycle
           authority.

        Parameters
        ----------
        result:
            The rendered payload to deliver.  Must be a
            :class:`RenderingResult`, **not** a :class:`CanonicalEvent`.

        Returns
        -------
        AdapterHandoffResult
            Local queue admission for ordinary messages, or synchronous native
            handoff for a guarded planned packet.

        Raises
        ------
        AdapterPermanentError
            If a permanent error occurs (invalid input type, adapter not
            started, payload encoding failure).
        AdapterSendError
            If a transient error occurs (timeout, connection, transport).
            ``transient`` is ``True``.
        asyncio.CancelledError
            Propagates without swallowing task cancellation.
        """
        if not isinstance(result, RenderingResult):
            raise AdapterPermanentError(
                f"MeshtasticAdapter.deliver() accepts RenderingResult only, "
                f"got {type(result).__name__}. Use simulate_inbound() for "
                f"the inbound path."
            )

        if result.radio_plan is not None:
            raise AdapterPermanentError(
                "Meshtastic radio plans require core-owned packet dispatch"
            )

        # Real transports require an active startup lifecycle. Fake mode has no
        # hardware-session requirement, but deferred admission still requires
        # an installed feedback context below.
        if not self._started and self._config.connection_type != "fake":
            raise AdapterPermanentError("Adapter not started")

        # Outbound gate: suppress radio sends when listen_only.
        if self._config.outbound_mode == "listen_only":
            self._outbound_gate_suppressed += 1
            raise AdapterPermanentError("outbound suppressed: listen_only mode")

        # Meshtastic hand-off is deferred: once work is admitted to the local
        # queue, completion/failure is reported asynchronously.  Accepting
        # that work without durable attempt provenance would create an
        # uncorrelatable queue item that core can never finalize safely.
        if result.attempt_provenance is None:
            raise AdapterPermanentError(
                "deferred Meshtastic delivery requires immutable " "attempt_provenance"
            )
        if result.before_native_send is None and (
            self.ctx is None or self.ctx.report_delivery_feedback is None
        ):
            raise AdapterPermanentError(
                "deferred Meshtastic delivery requires an installed "
                "report_delivery_feedback sink"
            )

        payload = dict(result.payload)
        channel_index = payload.get("channel_index", self._config.default_channel)
        if not isinstance(channel_index, int):
            channel_index = self._config.default_channel

        if result.before_native_send is not None:
            raw_channel = payload.get("channel_index")
            if type(raw_channel) is not int or raw_channel < 0:
                raise AdapterPermanentError(
                    "guarded queue channel must be a nonnegative integer"
                )
            session = self._session
            if session is None or not session.connected:
                raise AdapterSendError(
                    "guarded queue handoff requires a connected session", transient=True
                )
            guarded = GuardedQueuedSend(result.before_native_send)
            self._guarded_sends.add(guarded)
            try:
                await self._queue.enqueue(
                    payload,
                    channel_index,
                    attempt_provenance=result.attempt_provenance,
                    guarded_send=guarded,
                )
                return await guarded.completion
            except MeshtasticSendError as exc:
                if exc.transient:
                    raise AdapterSendError(str(exc), transient=True) from exc
                raise AdapterPermanentError(str(exc)) from exc
            finally:
                guarded.close()
                self._queue.discard_guarded(guarded)
                self._guarded_sends.discard(guarded)

        try:
            await self._queue.enqueue(
                payload,
                channel_index,
                event_id=result.event_id,
                delivery_plan_id=result.delivery_plan_id,
                outbox_id=result.outbox_id,
                attempt_number=result.attempt_number,
                attempt_provenance=result.attempt_provenance,
            )
        except asyncio.CancelledError:
            raise
        except MeshtasticSendError as exc:
            if exc.transient:
                raise AdapterSendError(str(exc), transient=True) from exc
            else:
                raise AdapterPermanentError(str(exc)) from exc
        except (TimeoutError, ConnectionError, OSError) as exc:
            raise AdapterSendError(str(exc), transient=True) from exc

        # Queue admission succeeded locally. Actual transport hand-off is
        # asynchronous via queue.process_one(), so no native message ID is
        # available yet. The closed ``deferred`` disposition tells core to
        # record queued attempt evidence; later feedback finalizes the hand-off.
        return AdapterHandoffResult(
            native_message_id=None,
            native_channel_id=str(channel_index),
            disposition="deferred",
            note="locally enqueued",
            confirmation_level="local_queue",
            metadata={
                "meshtastic": {
                    "schema_version": MESHTASTIC_NATIVE_SCHEMA_VERSION,
                    "channel_index": channel_index,
                }
            },
        )

    # -- Inbound callback ---------------------------------------------------

    def _increment_classifier_counters(self, classification: Any) -> None:
        """Increment classifier counters based on a ClassificationResult.

        Parameters
        ----------
        classification:
            A :class:`~medre.adapters.meshtastic.packet_classifier.ClassificationResult`.
        """
        self._classifier_packets_seen += 1

        action = classification.action

        # Action-level counter
        if action == "relay":
            self._classifier_packets_relayed += 1
        elif action == "ignore":
            self._classifier_packets_ignored += 1
        elif action == "drop":
            self._classifier_packets_dropped += 1
        elif action == "deferred":
            self._classifier_packets_deferred += 1

        # Reason-level sub-counters (exact match)
        if classification.reason == REASON_MALFORMED:
            self._classifier_packets_malformed += 1
        if classification.reason == REASON_ENCRYPTED:
            self._classifier_packets_encrypted_dropped += 1
        if classification.reason == REASON_DETECTION_SENSOR:
            self._classifier_packets_detection_sensor_deferred += 1
        if classification.reason == REASON_DIRECT_MESSAGE:
            self._classifier_packets_dm_ignored += 1
        if classification.reason == REASON_EMPTY_TEXT:
            self._classifier_packets_empty_text_ignored += 1
        if classification.reason == REASON_UNKNOWN_PORTNUM:
            self._classifier_packets_unknown_portnum_deferred += 1
        if classification.reason == REASON_SELF_ECHO:
            self._classifier_packets_self_echo_ignored += 1

    def _log_classification(self, classification: Any) -> None:
        """Log a structured classification decision.

        Parameters
        ----------
        classification:
            A :class:`~medre.adapters.meshtastic.packet_classifier.ClassificationResult`.
        """
        if self.ctx is None:
            return
        _logger = self.ctx.logger
        action = classification.action
        reason = classification.reason

        if action == "drop":
            _logger.debug(
                "MeshtasticAdapter %s: packet dropped reason=%s portnum=%s from_id=%s",
                self.adapter_id,
                reason,
                classification.portnum,
                classification.from_id,
            )
        elif action == "ignore":
            _logger.debug(
                "MeshtasticAdapter %s: packet ignored reason=%s category=%s portnum=%s",
                self.adapter_id,
                reason,
                classification.category,
                classification.portnum,
            )
        elif action == "deferred":
            _logger.debug(
                "MeshtasticAdapter %s: packet deferred reason=%s portnum=%s packet_id=%s",
                self.adapter_id,
                reason,
                classification.portnum,
                classification.packet_id,
            )
        elif action == "relay":
            # Log eligibility after startup gates, not delivery confirmation.
            _logger.info(
                "MeshtasticAdapter %s: packet relayed packet_id=%s from_id=%s",
                self.adapter_id,
                classification.packet_id,
                classification.from_id,
            )

    def _enrich_with_node_info(self, packet: dict[str, Any]) -> dict[str, str] | None:
        """Look up node info (longname/shortname) from the session node database.

        Reads ``fromId`` directly from the raw packet dict so we never
        need a preliminary codec decode just to obtain the sender ID.

        Returns ``None`` when the session is unavailable, the node ID is
        empty, the node is unknown, or the lookup raises.

        Parameters
        ----------
        packet:
            Raw Meshtastic packet dict.

        Returns
        -------
        dict[str, str] | None
            ``{"longname": ..., "shortname": ...}`` or ``None``.
        """
        from_id = str(packet.get("fromId", "") or "")
        if not from_id or self._session is None:
            return None
        try:
            return self._session.get_node_info(from_id)
        except Exception:
            return None  # Non-critical enrichment; names are best-effort

    def _arm_startup_backlog(self, epoch: int) -> None:
        """Set both baselines before the initial native receive subscription.

        A late hook from a retired adapter lifecycle must not re-arm it.
        Failed initial subscription attempts may re-arm; counters are reset
        only at adapter start so already recorded drops are not lost.
        """
        if epoch != self._health_epoch or self.ctx is None:
            return
        self._adapter_start_epoch = time.time()
        window = float(self._config.startup_packet_drain_seconds)
        with self._startup_drain_lock:
            self._startup_drain_deadline_monotonic = (
                time.monotonic() + window if window > 0 else None
            )

    def _record_connection_receive_ready(
        self, epoch: int, generation: int, *, initial: bool
    ) -> None:
        """Record the receive-ready watermark of one connection generation.

        Invoked by the session under its client-state lock on each client
        activation attempt, before receive callbacks of the new client can
        subscribe or dispatch. The wall-clock epoch is the freshness baseline
        the ``rxTime`` gate judges this generation's packets against; see :meth:`_check_startup_backlog_suppress`.

        A late hook from a retired adapter lifecycle must not record a
        watermark. Installations after successful initial setup retire the
        initial drain and arm the generation-scoped reconnect bootstrap window
        (monotonic), unless the operator disabled the connect-window drop family with
        ``startup_packet_drain_seconds == 0``.
        """
        if epoch != self._health_epoch or self.ctx is None:
            return
        wall_epoch = time.time()
        with self._startup_drain_lock:
            self._receive_ready_epochs[generation] = wall_epoch
            if len(self._receive_ready_epochs) > _RECEIVE_READY_EPOCH_HISTORY:
                for stale in sorted(self._receive_ready_epochs)[
                    :-_RECEIVE_READY_EPOCH_HISTORY
                ]:
                    del self._receive_ready_epochs[stale]
            if initial:
                # A failed initial subscription is still initial setup; the
                # initial hook re-arms its drain. Do not stack a second gate.
                self._reconnect_bootstrap = None
            else:
                # A replacement generation owns its own ingress policy. Even
                # a rapid reconnect must not inherit the blind startup drain.
                self._startup_drain_deadline_monotonic = None
                self._reconnect_bootstrap = (
                    (
                        generation,
                        time.monotonic() + _RECONNECT_RECEIVE_BOOTSTRAP_SECONDS,
                        0,
                    )
                    if self._config.startup_packet_drain_seconds > 0
                    else None
                )

    def _receive_ready_epoch(self, generation: int | None) -> float | None:
        """Return the receive-ready watermark for *generation*, if recorded.

        Unknown generations (direct test injection, fake mode, generations
        older than the retained history) fall back to the initial
        receive-ready baseline so the ``rxTime`` gate never widens beyond
        its original behavior.
        """
        with self._startup_drain_lock:
            recorded = (
                self._receive_ready_epochs.get(generation)
                if generation is not None
                else None
            )
        if recorded is not None:
            return recorded
        return self._adapter_start_epoch

    def _check_reconnect_receive_bootstrap(
        self,
        packet: dict[str, Any],
        connection_generation: int | None,
        packet_id: Any,
    ) -> bool:
        """Check whether an age-unprovable packet falls inside the reconnect bootstrap window.

        Every receive-ready installation after successful initial setup
        (a transparent reconnect) arms a short monotonic window scoped to its
        connection generation. While the window is open, a relay-classified
        packet on that generation without a usable ``rxTime`` is dropped: the radio
        may be emptying its FROMRADIO queue of pre-reconnect backlog and a
        packet without a timestamp cannot prove it is not part of that
        backlog.  Packets WITH a usable ``rxTime`` are never dropped by
        this window — their age is judged exclusively by the wall-clock
        ``rxTime`` gate, so provably fresh traffic relays immediately and a
        reconnect never spends a blind content-independent drain on live
        packets.  After lazy expiry the window stays closed for the rest of
        the generation; missing or malformed timestamps then relay as they
        always have, because their age cannot be judged.

        Disabled entirely when ``startup_packet_drain_seconds == 0`` — the
        operator opt-out that disables the startup drain also disables this
        reconnect window, keeping the connect-window drop family on one
        switch.

        Returns
        -------
        bool
            ``True`` when the packet must be dropped (unprovable age inside
            the live window); ``False`` otherwise.
        """
        # A usable timestamp can settle an expired window, but never counts
        # as a bootstrap drop. The wall-clock gate owns its age decision.
        packet_time = extract_meshtastic_rx_time(packet)

        expired = False
        window_drops = 0
        remaining_for_log = 0.0
        dropped = False
        with self._startup_drain_lock:
            entry = self._reconnect_bootstrap
            if entry is None:
                return False
            boot_generation, deadline, live_drops = entry
            if connection_generation != boot_generation:
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._reconnect_bootstrap = None
                expired = True
                window_drops = live_drops
            elif packet_time is None:
                self._reconnect_bootstrap = (
                    boot_generation,
                    deadline,
                    live_drops + 1,
                )
                self._reconnect_backlog_packets_dropped += 1
                window_drops = live_drops + 1
                remaining_for_log = remaining
                dropped = True

        if expired:
            ctx = self.ctx
            if ctx is not None:
                ctx.logger.info(
                    "MeshtasticAdapter %s: reconnect receive bootstrap window "
                    "ended - accepting packets (dropped=%d)",
                    self.adapter_id,
                    window_drops,
                )
            return False
        if not dropped:
            return False

        ctx = self.ctx
        if ctx is not None:
            ctx.logger.debug(
                "MeshtasticAdapter %s: reconnect backlog dropped "
                "transport=meshtastic packet_id=%s remaining=%.3fs "
                "window=%s",
                self.adapter_id,
                packet_id,
                remaining_for_log,
                _RECONNECT_RECEIVE_BOOTSTRAP_SECONDS,
            )
        return True

    def _check_startup_packet_drain(self, packet_id: Any) -> bool:
        """Check whether a relay-classified packet falls inside the startup drain window.

        The window is armed before the first receive-ready connection and is
        content-independent: while ``time.monotonic()`` is before the
        deadline, every relay-classified packet is dropped without
        consulting ``rxTime``.  This covers the backlog radios dump from
        their FROMRADIO queue on connect (BLE in particular), including
        packets whose ``rxTime`` is missing, malformed, or skewed by an
        unsynced node clock — cases the rxTime-based startup-backlog
        suppression cannot judge.

        Expiry is detected lazily on evaluation: the first check at or
        after the deadline clears the window, emits a single INFO
        completion line with the dropped count, and the window stays
        closed for the rest of the session. A transparent reconnect retires
        any remaining initial drain without an expiry summary. A configured
        window of ``0`` leaves the deadline unarmed.  The per-packet DEBUG drop log follows
        the startup-backlog suppression log format.

        Parameters
        ----------
        packet_id:
            The packet ID for safe logging.

        Returns
        -------
        bool
            ``True`` while the drain window is open (the packet should be
            dropped); ``False`` once the window has expired or is disabled.
        """
        # Both the SDK reader and the loop's simulated ingress can evaluate
        # this gate. Retire the deadline and capture the count atomically;
        # log outside the lock to avoid blocking the other ingress path.
        with self._startup_drain_lock:
            deadline = self._startup_drain_deadline_monotonic
            if deadline is None:
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._startup_drain_deadline_monotonic = None
            else:
                self._startup_drain_packets_dropped += 1
            dropped = self._startup_drain_packets_dropped

        ctx = self.ctx
        if remaining <= 0:
            if ctx is not None:
                ctx.logger.info(
                    "MeshtasticAdapter %s: startup packet drain window ended - "
                    "accepting packets (dropped=%d)",
                    self.adapter_id,
                    dropped,
                )
            return False

        if ctx is not None:
            ctx.logger.debug(
                "MeshtasticAdapter %s: startup packet drain dropped "
                "transport=meshtastic packet_id=%s remaining=%.3fs window=%s",
                self.adapter_id,
                packet_id,
                remaining,
                self._config.startup_packet_drain_seconds,
            )
        return True

    def _check_startup_backlog_suppress(
        self,
        packet: dict[str, Any],
        packet_id: Any,
        connection_generation: int | None = None,
    ) -> bool:
        """Check whether a relay-classified packet should be suppressed as stale backlog.

        Delegates ``rxTime`` extraction to
        :func:`~medre.adapters.meshtastic.startup_backlog.extract_meshtastic_rx_time`
        and the suppression decision to
        :func:`~medre.core.policies.startup_backlog_suppress.should_suppress_startup_backlog`.

        The wall-clock baseline is the receive-ready watermark of the
        packet's own connection generation (see
        :meth:`_record_connection_receive_ready`), so queue backlog drained
        by a freshly reconnected radio is judged against that reconnect
        instead of the original startup.  Generations without a recorded
        watermark (direct test injection, fake mode, pruned history) fall
        back to the initial receive-ready baseline.

        Returns ``True`` when the packet's ``rxTime`` predates
        ``baseline - startup_backlog_suppress_seconds``.

        Conservative: missing, ``None``, or non-numeric ``rxTime`` are **not**
        suppressed.  A suppression window of ``0`` disables suppression entirely.

        Parameters
        ----------
        packet:
            Raw Meshtastic packet dict (must contain top-level ``rxTime``).
        packet_id:
            The packet ID for safe logging.
        connection_generation:
            Connection generation captured at receive validation; ``None``
            retains the initial receive-ready baseline.

        Returns
        -------
        bool
            ``True`` if the packet should be suppressed; ``False`` otherwise.
        """
        window = self._config.startup_backlog_suppress_seconds
        baseline_epoch = self._receive_ready_epoch(connection_generation)
        if window <= 0 or baseline_epoch is None:
            return False

        packet_time = extract_meshtastic_rx_time(packet)
        baseline = datetime.fromtimestamp(baseline_epoch, tz=timezone.utc)

        if should_suppress_startup_backlog(packet_time, baseline, float(window)):
            if self.ctx is not None:
                cutoff = baseline_epoch - float(window)
                self.ctx.logger.debug(
                    "MeshtasticAdapter %s: startup backlog suppressed "
                    "transport=meshtastic packet_id=%s rxTime=%s cutoff=%s "
                    "window=%s generation=%s",
                    self.adapter_id,
                    packet_id,
                    packet.get("rxTime"),
                    cutoff,
                    window,
                    connection_generation,
                )
            return True

        return False

    def _on_packet(
        self,
        packet: dict[str, Any],
        *,
        source_session: MeshtasticSession | None = None,
        source_generation: int | None = None,
    ) -> None:
        """Process an inbound Meshtastic packet.

        Classifies the packet, decodes it via the codec, and publishes
        the resulting canonical event inbound.

        Called from the Meshtastic SDK reader thread.  The ``_started``
        guard rejects packets that arrive after :meth:`stop` has been
        called, preventing late coroutine scheduling against a torn-down
        session.

        Parameters
        ----------
        packet:
            Raw Meshtastic packet dict.
        source_session / source_generation:
            Native receive ownership captured at interface validation. Direct
            test injection may omit both; SDK dispatch supplies both.
        """
        # Inbound-path lifecycle guard: reject packets arriving after
        # stop() has cleared _started.  This check runs before any
        # coroutine is scheduled via run_coroutine_threadsafe.
        if not self._started:
            return
        if self.ctx is None:
            return

        try:
            reservations = self._ingress_reservations
            if source_session is not None:
                if (
                    self._session is not source_session
                    or source_generation is None
                    or not source_session.is_connection_generation_current(
                        source_generation
                    )
                ):
                    return
                session = source_session
                connection_generation = source_generation
            else:
                # Direct test injection retains the existing callback API.
                session = self._session
                connection_generation = (
                    session.connection_generation if session is not None else None
                )
            own_node_id = session.node_id if session is not None else None
            classification = self._classifier.classify(packet, own_node_id=own_node_id)
            self._increment_classifier_counters(classification)
            if classification.action != "relay":
                self._log_classification(classification)

            # Native routing results (ACK/NAK) are classified as ack or
            # deferred routing packets; offer them to send correlation
            # before the relay gate discards them from the publish path.
            if _is_native_routing_callback(classification):
                self._observe_native_routing_packet(
                    packet,
                    connection_generation=connection_generation,
                    own_node_id=own_node_id,
                )

            # Only relay packets proceed to decode and publish
            if classification.action != "relay":
                return

            # Startup packet drain gate — content-independent, evaluated
            # before the rxTime-based suppression check, which remains as
            # defense-in-depth behind it.  Drain-dropped packets do not
            # enter the rxTime counters.
            if self._check_startup_packet_drain(classification.packet_id):
                return

            # Startup backlog suppression gate — wall-clock rxTime judged
            # against the receive-ready watermark of this packet's own
            # connection generation, so pre-reconnect queue backlog cannot
            # pass against the original startup baseline.
            self._startup_backlog_packets_seen += 1
            if self._check_startup_backlog_suppress(
                packet, classification.packet_id, connection_generation
            ):
                self._startup_backlog_packets_suppressed += 1
                return

            # Reconnect receive bootstrap gate — drops only packets whose
            # age cannot be proven (no usable rxTime) while the short
            # generation-scoped window after a transparent reconnect is
            # open.  Provably fresh packets never reach the drop branch.
            if self._check_reconnect_receive_bootstrap(
                packet, connection_generation, classification.packet_id
            ):
                return

            self._log_classification(classification)

            # Reserve immediately after the relay and startup-backlog gates.
            # Refused packets never pay for node enrichment or codec decode
            # on the SDK reader thread; ACK/NAK and routing correlation are
            # handled above without consuming a slot.
            release = reservations.acquire(connection_generation)
            if release is None:
                self._record_ingress_overflow()
                return
            try:
                # Text packets contain no NODEINFO_APP user labels. Embed
                # available node names into metadata only after admission
                # to the bounded staging pool succeeds.
                node_info = self._enrich_with_node_info(packet)
                canonical = self._codec.decode(packet, node_info=node_info)
                loop = self._loop
                if loop is None or loop.is_closed():
                    release()
                    return
                # The reader thread cannot create asyncio tasks directly.
                # Close the coroutine if submission fails after creation
                # (e.g. loop closes between the check and scheduling).
                admission_started = threading.Event()
                publish = self._on_packet_async(
                    canonical,
                    session=session,
                    connection_generation=connection_generation,
                    admission_started=admission_started,
                )
                try:
                    future: concurrent.futures.Future[object] = (
                        asyncio.run_coroutine_threadsafe(publish, loop)
                    )
                except BaseException:
                    publish.close()
                    raise
            except BaseException:
                release()
                raise
            self._inbound_admission_started[future] = admission_started
            self._inbound_futures.add(future)

            def retire_submission(done: concurrent.futures.Future[object]) -> None:
                self._finish_inbound_future(done, release)

            future.add_done_callback(retire_submission)
            self._report_producer_pressure("scheduled")
        except Exception:
            if self.ctx is not None:
                self.ctx.logger.exception(
                    "MeshtasticAdapter %s: error processing inbound packet",
                    self.adapter_id,
                )

    async def _on_packet_async(
        self,
        canonical: CanonicalEvent,
        *,
        session: MeshtasticSession | None = None,
        connection_generation: int | None = None,
        admission_started: threading.Event | None = None,
    ) -> None:
        """Async handler for packets received via :meth:`_on_packet`.

        Publishes the canonical event and logs exceptions from the
        background task.

        The ``_started`` guard prevents publication after :meth:`stop`
        has been called — this catches coroutines that were already
        scheduled via ``run_coroutine_threadsafe`` but haven't executed
        yet when teardown begins.

        Parameters
        ----------
        canonical:
            The decoded canonical event to publish.
        """
        try:
            if session is not None and connection_generation is not None:
                if (
                    self._session is not session
                    or not session.is_connection_generation_current(
                        connection_generation
                    )
                ):
                    return
            if self.ctx is not None and self._started:
                if admission_started is not None:
                    admission_started.set()
                await self.publish_inbound(canonical)
                self._inbound_published += 1
        except Exception:
            if self.ctx is not None:
                self.ctx.logger.exception(
                    "MeshtasticAdapter %s: error in background publish",
                    self.adapter_id,
                )

    def _finish_inbound_future(
        self,
        future: concurrent.futures.Future[object],
        release: Callable[[], None],
    ) -> None:
        """Retire one staged inbound future and return its staging reservation.

        The done callback fires exactly once per future, on completion,
        error, and cancellation alike, so every acquire is balanced by
        exactly one release and no slot leaks on any terminal state.
        """
        self._inbound_futures.discard(future)
        self._inbound_admission_started.pop(future, None)
        release()

    def _record_ingress_overflow(self) -> None:
        """Record one refused reader-thread staging attempt as explicit loss.

        The refused packet was never scheduled and never admitted: no
        dedup state and no native identity was consumed. The loss is made
        visible — counted in ``producer_overflow`` diagnostics, reported
        through the optional producer-pressure sink. Warning logs are
        rate-limited and include the cumulative count, without message
        content or sender identity. The refusal implies no
        admission and no retry; the packet is simply gone.
        """
        self._producer_overflow += 1
        if self.ctx is not None:
            now = time.monotonic()
            if now - self._last_overflow_warning_at >= 5.0:
                self._last_overflow_warning_at = now
                self.ctx.logger.warning(
                    "MeshtasticAdapter %s: reader-thread ingress staging refused "
                    "reason=callback_capacity total_refused=%d",
                    self.adapter_id,
                    self._producer_overflow,
                )
        self._report_producer_pressure("overflow", reason="callback_capacity")

    def _report_producer_pressure(
        self,
        outcome: str,
        *,
        count: int = 1,
        reason: str | None = None,
    ) -> None:
        """Best-effort report one producer pressure fact to the context sink.

        The sink is optional (older core and direct tests omit it) and
        reporting is skipped when absent. The core sink never raises; the
        defensive catch keeps a misbehaving observer from ever disturbing
        the reader thread or the shutdown path. Reports are content-free:
        outcome, coalesced count, and a machine-token reason only.
        """
        ctx = self.ctx
        if ctx is None:
            return
        report = getattr(ctx, "report_producer_pressure", None)
        if report is None or ProducerPressureReport is None:
            return
        try:
            report(
                ProducerPressureReport(
                    self.adapter_id, outcome, count=count, reason=reason
                )
            )
        except Exception:
            if ctx.logger is not None:
                ctx.logger.debug(
                    "MeshtasticAdapter %s: producer pressure report failed (%s)",
                    self.adapter_id,
                    outcome,
                )

    async def simulate_inbound(self, packet: dict[str, Any]) -> None:
        """Simulate an inbound Meshtastic packet for testing.

        Classifies, decodes, and publishes the packet through the same
        path as a real inbound packet.

        Parameters
        ----------
        packet:
            Raw Meshtastic packet dict.

        Raises
        ------
        RuntimeError
            If the adapter has never been started in this lifecycle
            (no runtime context is available).

        Notes
        -----
        If the adapter was started and later stopped, this method
        returns without publishing.
        """
        if self.ctx is None:
            raise RuntimeError(
                f"Adapter {self.adapter_id!r} has not been started; "
                "call start() before simulate_inbound()."
            )
        if not self._started:
            return

        session = self._session
        classification = self._classifier.classify(
            packet,
            own_node_id=(session.node_id if session is not None else None),
        )
        self._increment_classifier_counters(classification)
        if classification.action != "relay":
            self._log_classification(classification)

        # Same send-correlation hook as the live callback path so tests
        # exercising simulate_inbound observe identical behavior.
        if _is_native_routing_callback(classification):
            self._observe_native_routing_packet(
                packet,
                connection_generation=(
                    session.connection_generation if session is not None else None
                ),
                own_node_id=(session.node_id if session is not None else None),
            )

        if classification.action != "relay":
            return

        # Startup packet drain gate — content-independent, evaluated before
        # the rxTime-based suppression check, which remains as
        # defense-in-depth behind it.  Drain-dropped packets do not enter
        # the rxTime counters.
        if self._check_startup_packet_drain(classification.packet_id):
            return

        # Startup backlog suppression gate — same per-generation receive
        # watermark baseline as the live callback path.
        connection_generation = (
            session.connection_generation if session is not None else None
        )
        self._startup_backlog_packets_seen += 1
        if self._check_startup_backlog_suppress(
            packet, classification.packet_id, connection_generation
        ):
            self._startup_backlog_packets_suppressed += 1
            return

        # Reconnect receive bootstrap gate — mirrors the live callback path.
        if self._check_reconnect_receive_bootstrap(
            packet, connection_generation, classification.packet_id
        ):
            return

        self._log_classification(classification)

        # Enrich with node info (same path as _on_packet)
        node_info = self._enrich_with_node_info(packet)
        canonical = self._codec.decode(packet, node_info=node_info)
        await self.publish_inbound(canonical)
        self._inbound_published += 1

    # -- Diagnostics --------------------------------------------------------

    def diagnostics(self) -> dict[str, Any]:
        """Return a diagnostic snapshot combining adapter and session state.

        No secrets, private keys, raw protobuf dumps, or sensitive radio
        identifiers beyond what is public.

        Returns
        -------
        dict
            Combined adapter + session diagnostics.
        """
        drain_task = self._drain_task
        with self._startup_drain_lock:
            latest_receive_ready_generation = (
                sorted(self._receive_ready_epochs)[-1]
                if self._receive_ready_epochs
                else None
            )
        result: dict[str, Any] = {
            "adapter_id": self.adapter_id,
            "platform": self.platform,
            "started": self._started,
            "connection_type": self._config.connection_type,
            "mode": self._config.connection_type,
            "health": self._last_health,
            QUEUE_PENDING: self._queue.pending_count,
            "queue_total_sent": self._queue.total_sent,
            "queue_total_failed": self._queue.total_failed,
            "queue_total_enqueued": self._queue.total_enqueued,
            "queue_total_dequeued": self._queue.total_dequeued,
            "queue_total_rejected": self._queue.total_rejected,
            "queue_total_requeued": self._queue.total_requeued,
            "queue_total_exhausted": self._queue.total_exhausted,
            "queue_total_permanent_failed": self._queue.total_permanent_failed,
            "queue_max_size": self._queue.max_queue_size,
            "queue_send_max_attempts": self._queue.max_attempts,
            "queue_pressure_state": self._queue.pressure_state,
            "queue_utilization_pct": self._queue.queue_health["utilization_pct"],
            "queue_warning_threshold_pct": self._queue.queue_health[
                "warning_threshold_pct"
            ],
            "queue_critical_threshold_pct": self._queue.queue_health[
                "critical_threshold_pct"
            ],
            "queue_peak_depth": self._queue.queue_health["peak_depth"],
            "queue_delay_between_messages": self._queue.delay_between_messages,
            "queue_last_send_time": self._queue.queue_health["last_send_time"],
            "drain_task_running": (drain_task is not None and not drain_task.done()),
            "background_tasks": len(self._background_tasks),
            # Classifier counters — inbound evidence
            "classifier_packets_seen": self._classifier_packets_seen,
            "classifier_packets_relayed": self._classifier_packets_relayed,
            "classifier_packets_ignored": self._classifier_packets_ignored,
            "classifier_packets_dropped": self._classifier_packets_dropped,
            "classifier_packets_deferred": self._classifier_packets_deferred,
            "classifier_packets_malformed": self._classifier_packets_malformed,
            "classifier_packets_encrypted_dropped": self._classifier_packets_encrypted_dropped,
            "classifier_packets_detection_sensor_deferred": self._classifier_packets_detection_sensor_deferred,
            "classifier_packets_dm_ignored": self._classifier_packets_dm_ignored,
            "classifier_packets_empty_text_ignored": self._classifier_packets_empty_text_ignored,
            "classifier_packets_unknown_portnum_deferred": self._classifier_packets_unknown_portnum_deferred,
            "classifier_packets_self_echo_ignored": self._classifier_packets_self_echo_ignored,
            "inbound_published": self._inbound_published,
            # Bounded reader-thread staging — explicit, counted loss
            "producer_overflow": self._producer_overflow,
            "inbound_staged_pending": len(self._inbound_futures),
            "max_pending_ingress_callbacks": self._config.max_pending_ingress_callbacks,
            # Startup backlog suppression
            "startup_backlog_packets_seen": self._startup_backlog_packets_seen,
            "startup_backlog_packets_suppressed": self._startup_backlog_packets_suppressed,
            "startup_backlog_suppress_seconds": self._config.startup_backlog_suppress_seconds,
            "adapter_start_epoch": self._adapter_start_epoch,
            # Startup packet drain — packets dropped by the content-
            # independent connect window before reaching the rxTime gate.
            "startup_drain_packets_dropped": self._startup_drain_packets_dropped,
            "startup_packet_drain_seconds": self._config.startup_packet_drain_seconds,
            # Reconnect receive watermark — per-generation rxTime baseline
            # and the generation-scoped unprovable-packet bootstrap window.
            "receive_ready_generation": latest_receive_ready_generation,
            "reconnect_backlog_packets_dropped": self._reconnect_backlog_packets_dropped,
            "reconnect_receive_bootstrap_seconds": _RECONNECT_RECEIVE_BOOTSTRAP_SECONDS,
            # Outbound gate
            "outbound_mode": self._config.outbound_mode,
            "outbound_gate_suppressed": self._outbound_gate_suppressed,
            # Send correlation (classification only; never emits observations)
            "send_correlation": self._send_correlation.snapshot(),
            # Full queue health snapshot (structurally identical to the
            # individual queue_* keys above but provided as a single
            # nested dict for programmatic consumers).
            "queue": self._queue.queue_health,
        }

        if self._session is not None:
            session_diag = self._session.diagnostics()
            result["session"] = {
                "connected": session_diag.connected,
                "reconnecting": session_diag.reconnecting,
                "reconnect_attempts": session_diag.reconnect_attempts,
                "reconnect_total_attempts": session_diag.reconnect_total_attempts,
                "liveness_enabled": session_diag.liveness_enabled,
                "liveness_probe_successes": session_diag.liveness_probe_successes,
                "liveness_probe_failures": session_diag.liveness_probe_failures,
                "liveness_consecutive_failures": (
                    session_diag.liveness_consecutive_failures
                ),
                "last_liveness_probe_time": session_diag.last_liveness_probe_time,
                "last_liveness_success_time": session_diag.last_liveness_success_time,
                "last_liveness_error": session_diag.last_liveness_error,
                "last_packet_time": session_diag.last_packet_time,
                "node_id": session_diag.node_id,
                "channel_count": session_diag.channel_count,
                "transient_delivery_failures": session_diag.transient_delivery_failures,
                "permanent_delivery_failures": session_diag.permanent_delivery_failures,
                "stale_receive_callbacks": session_diag.stale_receive_callbacks,
                "stale_disconnect_callbacks": session_diag.stale_disconnect_callbacks,
                "last_error": session_diag.last_error,
            }
        else:
            result["session"] = {
                "connected": False,
                "reconnecting": False,
                "reconnect_attempts": 0,
                "reconnect_total_attempts": 0,
                "liveness_enabled": False,
                "liveness_probe_successes": 0,
                "liveness_probe_failures": 0,
                "liveness_consecutive_failures": 0,
                "last_liveness_probe_time": None,
                "last_liveness_success_time": None,
                "last_liveness_error": None,
                "last_packet_time": None,
                "node_id": None,
                "channel_count": 0,
                "transient_delivery_failures": 0,
                "permanent_delivery_failures": 0,
                "stale_receive_callbacks": 0,
                "stale_disconnect_callbacks": 0,
                "last_error": None,
            }

        return result

    # -- Background task management -----------------------------------------

    def _observe_callback_task(
        self,
        task: asyncio.Task[Any],
        label: str,
        event_id: str = "",
    ) -> None:
        """Observe a callback task's result after cancellation.

        When ``_process_queue`` is cancelled, shielded callback tasks are
        added to ``_background_tasks`` for later draining.  This helper
        registers a done-callback that retrieves and logs any exception
        before the task is removed from ``_background_tasks``, preventing
        ``"Task exception was never retrieved"`` warnings from the event
        loop.

        Parameters
        ----------
        task:
            The background callback task to observe.
        label:
            Human-readable label for log messages (e.g. ``"terminal"``,
            ``"native-ref"``).
        event_id:
            Optional event ID for log context.
        """
        adapter_id = self.adapter_id

        def _on_done(t: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                if self.ctx is not None:
                    self.ctx.logger.warning(
                        "MeshtasticAdapter %s: %s callback "
                        "exception observed for event_id=%s: %r",
                        adapter_id,
                        label,
                        event_id,
                        exc,
                    )

        self._background_tasks.add(task)
        task.add_done_callback(_on_done)

    def _observe_detached_task(self, task: asyncio.Task[Any]) -> None:
        """Done callback for tasks detached after shutdown timeout.

        When a critical callback suppresses CancelledError and remains
        pending past the drain timeout, it is detached from
        ``_background_tasks`` with this callback.  When the task finally
        completes, this callback retrieves and logs any exception so
        nothing becomes "Task exception was never retrieved."
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None and self.ctx is not None:
            self.ctx.logger.warning(
                "MeshtasticAdapter %s: detached background callback "
                "exception after shutdown timeout: %r",
                self.adapter_id,
                exc,
            )

    async def _drain_background_tasks(self, timeout: float = 5.0) -> None:
        """Drain inbound futures and await tracked background tasks.

        First drains ``concurrent.futures.Future`` instances submitted by
        :meth:`_on_packet` via ``run_coroutine_threadsafe`` — cancelling
        any that haven't started yet and suppressing results from those
        still in flight.  Then awaits tracked :class:`asyncio.Task`
        instances (deferred delivery feedback) with a bounded timeout.

        Completed tasks have their exceptions observed and logged.  If any
        tasks are still pending after the timeout, they are explicitly
        cancelled and given a second bounded timeout to finish.  Tasks
        that suppress ``CancelledError`` and remain pending after the
        second timeout are detached with a done-callback observer so their
        exceptions are still logged if/when they eventually finish.  No
        callback exception is intentionally left unobserved.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for tasks to finish in each phase
            (initial drain and post-cancel drain).
        """
        # --- Drain inbound futures (run_coroutine_threadsafe) ---
        # Cancel any that haven't started; suppress results from the rest.
        # Only a submission known not to have entered publication supplies
        # a pre-admission cancellation fact. A cancelled cross-thread future
        # cannot establish absence of durable admission by a running publisher.
        # Coalesce the known pre-publication cancellations into one report.
        cancelled_staged = 0
        for future in list(self._inbound_futures):
            # A cancelled concurrent future says nothing about admission by
            # a publisher that has already started (or suppresses cancellation).
            started = self._inbound_admission_started.get(future)
            if future.cancel() and started is not None and not started.is_set():
                cancelled_staged += 1
        self._inbound_futures.clear()
        if cancelled_staged:
            self._report_producer_pressure(
                "cancelled_before_admission", count=cancelled_staged
            )

        # --- Drain asyncio background tasks ---
        # Critical callback tasks (terminal / native-ref) are given a
        # bounded timeout to complete.  Completed tasks have exceptions
        # observed; pending tasks are cancelled and given a second
        # timeout.  Still-pending tasks are detached with an observer.
        if self._background_tasks:
            _done, _pending = await asyncio.wait(
                self._background_tasks,
                timeout=timeout,
            )

            # Retrieve and log exceptions from completed tasks.
            for task in _done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None and self.ctx is not None:
                    self.ctx.logger.warning(
                        "MeshtasticAdapter %s: background callback "
                        "exception during drain: %r",
                        self.adapter_id,
                        exc,
                    )

            # If any tasks are still pending after timeout, cancel them
            # explicitly and await so no task remains untracked or raises
            # an unobserved exception.
            if _pending:
                for task in _pending:
                    task.cancel()
                # Await cancelled tasks to collect CancelledError.
                done_after_cancel, still_pending = await asyncio.wait(
                    _pending,
                    timeout=timeout,
                )
                for task in done_after_cancel:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is not None and self.ctx is not None:
                        self.ctx.logger.warning(
                            "MeshtasticAdapter %s: background callback "
                            "exception after timeout cancel: %r",
                            self.adapter_id,
                            exc,
                        )
                # Tasks that suppress CancelledError and remain pending
                # after the second timeout are detached with a done
                # callback that observes their result.  They are NOT
                # left in _background_tasks (shutdown must progress) but
                # their exceptions will be observed if/when they finish.
                for task in still_pending:
                    if self.ctx is not None:
                        self.ctx.logger.warning(
                            "MeshtasticAdapter %s: background callback "
                            "task still pending after shutdown timeout; "
                            "detaching with observer",
                            self.adapter_id,
                        )
                    self._background_tasks.discard(task)
                    task.add_done_callback(self._observe_detached_task)

        self._background_tasks.clear()

    # -- Queue / send helpers -----------------------------------------------

    async def send_one(self) -> QueueDeliveryResult | QueueTerminalResult | None:
        """Send one queued payload via the session, if connected.

        Creates an async wrapper around the session's ``send`` method
        and delegates to :meth:`MeshtasticOutboundQueue.process_one`.

        Returns ``None`` if the queue is empty or the session is not
        connected (fake mode).

        Returns
        -------
        QueueDeliveryResult | QueueTerminalResult | None
            Delivery result on success, terminal result on exhaustion or
            permanent failure, or ``None`` when the queue is empty /
            session is disconnected.
        """
        session = self._session
        if session is None or not session.connected:
            for guarded in tuple(self._guarded_sends):
                guarded.fail(
                    "session disconnected before guarded handoff", transient=True
                )
            return None

        async def _send_fn(item: dict[str, Any]) -> Any:
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                raise MeshtasticSendError(
                    "Meshtastic queued payload must be a mapping", transient=False
                )
            raw_text = payload.get("text")
            if not isinstance(raw_text, str):
                raise MeshtasticSendError(
                    "Meshtastic queued text must be a string", transient=False
                )
            text = raw_text
            send_dict: dict[str, Any] = {
                "text": text,
                "channel_index": item.get("channel_index", 0),
            }
            if "reply_id" in payload:
                send_dict["reply_id"] = payload["reply_id"]
            if "emoji" in payload:
                send_dict["emoji"] = payload["emoji"]
            guarded = item.get("guarded_send")
            if "guarded_send" in item and not isinstance(guarded, GuardedQueuedSend):
                raise MeshtasticSendError(
                    "malformed guarded queue ownership", transient=False
                )
            if isinstance(guarded, GuardedQueuedSend):

                async def grant_current() -> None:
                    """Do not lend a core grant to an adapter session replaced while waiting."""
                    if not self._started or self._session is not session:
                        raise AdapterPermanentError(
                            "guarded queue owner is no longer active"
                        )
                    await guarded.grant()
                    if not self._started or self._session is not session:
                        raise AdapterPermanentError(
                            "guarded queue owner changed during grant"
                        )

                try:
                    return await session.send(
                        send_dict,
                        before_native_send=grant_current,
                        is_cancelled=lambda: (
                            guarded.cancelled.is_set()
                            or not self._started
                            or self._session is not session
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise MeshtasticSendError(
                        "guarded queued send did not complete",
                        transient=not guarded.entered
                        and getattr(exc, "transient", False),
                    ) from exc
            # The accepting attempt's provenance comes from inside the
            # session's retry loop: a reconnect between attempts replaces
            # the client and its generation, and the correlation fence
            # must name the generation that actually accepted. Session
            # doubles without the provenance boundary keep the plain
            # send path and are never correlated.
            provenance_send = getattr(session, "send_with_provenance", None)
            if callable(provenance_send):
                attempt = await provenance_send(send_dict)
                item["_connection_generation"] = attempt.connection_generation
                return attempt.native
            return await session.send(send_dict)

        result = await self._queue.process_one(send_fn=_send_fn)
        if result is not None and isinstance(
            (completed_guard := result.item.get("guarded_send")), GuardedQueuedSend
        ):
            if isinstance(result, QueueDeliveryResult):
                completed_guard.complete(result.handoff)
            else:
                completed_guard.fail(result.error or "guarded queue handoff failed")
        if isinstance(result, QueueDeliveryResult):
            self._record_send_correlation(result)
        return result

    def _record_send_correlation(self, result: QueueDeliveryResult) -> None:
        """Retain one accepted send's trusted local facts for later callbacks.

        Records only validated outbox-backed work that carried a real
        native packet id. Guarded planned packets are excluded: their
        synchronous handoff belongs to core's own dispatch. An entry
        without a native id could never match a callback request id and
        is skipped instead of guessed.
        """
        if isinstance(result.item.get("guarded_send"), GuardedQueuedSend):
            return
        if result.handoff.native_message_id is None:
            return
        provenance = self._callback_attempt_provenance(
            result.item, callback_kind="send-correlation"
        )
        if provenance is None:
            return
        generation = result.item.get("_connection_generation")
        if isinstance(generation, bool) or not isinstance(generation, int):
            # Without the owning generation the fact cannot be fenced;
            # refusing it keeps every correlated pair session-exact.
            return
        self._send_correlation.record(
            AcceptedSendFacts(
                native_message_id=result.handoff.native_message_id,
                native_channel_id=result.handoff.native_channel_id,
                connection_generation=generation,
                event_id=provenance.event_id,
                delivery_plan_id=provenance.delivery_plan_id,
                outbox_id=provenance.outbox_id,
                attempt_number=provenance.attempt_number,
            )
        )

    def _observe_native_routing_packet(
        self,
        packet: dict[str, Any],
        *,
        connection_generation: int | None,
        own_node_id: str | None = None,
    ) -> None:
        """Pair one native routing callback against accepted sends.

        Classification and counting only: no observation is ever
        submitted to core from here, because no routing result this
        transport can produce qualifies as delivered evidence. The
        local node's id lets the local implicit acknowledgment
        classify as itself rather than as a third party. Failures
        are contained so the inbound path is never disturbed.
        """
        try:
            normalized = normalize_native_routing_result(packet)
            outcome = self._send_correlation.observe(
                normalized,
                connection_generation=connection_generation,
                own_node_id=own_node_id,
            )
            if self.ctx is not None:
                reason = getattr(outcome, "reason", None)
                if isinstance(reason, str):
                    self.ctx.logger.debug(
                        "MeshtasticAdapter %s: routing callback uncorrelated: %s",
                        self.adapter_id,
                        reason,
                    )
        except Exception:
            if self.ctx is not None:
                self.ctx.logger.debug(
                    "MeshtasticAdapter %s: routing callback normalization failed",
                    self.adapter_id,
                    exc_info=True,
                )

    async def _process_queue(self) -> None:
        """Background task that continuously drains the outbound queue.

        Started during :meth:`start` and cancelled during :meth:`stop`.
        Calls :meth:`send_one` in a loop; sleeps when the queue is empty
        or on transient errors.

        After each successful transport hand-off, reports a
        :class:`~medre.core.contracts.delivery.DeferredHandoffCompleted`
        through the unified delivery-feedback callback. Callback failures are
        caught and logged so they never crash the queue drain.

        When :meth:`send_one` returns a :class:`QueueTerminalResult`
        (exhausted or permanent failure), reports a
        :class:`~medre.core.contracts.delivery.DeferredHandoffFailed`.  On
        cancellation or shutdown, the in-flight cancelled item is reported.
        Remaining queued items are drained and reported as abandoned only
        when there is evidence the drain task was actively processing work.
        If no in-flight item existed when stop cancelled the drain task,
        local queued items remain in memory for next start and the durable
        outbox remains queued/stale-recoverable.

        See :meth:`_report_cancelled_and_drain` for the drain-on-cancel
        implementation.
        """
        try:
            while self._started:
                try:
                    result = await self.send_one()
                    if result is None:
                        await asyncio.sleep(0.1)
                        continue

                    if isinstance(result.item.get("guarded_send"), GuardedQueuedSend):
                        continue  # Its waiting core dispatch owns parent completion.

                    if isinstance(result, QueueTerminalResult):
                        # Shield feedback so stop() cancellation cannot abort
                        # reporting an item already dequeued from the queue.
                        cb_task = asyncio.ensure_future(
                            self._report_deferred_failure(result)
                        )
                        try:
                            await asyncio.shield(cb_task)
                        except asyncio.CancelledError:
                            event_id = str(result.item.get("event_id") or "")
                            self._observe_callback_task(
                                cb_task,
                                "deferred-failure",
                                event_id,
                            )
                            raise
                        continue

                    # A successful queue send reached the radio transport
                    # boundary. Report completion even when the SDK did not
                    # assign a native message ID; native identity is optional.
                    if (
                        self.ctx is not None
                        and self.ctx.report_delivery_feedback is not None
                    ):
                        event_id = str(result.item.get("event_id") or "")
                        try:
                            cb_task = asyncio.ensure_future(
                                self._report_deferred_completion(result)
                            )
                            try:
                                await asyncio.shield(cb_task)
                            except asyncio.CancelledError:
                                self._observe_callback_task(
                                    cb_task,
                                    "deferred-completion",
                                    event_id,
                                )
                                raise
                        except Exception:
                            if self.ctx is not None:
                                self.ctx.logger.exception(
                                    "MeshtasticAdapter %s: error reporting "
                                    "deferred hand-off completion for event_id=%s",
                                    self.adapter_id,
                                    event_id,
                                )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if self.ctx is not None:
                        self.ctx.logger.exception(
                            "MeshtasticAdapter %s: error in queue drain",
                            self.adapter_id,
                        )
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            # Report the in-flight cancelled item (if any) and drain
            # remaining queued items as abandoned.
            await self._report_cancelled_and_drain()

    def _callback_attempt_provenance(
        self,
        item: Mapping[str, object],
        *,
        callback_kind: str,
    ) -> DeliveryAttemptProvenance | None:
        """Return safe callback provenance, logging and dropping invalid work.

        Outbox-less queue work can still be useful in standalone/direct adapter
        mode, but it has no durable authority for asynchronous MEDRE evidence.
        Such work is sent without emitting durable delivery feedback. Missing
        or invalid provenance on outbox-backed work is an internal contract
        violation and
        likewise fails closed without crashing the queue/shutdown task.
        """
        if isinstance(item.get("guarded_send"), GuardedQueuedSend):
            return None  # Synchronous packet facts must not become deferred parent feedback.

        provenance = item.get("attempt_provenance")
        outbox_id = item.get("outbox_id")
        mismatch: str | None = None
        if isinstance(provenance, DeliveryAttemptProvenance):
            if self.adapter_id != provenance.target_adapter:
                mismatch = "target_adapter contradicts immutable attempt provenance"
            else:
                # ``channel_index`` is the resolved native radio channel. It is
                # transport evidence, not a mirror of the route-level
                # ``target_channel`` (which may be None or an adapter alias).
                for name, value, expected in (
                    ("event_id", item.get("event_id"), provenance.event_id),
                    (
                        "delivery_plan_id",
                        item.get("delivery_plan_id"),
                        provenance.delivery_plan_id,
                    ),
                    ("outbox_id", outbox_id, provenance.outbox_id),
                    (
                        "attempt_number",
                        item.get("attempt_number"),
                        provenance.attempt_number,
                    ),
                ):
                    if value is not None and value != expected:
                        mismatch = f"{name} contradicts immutable attempt provenance"
                        break
            if mismatch is None:
                return provenance
        elif outbox_id is not None:
            mismatch = "missing immutable attempt provenance"

        if self.ctx is not None:
            log = self.ctx.logger.error if outbox_id else self.ctx.logger.debug
            log(
                "MeshtasticAdapter %s: suppressing %s callback without exact "
                "attempt provenance (event_id=%s outbox_id=%s reason=%s)",
                self.adapter_id,
                callback_kind,
                item.get("event_id"),
                outbox_id,
                mismatch or "outbox-less direct delivery",
            )
        return None

    async def _report_deferred_failure(self, result: QueueTerminalResult) -> None:
        """Report one terminal failure for a previously deferred attempt."""
        callback = self.ctx.report_delivery_feedback if self.ctx is not None else None
        if callback is None:
            return
        provenance = self._callback_attempt_provenance(
            result.item, callback_kind="deferred-failure"
        )
        if provenance is None:
            return
        feedback = DeferredHandoffFailed(
            attempt_provenance=provenance,
            outcome=result.outcome,
            error=result.error,
        )
        try:
            await callback(feedback)
        except Exception:
            if self.ctx is not None:
                self.ctx.logger.exception(
                    "MeshtasticAdapter %s: error reporting deferred failure "
                    "for event_id=%s outcome=%s",
                    self.adapter_id,
                    provenance.event_id,
                    feedback.outcome,
                )

    async def _report_cancelled_and_drain(self) -> None:
        """Report in-flight cancellation and abandon orphaned deferred work."""
        callback = self.ctx.report_delivery_feedback if self.ctx is not None else None
        for guarded in tuple(self._guarded_sends):
            guarded.fail("queue owner stopped before guarded handoff completed")

        cancelled_item = self._queue.pop_cancelled_item()
        if cancelled_item is not None:
            if (
                callback is not None
                and (
                    provenance := self._callback_attempt_provenance(
                        cancelled_item, callback_kind="cancelled"
                    )
                )
                is not None
            ):
                feedback = DeferredHandoffFailed(
                    attempt_provenance=provenance,
                    outcome="cancelled",
                    error="queue drain task cancelled while item was in-flight",
                )
                try:
                    cb_task = asyncio.ensure_future(callback(feedback))
                    try:
                        await asyncio.shield(cb_task)
                    except asyncio.CancelledError:
                        self._observe_callback_task(
                            cb_task,
                            "cancelled",
                            provenance.event_id,
                        )
                        raise
                except Exception:
                    if self.ctx is not None:
                        self.ctx.logger.exception(
                            "MeshtasticAdapter %s: error reporting cancelled "
                            "item for event_id=%s",
                            self.adapter_id,
                            provenance.event_id,
                        )

            # An in-flight cancellation means this drain task was actively
            # processing work. Remaining local items are therefore orphaned by
            # shutdown and must be reported as abandoned.
            remaining = self._queue.drain_all()
            if callback is not None:
                for item in remaining:
                    provenance = self._callback_attempt_provenance(
                        item, callback_kind="abandoned"
                    )
                    if provenance is None:
                        continue
                    feedback = DeferredHandoffFailed(
                        attempt_provenance=provenance,
                        outcome="abandoned",
                        error="adapter shutdown with unsent queued items",
                    )
                    try:
                        cb_task = asyncio.ensure_future(callback(feedback))
                        try:
                            await asyncio.shield(cb_task)
                        except asyncio.CancelledError:
                            self._observe_callback_task(
                                cb_task,
                                "abandoned",
                                provenance.event_id,
                            )
                            raise
                    except Exception:
                        if self.ctx is not None:
                            self.ctx.logger.exception(
                                "MeshtasticAdapter %s: error reporting abandoned "
                                "item for event_id=%s",
                                self.adapter_id,
                                provenance.event_id,
                            )

    async def _report_deferred_completion(
        self,
        result: QueueDeliveryResult,
    ) -> None:
        """Report successful radio hand-off for one exact queued attempt."""
        callback = self.ctx.report_delivery_feedback if self.ctx else None
        if callback is None:
            return
        provenance = self._callback_attempt_provenance(
            result.item, callback_kind="deferred-completion"
        )
        if provenance is None:
            return

        handoff = result.handoff
        send_meta: dict[str, object] = {}
        meshtastic_meta: dict[str, object] = {}
        transport_keys = {
            "id",
            "packet_id",
            "channel",
            "reply_id",
            "emoji",
            "reaction_id",
            "to",
        }
        for key, value in handoff.metadata.items():
            if key == "meshtastic" and isinstance(value, Mapping):
                for nested_key, nested_value in value.items():
                    if nested_key != "schema_version":
                        meshtastic_meta[nested_key] = nested_value
            elif key in transport_keys:
                meshtastic_meta[key] = value
            else:
                meshtastic_meta[key] = value

        payload = result.item.get("payload", {})
        if isinstance(payload, Mapping):
            text = payload.get("text")
            if text is not None:
                meshtastic_meta["text"] = str(text)
            channel_name = payload.get("channel_name")
            if channel_name not in (None, ""):
                meshtastic_meta["channel_name"] = str(channel_name)
            reply_id = payload.get("reply_id")
            if reply_id is not None:
                meshtastic_meta["reply_id"] = reply_id
            emoji = payload.get("emoji")
            if emoji is not None:
                meshtastic_meta["emoji"] = emoji

        meshtastic_meta["schema_version"] = MESHTASTIC_NATIVE_SCHEMA_VERSION
        send_meta["meshtastic"] = meshtastic_meta
        completed_handoff = AdapterHandoffResult(
            native_message_id=handoff.native_message_id,
            native_channel_id=handoff.native_channel_id,
            native_thread_id=handoff.native_thread_id,
            native_relation_id=handoff.native_relation_id,
            confirmation_level=handoff.confirmation_level,
            note=handoff.note,
            metadata=send_meta,
        )
        await callback(
            DeferredHandoffCompleted(
                attempt_provenance=provenance,
                handoff=completed_handoff,
            )
        )

    @property
    def queue_health(self) -> dict[str, Any]:
        """Proxy for the outbound queue's health snapshot.

        Returns
        -------
        dict
            Queue operational state (pending, sent, failed counts, etc.).
        """
        return self._queue.queue_health

    @property
    def queue(self) -> MeshtasticOutboundQueue:
        """The adapter's owned outbound queue."""
        return self._queue

    # -- Codec access -------------------------------------------------------

    def get_codec(self) -> MeshtasticCodec:
        """Return the adapter's codec.

        Returns
        -------
        MeshtasticCodec
            The codec instance.
        """
        return self._codec

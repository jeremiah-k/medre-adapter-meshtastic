"""Meshtastic declaration: device-attached firmware state, no local files.

Layout contract read from this distribution: the adapter keeps no local
files; radio identity and packet state live in the attached radio's firmware.
Truthful coverage means saying so, not inventing files to capture. This
module follows the seam contract in
``medre.maintenance.stopped_declare``.
"""

from __future__ import annotations

from medre.maintenance.stopped_declare import (
    LocalExclusion,
    ProviderCoverage,
    StoppedStateDeclaration,
    declared,
)

_NOTES = (
    "radio identity and firmware state live on the radio and are outside "
    "host-file capture; an ordinary power cycle normally preserves stored "
    "settings, but a factory reset or destructive reflash may erase them",
    "a file snapshot cannot prove exclusive radio ownership; verify the "
    "configured TCP endpoint (which may be remote), or the selected serial "
    "or Bluetooth device and its exclusive use, before restarting",
)


class MeshtasticStoppedState:
    """Provider-owned declaration of one configured radio connection."""

    provider = "meshtastic"

    def coverage(self, config: object, *, adapter_id: str) -> ProviderCoverage:
        """Declare the honest device-attached representation with zero files."""
        if getattr(config, "connection_type", "") == "fake":
            return declared(
                provider=self.provider,
                adapter_id=adapter_id,
                files=(),
                state_class="stateless_local",
                notes=("fake connectivity journals in memory only",),
            )
        return declared(
            provider=self.provider,
            adapter_id=adapter_id,
            files=(),
            state_class="device_attached",
            notes=_NOTES,
        )

    def local_exclusion(self, config: object, *, adapter_id: str) -> LocalExclusion:
        """Report that device ownership cannot be established from files."""
        connection_type = getattr(config, "connection_type", "")
        if connection_type == "tcp":
            instruction = "verify the configured TCP endpoint and exclusive radio use"
        elif connection_type in ("serial", "ble"):
            instruction = "verify the selected physical radio and exclusive device use"
        else:
            instruction = "verify the selected radio and exclusive device use"
        return LocalExclusion(
            self.provider,
            note=f"no local ownership primitive exists; {instruction}",
        )


def declaration() -> StoppedStateDeclaration:
    """Return the provider-owned declaration for stopped-state planning."""
    return MeshtasticStoppedState()

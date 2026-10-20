"""Adapter-owned runtime configuration wrapper for meshtastic."""

from dataclasses import dataclass
from typing import Any, Self

from medre.config.adapters.meshtastic import MeshtasticConfig
from medre.config.model import GenericAdapterRuntimeConfig


@dataclass(frozen=True)
class MeshtasticRuntimeConfig(GenericAdapterRuntimeConfig):
    """Validated runtime configuration for one meshtastic instance."""

    config: MeshtasticConfig | None = None

    @classmethod
    def from_dict(cls, instance_name: str, data: dict[str, Any]) -> Self:
        return cls.from_transport_dict(
            instance_name, data, transport="meshtastic", config_cls=MeshtasticConfig
        )

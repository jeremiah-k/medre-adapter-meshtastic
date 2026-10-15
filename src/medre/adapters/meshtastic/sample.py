"""Offline example values owned by this adapter distribution."""

from medre.adapter_spec import AdapterSample


def sample_config() -> AdapterSample:
    """Supply fake configuration and its opaque conversation address."""
    return AdapterSample(
        config={"connection_type": "fake", "origin_label": "MyMesh"},
        channel="1",
    )

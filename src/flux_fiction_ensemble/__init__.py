"""Standalone public interface for Flux Fiction ensemble campaigns."""

from flux_fiction.ensemble.config import CampaignSpec, EnsembleConfigError, load_campaign_spec

__all__ = [
    "CampaignSpec",
    "EnsembleConfigError",
    "load_campaign_spec",
]

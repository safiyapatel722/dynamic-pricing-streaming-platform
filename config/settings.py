"""
Central config using pydantic-settings.
Reads secrets from .env locally, from GCP environment variables in production.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import time


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # secrets — never commit these, must be set in .env or GCP Cloud Run
    gcp_project_id: str
    pubsub_topic: str
    gcs_bucket: str
    pubsub_subscription_consumer: str  # for consumer.py
    pubsub_subscription_archiver: str  # for archiver.py

    # config — safe to commit, overridable per environment
    window_size_sec: int = 60
    surge_cap: int = 3
    base_fare: int = 100
    locations: list[str] = [
        "Hinjewadi",
        "Baner",
        "Koregaon_Park",
        "Viman_Nagar",
        "Wakad"
    ]
    # event generation weights
    rider_peak_weight: int = 4      # during peak: heavily rider-biased
    driver_peak_weight: int = 1
    rider_offpeak_weight: int = 1   # off-peak: slightly driver-biased  
    driver_offpeak_weight: int = 2

    # stable business rules — not environment-driven
    peak_hours: list[tuple] = [
        (time(8, 0), time(11, 0)),    # morning rush
        (time(17, 0), time(21, 0))    # evening rush
    ]


try:
    settings = Settings()
except Exception as e:
    raise RuntimeError(f"Configuration error: {e}")
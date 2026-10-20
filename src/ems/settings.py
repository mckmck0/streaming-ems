"""Runtime settings (pydantic-settings): the only place that reads the environment (P11)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

TopicsProfile = Literal["live", "sim", "scale", "cluster"]


class EmsSettings(BaseSettings):
    """Connection and runtime settings. Behaviour lives in YAML under ``config_dir``.

    Variables use the ``EMS_`` prefix (``EMS_KAFKA_BOOTSTRAP``) so they never collide with the
    ``KAFKA_*`` variables the Kafka image turns into broker properties.
    """

    model_config = SettingsConfigDict(env_prefix="EMS_", env_file=None, extra="ignore")

    kafka_bootstrap: str = "localhost:9092"
    topics_profile: TopicsProfile = "live"
    topic_prefix: str = Field(default="", pattern=r"^[A-Za-z0-9._-]*$")
    config_dir: Path = Path("config")
    data_dir: Path = Path("data")
    delivery_timeout_ms: int = Field(default=120_000, gt=0)
    open_meteo_api_key: SecretStr | None = None
    entsoe_token: SecretStr | None = None


def load_settings() -> EmsSettings:
    return EmsSettings()

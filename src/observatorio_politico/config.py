from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "observatorio-politico-brasil"
    app_env: str = "development"
    log_level: str = "INFO"

    portal_transparencia_base_url: str = (
        "https://api.portaldatransparencia.gov.br/api-de-dados"
    )
    portal_transparencia_api_key: str = Field(
        min_length=1,
        repr=False,
    )

    request_timeout_seconds: float = Field(default=60, gt=0)
    request_max_attempts: int = Field(default=5, ge=1)

    bronze_path: Path = Path("data/bronze")
    logs_path: Path = Path("logs")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8-sig",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()

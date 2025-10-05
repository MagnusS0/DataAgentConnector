from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Settings for the application.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    log_level: str = Field(default="INFO")
    log_folder: Path = Field(default=Path("logs"))


@lru_cache
def get_settings() -> Settings:
    return Settings()

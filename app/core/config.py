from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.models.database_registry import DatabaseRegistry


class Settings(BaseSettings):
    """
    Settings for the application.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = Field(default="INFO")
    log_folder: Path = Field(default=Path("logs"))

    databases_file: Path = Field(
        default=Path("databases.toml"),
        description="TOML file defining available databases.",
    )
    databases: DatabaseRegistry | None = None

    allowed_sql_commands: tuple[str, ...] = Field(
        default=(
            "SELECT",
            "SHOW",
            "DESCRIBE",
            "DESC",
            "EXPLAIN",
            "WITH",
        ),
        description="SQL keywords that are allowed, defaults to read-only mode.",
    )

    @field_validator("databases_file")
    @classmethod
    def ensure_file_exists(cls, value: Path) -> Path:
        if not value.exists():
            raise FileNotFoundError(f"Databases file '{value}' is missing.")
        return value

    @field_validator("databases", mode="before")
    @classmethod
    def validate_registry(cls, value):
        if value is None or isinstance(value, DatabaseRegistry):
            return value
        raise TypeError("databases must be a DatabaseRegistry instance or None.")

    @model_validator(mode="after")
    def ensure_registry(self) -> "Settings":
        if self.databases is None:
            self.databases = DatabaseRegistry.from_toml(self.databases_file)
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
